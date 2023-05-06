#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Type

import pendulum
from pendulum import Date
import requests
from requests.exceptions import HTTPError
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class SoundchartsStream(HttpStream, ABC):
    url_base = "https://customer.api.soundcharts.com/api/"
    primary_key = "uuid"

    def __init__(
        self,
        app_id: str,
        api_key: str,
        project_timezone: str,
        start_date: Date = None,
        end_date: Date = None,
        date_window_size: int = 30,  # in days
        artist_uuids: list = [],  # in days
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.app_id = app_id
        self.api_key = api_key
        self.start_date = start_date
        self.end_date = end_date
        self.date_window_size = date_window_size
        self.project_timezone = project_timezone
        self.artist_uuids = artist_uuids

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Responses contains a "page" object with the following structure:
        {
            "offset": 0,
            "limit": 100,
            "next": "/api/v2/artist/11e83fed-288f-9b72-a4a0-aa1c026db3d8/songs?sortBy=name&sortOrder=asc&offset=100&limit=100",
            "previous": null,
            "total": 230
        }
        """

        page = response.json().get("page")
        if page:
            total = page["total"]
            next_offset = page["offset"] + page["limit"]
            if next_offset < total:
                return {"offset": next_offset}

        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        # Max number of records to fetch per request is 100
        params = {"limit": 100}

        # Handle pagination by inserting the next page's token in the request parameters
        if next_page_token:
            params.update(next_page_token)

        return params

    def request_headers(self, **kwargs) -> Mapping[str, Any]:
        if self.app_id:
            return {"x-app-id": self.app_id}

        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("items", [])  # Soundcharts puts records in a container array "items"

    def read_records(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        # Reading records while handling the errors
        try:
            yield from super().read_records(stream_slice=stream_slice, **kwargs)
        except HTTPError as e:
            # This whole try/except situation in `read_records()` isn't good but right now in `self._send_request()`
            # function we have `response.raise_for_status()` so we don't have much choice on how to handle errors.
            # Bocked on https://github.com/airbytehq/airbyte/issues/3514.
            if e.response.status_code == requests.codes.NOT_FOUND:
                error = e.response.json().get("errors")[0]
                error_msg = f"Syncing `{self.__class__.__name__}` stream isn't available: `{str(error.get('message'))}`."
            elif e.response.status_code == requests.codes.BAD_REQUEST:
                error = e.response.json().get("errors")[0]
                error_msg = f"Syncing `{self.__class__.__name__}` stream isn't available: `{str(error.get('message'))}`."
                self.logger.error(error_msg)
                raise e
            elif e.response.status_code == requests.codes.FORBIDDEN:
                error = e.response.json().get("errors")[0]
                error_msg = f"Syncing `{self.__class__.__name__}` stream is forbidden: `{str(error.get('message'))}`."
                self.logger.error(error_msg)
                raise e
            else:
                # most probably here we're facing a 500 server error and a risk to get a non-json response, so lets output response.text
                self.logger.error(f"Undefined error while reading records: {e.response.text}")
                raise e

            self.logger.warn(error_msg)


class DateSlicesMixin:
    def stream_slices(
        self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        date_slices: list = []

        # use the latest date between self.start_date and stream_state
        start_date = self.start_date
        if stream_state and self.cursor_field and self.cursor_field in stream_state:
            # Remove time part from state because API accept 'from_date' param in date format only ('YYYY-MM-DD')
            # It also means that sync returns duplicated entries for the date from the state (date range is inclusive)
            stream_state_date = pendulum.parse(stream_state[self.cursor_field]).date()
            start_date = max(start_date, stream_state_date)

        # end_date cannot be later than today
        end_date = min(self.end_date, pendulum.today(tz=self.project_timezone).date())

        while start_date <= end_date:
            current_end_date = start_date + timedelta(days=self.date_window_size - 1)  # -1 is needed because dates are inclusive
            date_slices.append(
                {
                    "start_date": str(start_date),
                    "end_date": str(min(current_end_date, end_date)),
                }
            )
            # add 1 additional day because date range is inclusive
            start_date = current_end_date + timedelta(days=1)

        return date_slices

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        return {
            **params,
            "startDate": stream_slice["start_date"],
            "endDate": stream_slice["end_date"],
        }


class IncrementalSoundchartsStream(SoundchartsStream, ABC):
    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, any]:
        updated_state = latest_record.get(self.cursor_field)
        if updated_state:
            state_value = current_stream_state.get(self.cursor_field)
            if state_value:
                updated_state = max(updated_state, state_value)
            current_stream_state[self.cursor_field] = updated_state
        return current_stream_state


class Platforms(SoundchartsStream):
    primary_key = "code"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v2/referential/platforms"


class PlatformsSocial(Platforms):
    use_cache = True

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v2/referential/platforms/social"


class PlatformsStreaming(Platforms):
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v2/referential/platforms/streaming"


class SoundchartsArtistSubStream(SoundchartsStream, ABC):
    @property
    @abstractmethod
    def path_template(self) -> str:
        """
        :return: sub stream path template
        """

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for artist_uuid in self.artist_uuids:
            yield {"artist_uuid": artist_uuid}

    def path(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> str:
        return self.path_template.format(artist_uuid=stream_slice["artist_uuid"])

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        response_json = response.json()
        for record in response_json.get("items", []):
            yield self.transform(record=record, stream_slice=stream_slice)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any]) -> MutableMapping[str, Any]:
        """
        We need to provide the "artist_uuid" for the primary_key attribute
        """
        record["artist_uuid"] = stream_slice["artist_uuid"]
        return record


class Artist(SoundchartsArtistSubStream):
    path_template = "v2/artist/{artist_uuid}"

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        response_json = response.json()
        yield response_json.get("object")


class ArtistSongs(SoundchartsArtistSubStream):
    path_template = "v2/artist/{artist_uuid}/songs"


class ArtistAlbums(SoundchartsArtistSubStream):
    path_template = "v2/artist/{artist_uuid}/albums"


class ArtistIdentifiers(SoundchartsArtistSubStream):
    primary_key = ["artist_uuid", "platform"]
    path_template = "v2/artist/{artist_uuid}/identifiers"


class SoundchartsArtistPlatformSubStream(DateSlicesMixin, IncrementalSoundchartsStream):
    @property
    @abstractmethod
    def path_template(self) -> str:
        """
        :return: sub stream path template
        """

    def __init__(self, parent: HttpStream, **kwargs):
        super().__init__(**kwargs)
        self.parent = parent

    def stream_slices(
        self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, any]]]:
        stream_slices: List = []
        self.logger.info(
            {"stream_state": stream_state, "cursor_field": cursor_field, "sync_mode": sync_mode, "authenticator": self.authenticator}
        )

        date_slices = super().stream_slices(sync_mode, cursor_field=cursor_field, stream_state=stream_state)

        for date_slice in date_slices:
            for artist_uuid in self.artist_uuids:
                for platform in self.parent.read_records(sync_mode=SyncMode.full_refresh):
                    artist_platform_slice = {"artist_uuid": artist_uuid, "platform_code": platform["code"]}
                    stream_slices.append({**artist_platform_slice, **date_slice})

        return stream_slices

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        return params

    def path(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> str:
        return self.path_template.format(artist_uuid=stream_slice["artist_uuid"], platform_code=stream_slice["platform_code"])

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        for record in super().parse_response(response, stream_slice=stream_slice, **kwargs):
            yield self.transform(record=record, stream_slice=stream_slice)

    def transform(self, record: MutableMapping[str, Any], stream_slice: Mapping[str, Any]) -> MutableMapping[str, Any]:
        """
        We need to provide the "artist_uuid" and "platform_code" for the primary_key attribute
        """
        record["artist_uuid"] = stream_slice["artist_uuid"]
        record["platform_code"] = stream_slice["platform_code"]
        return record


class ArtistAudience(SoundchartsArtistPlatformSubStream):
    primary_key = ["artist_uuid", "platform_code", "date"]
    cursor_field: str = "date"
    path_template = "v2/artist/{artist_uuid}/audience/{platform_code}"
