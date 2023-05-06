#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, List, Mapping, Tuple
import logging

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from source_soundcharts.streams import (
    Artist,
    ArtistAlbums,
    ArtistAudience,
    ArtistIdentifiers,
    ArtistSongs,
    Platforms,
    PlatformsSocial,
    PlatformsStreaming,
)


# Source
class SourceSoundcharts(AbstractSource):
    def _validate_and_transform(self, config: Mapping[str, Any]):
        logger = logging.getLogger("airbyte")
        source_spec = self.spec(logger)
        default_project_timezone = source_spec.connectionSpecification["properties"]["project_timezone"]["default"]
        config["project_timezone"] = pendulum.timezone(config.get("project_timezone", default_project_timezone))

        today = pendulum.today(tz=config["project_timezone"]).date()
        start_date = config.get("start_date")
        if start_date:
            config["start_date"] = pendulum.parse(start_date).date()
        else:
            config["start_date"] = today.subtract(days=365)

        end_date = config.get("end_date")
        if end_date:
            config["end_date"] = pendulum.parse(end_date).date()
        else:
            config["end_date"] = today

        for k in ["date_window_size"]:
            if k not in config:
                config[k] = source_spec.connectionSpecification["properties"][k]["default"]

        return config

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            config = self._validate_and_transform(config)
            authenticator = TokenAuthenticator(token=config["api_key"], auth_header="x-api-key", auth_method="")
            args = {
                "authenticator": authenticator,
            }

            # Apply config to args passed to all streams
            args.update(config)

            stream = Platforms(**args)
            records = stream.read_records(sync_mode=SyncMode.full_refresh)
            next(records)
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        config = self._validate_and_transform(config)
        authenticator = TokenAuthenticator(token=config["api_key"], auth_header="x-api-key", auth_method="")
        args = {
            "authenticator": authenticator,
        }

        # Apply config to args passed to all streams
        args.update(config)
        platforms_social = PlatformsSocial(**args)
        return [
            Artist(**args),
            ArtistAlbums(**args),
            ArtistAudience(parent=platforms_social, **args),
            ArtistIdentifiers(**args),
            ArtistSongs(**args),
            Platforms(**args),
            platforms_social,
            PlatformsStreaming(**args),
        ]
