#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.utils import is_cloud_environment

from typing import Any, Mapping, MutableMapping, Tuple, Union

import pendulum
from airbyte_cdk.config_observation import emit_configuration_as_airbyte_control_message
from airbyte_cdk.sources.declarative.yaml_declarative_source import YamlDeclarativeSource
from airbyte_cdk.sources.streams.http.requests_native_auth.oauth import SingleUseRefreshTokenOauth2Authenticator
from airbyte_cdk.sources.streams.http.requests_native_auth.token import TokenAuthenticator
from airbyte_cdk.utils import AirbyteTracedException
from requests.exceptions import HTTPError

from .utils import parse_url


class SingleUseRefreshTokenGitlabOAuth2Authenticator(SingleUseRefreshTokenOauth2Authenticator):
    def __init__(self, *args, created_at_name: str = "created_at", **kwargs):
        super().__init__(*args, **kwargs)
        self._created_at_name = created_at_name

    def get_created_at_name(self) -> str:
        return self._created_at_name

    def get_access_token(self) -> str:
        if self.token_has_expired():
            new_access_token, access_token_expires_in, access_token_created_at, new_refresh_token = self.refresh_access_token()
            new_token_expiry_date = self.get_new_token_expiry_date(access_token_expires_in, access_token_created_at)
            self.access_token = new_access_token
            self.set_refresh_token(new_refresh_token)
            self.set_token_expiry_date(new_token_expiry_date)
            emit_configuration_as_airbyte_control_message(self._connector_config)
        return self.access_token

    @staticmethod
    def get_new_token_expiry_date(access_token_expires_in: int, access_token_created_at: int) -> pendulum.DateTime:
        return pendulum.from_timestamp(access_token_created_at + access_token_expires_in)

    def refresh_access_token(self) -> Tuple[str, int, int, str]:
        response_json = self._get_refresh_access_token_response()
        return (
            response_json[self.get_access_token_name()],
            response_json[self.get_expires_in_name()],
            response_json[self.get_created_at_name()],
            response_json[self.get_refresh_token_name()],
        )


def get_authenticator(config: MutableMapping) -> Union[SingleUseRefreshTokenOauth2Authenticator, TokenAuthenticator]:
    if config["credentials"]["auth_type"] == "access_token":
        return TokenAuthenticator(token=config["credentials"]["access_token"])
    return SingleUseRefreshTokenGitlabOAuth2Authenticator(
        config,
        token_refresh_endpoint=f"https://{config['api_url']}/oauth/token",
        refresh_token_error_status_codes=(400,),
        refresh_token_error_key="error",
        refresh_token_error_values="invalid_grant",
    )


class SourceGitlab(YamlDeclarativeSource):
    def __init__(self):
        super().__init__(**{"path_to_yaml": "manifest.yaml"})

    @staticmethod
    def _ensure_default_values(config: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        config["api_url"] = config.get("api_url") or "gitlab.com"
        return config

    def _try_refresh_access_token(self, logger, config: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        This method attempts to refresh the expired `access_token`, while `refresh_token` is still valid.
        In order to obtain the new `refresh_token`, the Customer should `re-auth` in the source settings.
        """
        # get current authenticator
        authenticator: Union[SingleUseRefreshTokenOauth2Authenticator, TokenAuthenticator] = get_authenticator(config)
        if isinstance(authenticator, SingleUseRefreshTokenOauth2Authenticator):
            try:
                creds = authenticator.refresh_access_token()
                # update the actual config values
                config["credentials"]["access_token"] = creds[0]
                config["credentials"]["refresh_token"] = creds[3]
                config["credentials"]["token_expiry_date"] = authenticator.get_new_token_expiry_date(creds[1], creds[2]).to_rfc3339_string()
                # update the config
                emit_configuration_as_airbyte_control_message(config)
                logger.info("The `access_token` was successfully refreshed.")
                return config
            except (AirbyteTracedException, HTTPError) as http_error:
                raise http_error
            except Exception as e:
                raise Exception(f"Unknown error occurred while refreshing the `access_token`, details: {e}")

    def _handle_expired_access_token_error(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            return self.check_connection(logger, self._try_refresh_access_token(logger, config))
        except HTTPError as http_error:
            return False, f"Unable to refresh the `access_token`, please re-authenticate in Sources > Settings. Details: {http_error}"

    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        config = self._ensure_default_values(config)
        is_valid, scheme, _ = parse_url(config["api_url"])
        if not is_valid:
            return False, "Invalid API resource locator."
        if scheme == "http" and is_cloud_environment():
            return False, "Http scheme is not allowed in this environment. Please use `https` instead."
        try:
            return super().check_connection(logger, config)
        except HTTPError as http_error:
            if config["credentials"]["auth_type"] == "oauth2.0":
                if http_error.response.status_code == 401:
                    return self._handle_expired_access_token_error(logger, config)
                elif http_error.response.status_code == 500:
                    return False, f"Unable to connect to Gitlab API with the provided credentials - {repr(http_error)}"
            else:
                return False, f"Unable to connect to Gitlab API with the provided Private Access Token - {repr(http_error)}"
        except Exception as error:
            return False, f"Unknown error occurred while checking the connection - {repr(error)}"
