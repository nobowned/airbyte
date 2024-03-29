# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from dataclasses import dataclass

from airbyte_cdk.sources.declarative.auth.declarative_authenticator import NoAuth, DeclarativeAuthenticator
from airbyte_cdk.sources.declarative.auth.token import BearerAuthenticator, BasicHttpAuthenticator

@dataclass
class CustomAuthenticator(NoAuth):
    authenticator: DeclarativeAuthenticator
    config: Config
    email: Union[InterpolatedString, str]
    api_token: Union[InterpolatedString, str]

    def __post_init__(self, parameters: Mapping[str, Any]):
        confluence_server: bool = self.config.get("confluence_server")
        self.authenticator = BearerAuthenticator(
            InterpolatedStringTokenProvider(api_token=self.api_token or "", config=self.config, parameters=parameters),
            config=self.config,
            parameters=parameters,
        )
        if confluence_server else
        BasicHttpAuthenticator(
            password=self.api_token, username=self.email, config=self.config, parameters=self.parameters
        )

    @property
    def auth_header(self) -> str:
        return "Authorization"

    @property
    def token(self) -> str:
        return authenticator.token()