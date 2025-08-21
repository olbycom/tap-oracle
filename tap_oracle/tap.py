"""oracle tap class."""

from __future__ import annotations

import atexit
import copy
import io
import os
import signal
import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

import paramiko
from nekt_singer_sdk import SQLStream, SQLTap, Stream
from nekt_singer_sdk import typing as th  # JSON schema typing helpers
from nekt_singer_sdk.contrib.msgspec import MsgSpecWriter
from nekt_singer_sdk.singerlib import Catalog, Metadata, Schema, StateMessage
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url

from tap_oracle.connector import OracleConnector
from tap_oracle.ssh_tunnel import SSHTunnelForwarder
from tap_oracle.streams import (
    OracleLogBasedStream,
    OracleSingleLogBasedStream,
    OracleStream,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class TapOracle(SQLTap):
    name = "tap-oracle"
    default_stream_class = OracleStream
    earliest_scn_file_name: str | None = None
    latest_scn_file_name: str | None = None
    message_writer_class = MsgSpecWriter

    def __init__(
        self,
        *args: tuple,
        **kwargs: dict,
    ) -> None:
        """Construct a Oracle tap.

        Should use JSON Schema instead
        See https://github.com/meltano/sdk/pull/1525
        """
        super().__init__(*args, **kwargs)
        sql_alchemy_url_exists = self.config.get("sqlalchemy_url") is not None
        oracle_dsn_exists = self.config.get("oracle_dsn") is not None

        # Check for individual connection parameters
        individual_url_params_exist = all(
            [
                self.config.get("host") is not None,
                self.config.get("port") is not None,
                self.config.get("user") is not None,
                self.config.get("password") is not None,
            ]
        )

        # For individual params, also need service/SID
        if individual_url_params_exist:
            has_database_param = any(
                [
                    self.config.get("service_name"),
                    self.config.get("sid"),
                ]
            )
            if not has_database_param:
                msg = "When using individual connection parameters, must specify service_name or sid"
                self.user_logger.error(msg)
                sys.exit(1)

        if not (sql_alchemy_url_exists or oracle_dsn_exists or individual_url_params_exist):
            msg = "Need either sqlalchemy_url, oracle_dsn, or host/port/user/password with service_name/sid parameters to be set"
            self.user_logger.error(msg)
            sys.exit(1)

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            description=("Hostname for Oracle instance. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=1521,
            description=("The port on which Oracle is awaiting connection. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "user",
            th.StringType,
            description=("User name used to authenticate. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "password",
            th.StringType,
            secret=True,
            description=("Password used to authenticate. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,
            description=(
                "Example oracle+oracledb://[username]:[password]@localhost:1521/[service_name] "  # noqa: E501
                "see https://docs.sqlalchemy.org/en/20/dialects/oracle.html for more information. "  # noqa: E501
                "For Oracle 19c, use oracle+oracledb driver for best performance."
            ),
        ),
        th.Property(
            "filter_schemas",
            th.ArrayType(th.StringType),
            description=(
                "If an array of schema names is provided, the tap will only process "
                "the specified Oracle schemas and ignore others. If left blank, the "
                "tap automatically determines ALL available Oracle schemas."
            ),
        ),
        th.Property(
            "ssh_tunnel",
            th.ObjectType(
                th.Property(
                    "enable",
                    th.BooleanType,
                    required=False,
                    default=False,
                    description=("Enable an ssh tunnel (also known as bastion server), see the other ssh_tunnel.* properties for more details"),
                ),
                th.Property(
                    "host",
                    th.StringType,
                    required=False,
                    description="Host of the bastion server, this is the host we'll connect to via ssh",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    required=False,
                    description="Username to connect to bastion server",
                ),
                th.Property(
                    "port",
                    th.IntegerType,
                    required=False,
                    default=22,
                    description="Port to connect to bastion server",
                ),
                th.Property(
                    "password",
                    th.StringType,
                    required=False,
                    secret=True,
                    description="Password for authentication to the bastion server",
                ),
                th.Property(
                    "private_key",
                    th.StringType,
                    required=False,
                    secret=True,
                    description="Private Key for authentication to the bastion server",
                ),
                th.Property(
                    "private_key_password",
                    th.StringType,
                    required=False,
                    secret=True,
                    default=None,
                    description="Private Key Password, leave None if no password is set",
                ),
                th.Property(
                    "run_tunnel_auth_interactive_dumb",
                    th.BooleanType,
                    required=False,
                    default=False,
                    description=("Enable dumb interaction on auth for ssh tunnel"),
                ),
            ),
            required=False,
            description="SSH Tunnel Configuration, this is a json object",
        ),
        th.Property(
            "chunk_size",
            th.IntegerType,
            default=5000,
            description=("The number of rows to fetch at a time. If set to 0, the tap will fetch all rows at once (no chunking)."),
        ),
        th.Property(
            "service_name",
            th.StringType,
            required=False,
            description=("Oracle service name for service name connections."),
        ),
        th.Property(
            "sid",
            th.StringType,
            required=False,
            description=("Oracle SID for SID connections."),
        ),
        th.Property(
            "oracle_dsn",
            th.StringType,
            required=False,
            description=("Complete Oracle DSN string. If provided, will override host/port/database parameters."),
        ),
        th.Property(
            "thick_mode",
            th.BooleanType,
            default=True,
            description=("Enable Oracle thick mode for better performance. Requires Oracle Instant Client."),
        ),
        th.Property(
            "date_format",
            th.StringType,
            required=False,
            description=("Custom date format for Oracle date columns. If not specified, uses ISO format."),
        ),
        th.Property(
            "ssl_enable",
            th.BooleanType,
            default=False,
            description=("Enable SSL/TLS encryption for Oracle connection."),
        ),
        th.Property(
            "ssl_certificate_authority",
            th.StringType,
            required=False,
            description=("SSL Certificate Authority for Oracle TLS connections."),
        ),
        th.Property(
            "ssl_client_certificate",
            th.StringType,
            required=False,
            description=("SSL client certificate for Oracle TLS connections."),
        ),
        th.Property(
            "ssl_client_private_key",
            th.StringType,
            required=False,
            secret=True,
            description=("SSL client private key for Oracle TLS connections."),
        ),
        th.Property(
            "ssl_storage_directory",
            th.StringType,
            default="/tmp",
            description=("Directory to store SSL certificates for Oracle connections."),
        ),
    ).to_dict()

    def get_sqlalchemy_url(self, config: Mapping[str, Any]) -> str:
        """Generate a SQLAlchemy URL for Oracle.

        Args:
            config: The configuration for the connector.
        """
        if config.get("sqlalchemy_url"):
            return cast(str, config["sqlalchemy_url"])

        # Handle Oracle DSN if provided
        if config.get("oracle_dsn"):
            sqlalchemy_url = URL.create(
                drivername="oracle+oracledb",
                username=config["user"],
                password=config["password"],
                host=None,  # DSN includes connection info
                database=config["oracle_dsn"],
                query=self.get_sqlalchemy_query(config=config),
            )
            return cast(str, sqlalchemy_url)

        # Determine service name or SID
        database = None
        if config.get("service_name"):
            database = config["service_name"]
        elif config.get("sid"):
            database = config["sid"]

        sqlalchemy_url = URL.create(
            drivername="oracle+oracledb",
            username=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=database,
            query=self.get_sqlalchemy_query(config=config),
        )
        return cast(str, sqlalchemy_url)

    def get_sqlalchemy_query(self, config: Mapping[str, Any]) -> dict:
        """Build Oracle-specific query parameters for SQLAlchemy URL.

        Args:
            config: The configuration for the connector.

        Returns:
            Dictionary of query parameters for Oracle connection.
        """
        query = {}

        # Oracle TLS/SSL configuration
        if config.get("ssl_enable", False):
            # Oracle uses different SSL parameters than MySQL/PostgreSQL
            query["ssl_context"] = "true"

            if config.get("ssl_certificate_authority"):
                query["ssl_ca"] = self.filepath_or_certificate(
                    value=config["ssl_certificate_authority"],
                    alternative_name=config.get("ssl_storage_directory", "/tmp") + "/oracle_ca.crt",
                )

            if config.get("ssl_client_certificate"):
                query["ssl_cert"] = self.filepath_or_certificate(
                    value=config["ssl_client_certificate"],
                    alternative_name=config.get("ssl_storage_directory", "/tmp") + "/oracle_client.crt",
                )

            if config.get("ssl_client_private_key"):
                query["ssl_key"] = self.filepath_or_certificate(
                    value=config["ssl_client_private_key"],
                    alternative_name=config.get("ssl_storage_directory", "/tmp") + "/oracle_client.key",
                    restrict_permissions=True,
                )

        return query

    def filepath_or_certificate(
        self,
        value: str,
        alternative_name: str,
        restrict_permissions: bool = False,
    ) -> str:
        if os.path.isfile(value):
            return value

        with open(alternative_name, "wb") as alternative_file:
            alternative_file.write(
                value.replace("\\n", "\n")
                .replace(" ", "")
                .replace("-----BEGINCERTIFICATE-----", "-----BEGIN CERTIFICATE-----")
                .replace("-----ENDCERTIFICATE-----", "-----END CERTIFICATE-----")
            )
        if restrict_permissions:
            os.chmod(alternative_name, 0o600)

        return alternative_name

    @cached_property
    def connector(self) -> OracleConnector:
        url = make_url(self.get_sqlalchemy_url(config=self.config))
        ssh_config = self.config.get("ssh_tunnel", {})

        if ssh_config.get("enable", False):
            # Return a new URL with SSH tunnel parameters
            url = self.ssh_tunnel_connect(ssh_config=ssh_config, url=url)

        return OracleConnector(
            is_running_discovery=self.is_running_discovery,
            config=dict(self.config),
            sqlalchemy_url=url.render_as_string(hide_password=False),
        )

    def guess_key_type(self, key_data: str) -> paramiko.PKey:
        for key_class in (
            paramiko.RSAKey,
            paramiko.DSSKey,
            paramiko.ECDSAKey,
            paramiko.Ed25519Key,
        ):
            try:
                key = key_class.from_private_key(io.StringIO(key_data))  # type: ignore[attr-defined]
            except paramiko.SSHException:  # noqa: PERF203
                continue
            else:
                return key

        errmsg = "Could not determine the key type."
        raise ValueError(errmsg)

    def ssh_tunnel_connect(self, *, ssh_config: dict[str, Any], url: URL) -> URL:
        """Connect to the SSH Tunnel and swap the URL to use the tunnel.

        Args:
            ssh_config: The SSH Tunnel configuration
            url: The original URL to connect to.

        Returns:
            The new URL to connect to, using the tunnel.
        """
        if ssh_config.get("password"):
            credentials = {
                "ssh_password": ssh_config.get("password"),
            }
        else:
            credentials = {
                "ssh_private_key": self.guess_key_type(ssh_config["private_key"]),
                "ssh_private_key_password": ssh_config.get("private_key_password"),
            }

        self.ssh_tunnel: SSHTunnelForwarder = SSHTunnelForwarder(
            ssh_address_or_host=(ssh_config["host"], ssh_config["port"]),
            ssh_username=ssh_config["username"],
            remote_bind_address=(url.host, url.port),
            run_tunnel_auth_interactive_dumb=ssh_config.get("run_tunnel_auth_interactive_dumb", False),
            **credentials,
        )
        self.ssh_tunnel.start()
        self.internal_logger.info("SSH Tunnel started")
        # On program exit clean up, want to also catch signals
        atexit.register(self.clean_up)
        signal.signal(signal.SIGTERM, self.catch_signal)
        # Probably overkill to catch SIGINT, but needed for SIGTERM
        signal.signal(signal.SIGINT, self.catch_signal)

        # Swap the URL to use the tunnel
        return url.set(
            host=self.ssh_tunnel.local_bind_host,
            port=self.ssh_tunnel.local_bind_port,
        )

    def clean_up(self) -> None:
        self.internal_logger.info("Shutting down SSH Tunnel")
        self.ssh_tunnel.stop()

    def catch_signal(self, signum, frame) -> None:  # noqa: ANN001 ARG002
        sys.exit(1)  # Calling this to be sure atexit is called, so clean_up gets called

    @property
    def catalog_dict(self) -> dict:
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(self.connector.discover_catalog_entries())

        self._catalog_dict: dict = result
        return self._catalog_dict

    @cached_property
    def catalog(self) -> Catalog:
        """Get the tap's working catalog.

        Override to do LOG_BASED modifications.

        Returns:
            A Singer catalog object.
        """
        new_catalog: Catalog = Catalog()
        modified_streams: list = []
        for stream in super().catalog.streams:
            stream_modified = False
            new_stream = copy.deepcopy(stream)
            # If LOG_BASED, apply existing nullability and _sdc column logic
            if new_stream.replication_method == "LOG_BASED" and new_stream.schema.properties:
                for property in new_stream.schema.properties.values():
                    if "null" not in property.type:
                        if isinstance(property.type, list):
                            property.type.append("null")
                        else:
                            property.type = [property.type, "null"]
                if new_stream.schema.required:
                    stream_modified = True
                    new_stream.schema.required = None
                if "_sdc_deleted_at" not in new_stream.schema.properties:
                    stream_modified = True
                    new_stream.schema.properties.update({"_sdc_deleted_at": Schema(type=["string", "null"], format="date-time")})
                    new_stream.metadata.update({("properties", "_sdc_deleted_at"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)})
                if "_sdc_scn" not in new_stream.schema.properties:
                    stream_modified = True
                    new_stream.schema.properties.update({"_sdc_scn": Schema(type=["string", "null"])})
                    new_stream.metadata.update({("properties", "_sdc_scn"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)})
            if stream_modified:
                modified_streams.append(new_stream.tap_stream_id)
            new_catalog.add_stream(new_stream)
        if modified_streams:
            self.internal_logger.info(
                "One or more LOG_BASED catalog entries were modified "
                f"({modified_streams=}) to allow nullability and include _sdc columns. "
                "See README for further information. SCN columns added for Oracle Log Miner support."
            )
        return new_catalog

    @property
    def streams(self) -> dict[str, Stream]:
        if self._streams is None:
            self._streams = {}

            for stream in self.load_streams():
                if self.catalog is not None:
                    stream.apply_catalog(self.catalog)
                self._streams[stream.name] = stream
        return self._streams

    def discover_streams(self) -> Sequence[Stream]:
        streams: list[SQLStream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            if catalog_entry["replication_method"] == "LOG_BASED":
                streams.append(OracleLogBasedStream(self, catalog_entry, connector=self.connector))
            else:
                streams.append(OracleStream(self, catalog_entry, connector=self.connector))
        return streams

    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        if self.state:
            self.write_message(StateMessage(value=self.state))

        log_based_streams = [stream for stream in self.streams.values() if stream.replication_method == "LOG_BASED" and stream.selected]
        other_streams = [stream for stream in self.streams.values() if stream.replication_method != "LOG_BASED" and stream.selected]

        if log_based_streams:
            log_based_stream = OracleSingleLogBasedStream(
                tap=self,
                connector=self.connector,
                log_based_streams=log_based_streams,
            )
            log_based_stream.sync()
            log_based_stream.finalize_state_progress_markers()
        else:
            try:
                log_based_stream = OracleSingleLogBasedStream(
                    tap=self,
                    connector=self.connector,
                    log_based_streams=[],
                )
                log_based_stream.fast_forward_to_latest_scn()
            except Exception:
                pass

        for stream in other_streams:
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info("Skipping deselected stream '%s'.", stream.name)
                continue

            if stream.parent_stream_type:
                self.logger.debug(
                    "Child stream '%s' is expected to be called by parent stream '%s'. Skipping direct invocation.",
                    type(stream).__name__,
                    stream.parent_stream_type.__name__,
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()

        # this second loop is needed for all streams to print out their costs
        # including child streams which are otherwise skipped in the loop above
        for stream in self.streams.values():
            stream.log_sync_costs()


if __name__ == "__main__":
    TapOracle.cli()
