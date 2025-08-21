"""SQL client handling."""

from __future__ import annotations

import datetime
import os
from typing import TYPE_CHECKING, Any

import pendulum
import singer_sdk.helpers._typing
import sqlalchemy as sa
import sqlalchemy.types
from nekt_singer_sdk import SQLConnector
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.custom_logger import internal_logger
from nekt_singer_sdk.singerlib import CatalogEntry, MetadataMapping, Schema
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import QueuePool

try:
    import oracledb
except ImportError:
    oracledb = None

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine, reflection
    from sqlalchemy.engine.reflection import Inspector

unpatched_conform = singer_sdk.helpers._typing._conform_primitive_property  # noqa: SLF001


def patched_conform(
    elem: Any,  # noqa: ANN401
    property_schema: dict,
) -> Any:  # noqa: ANN401
    """Override type conformance to prevent dates turning into datetimes.

    Ensures that ``date``, ``datetime`` and ``time`` objects are always
    serialised to their ISO formatted string representation so they can be
    safely consumed downstream, regardless of schema settings.
    """

    if isinstance(elem, (datetime.date, datetime.datetime, datetime.time)):
        # ``isoformat()`` gives the canonical representation for all three
        # objects (YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS[.ffffff] and HH:MM:SS[.ffffff])
        return elem.isoformat()

    return unpatched_conform(elem=elem, property_schema=property_schema)


singer_sdk.helpers._typing._conform_primitive_property = patched_conform  # noqa: SLF001


class OracleConnector(SQLConnector):
    """Connects to the Oracle SQL source."""

    def __init__(
        self,
        is_running_discovery: bool,  # noqa: FBT001
        config: dict | None = None,
        sqlalchemy_url: str | None = None,
    ) -> None:
        config = config or {}
        self.pool_size = 40
        self._table_cols_cache = {}  # Initialize table columns cache
        super().__init__(
            is_running_discovery=is_running_discovery,
            config=config,
            sqlalchemy_url=sqlalchemy_url,
        )

    def to_jsonschema_type(
        self,
        sql_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine] | Any,  # noqa: ANN401
    ) -> dict:
        """Return a JSON Schema representation of the provided Oracle type.

        Overridden from SQLConnector to correctly handle Oracle-specific types.

        Args:
            sql_type: The string representation of the Oracle SQL type, a SQLAlchemy
                TypeEngine class or object, or a custom-specified object.

        Raises:
            ValueError: If the type received could not be translated to
            jsonschema.

        Returns:
            The JSON Schema representation of the provided type.

        """
        type_name = None
        if isinstance(sql_type, str):
            type_name = sql_type.upper()
        elif isinstance(sql_type, sqlalchemy.types.TypeEngine):
            type_name = type(sql_type).__name__.upper()

        # Handle Oracle-specific JSON/XML types
        if type_name is not None:
            # Oracle XMLType should be treated as string
            if "XMLTYPE" in type_name:
                return th.StringType().type_dict
            # Oracle JSON support (available in 21c+)
            elif type_name in ("JSON",):
                return th.ObjectType().type_dict
            # Oracle BLOB/CLOB types should be strings in JSON schema
            elif type_name in ("BLOB", "CLOB", "NCLOB", "BFILE"):
                return th.StringType().type_dict
            # Oracle ROWID types
            elif "ROWID" in type_name:
                return th.StringType().type_dict

        # Use the SDK typing helper to build the base schema.
        result_dict = self.sdk_typing_object(sql_type).type_dict

        return result_dict

    def sdk_typing_object(
        self,
        from_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> th.DateTimeType | th.NumberType | th.IntegerType | th.DateType | th.StringType | th.BooleanType:
        """Return the JSON Schema dict that describes the sql type.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Raises:
            ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.

        Returns:
            A compatible JSON Schema type definition.

        """
        sqltype_lookup: dict[
            str,
            th.DateTimeType | th.NumberType | th.IntegerType | th.DateType | th.StringType | th.BooleanType,
        ] = {
            # NOTE: This is an ordered mapping, with earlier mappings taking
            # precedence. If the SQL-provided type contains the type name on
            #  the left, the mapping will return the respective singer type.
            # Oracle-specific type mappings
            "timestamp": th.DateTimeType(),
            "datetime": th.DateTimeType(),
            "date": th.DateType(),
            "time": th.StringType(),
            "interval": th.StringType(),
            # Oracle numeric types
            "number": th.NumberType(),
            "numeric": th.NumberType(),
            "decimal": th.NumberType(),
            "float": th.NumberType(),
            "binary_float": th.NumberType(),
            "binary_double": th.NumberType(),
            "real": th.NumberType(),
            "double_precision": th.NumberType(),
            # Oracle integer types (NUMBER with scale 0)
            "integer": th.IntegerType(),
            "int": th.IntegerType(),
            "smallint": th.IntegerType(),
            "bigint": th.IntegerType(),
            # Oracle string types
            "varchar": th.StringType(),
            "varchar2": th.StringType(),
            "nvarchar2": th.StringType(),
            "char": th.StringType(),
            "nchar": th.StringType(),
            "clob": th.StringType(),
            "nclob": th.StringType(),
            "long": th.StringType(),
            "text": th.StringType(),
            "string": th.StringType(),
            # Oracle binary types
            "blob": th.StringType(),  # Treat as string for now
            "bfile": th.StringType(),
            "raw": th.StringType(),
            "long_raw": th.StringType(),
            # Oracle special types
            "xmltype": th.StringType(),
            "urowid": th.StringType(),
            "rowid": th.StringType(),
        }
        if isinstance(from_type, str):
            type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(
            from_type,
            sqlalchemy.types.TypeEngine,
        ):
            type_name = from_type.__name__
        else:
            msg = "Expected `str` or a SQLAlchemy `TypeEngine` object or type."
            raise TypeError(
                msg,
            )

        # Look for the type name within the known SQL type names:
        for sqltype, jsonschema_type in sqltype_lookup.items():
            if sqltype.lower() in type_name.lower():
                return jsonschema_type

        return sqltype_lookup["string"]  # safe failover to str

    def get_schema_names(self, engine: Engine, inspected: Inspector) -> list[str]:
        if "filter_schemas" in self.config and len(self.config["filter_schemas"]) != 0:
            return self.config["filter_schemas"]
        schemas = super().get_schema_names(engine, inspected)
        # Oracle system schemas to exclude
        exclude_schemas = [
            # Standard Oracle system schemas
            "SYS",
            "SYSTEM",
            "DBSNMP",
            "SYSMAN",
            "OUTLN",
            "MGMT_VIEW",
            "FLOWS_FILES",
            "MDSYS",
            "ORDSYS",
            "EXFSYS",
            "WMSYS",
            "APPQOSSYS",
            "APEX_030200",
            "OWBSYS_AUDIT",
            "ORDDATA",
            "CTXSYS",
            "ANONYMOUS",
            "XDB",
            "ORDPLUGINS",
            "OWBSYS",
            "SI_INFORMTN_SCHEMA",
            "OLAPSYS",
            "MDDATA",
            "SPATIAL_CSW_ADMIN_USR",
            "FLOWS_030000",
            "APEX_040200",
            "SPATIAL_WFS_ADMIN_USR",
            "DIP",
            "ORACLE_OCM",
            "XS$NULL",
            "INFORMATION_SCHEMA",
            # Additional Oracle system/administrative schemas
            "AUDSYS",  # Oracle audit system
            "DBSFWUSER",  # Database firewall user
            "GGSYS",  # Oracle GoldenGate system
            "GSMADMIN_INTERNAL",  # Global Data Services Manager internal
            "GSMCATUSER",  # GSM catalog user
            "GSMUSER",  # GSM user
            "RDSADMIN",  # RDS administrative schema
            "REMOTE_SCHEDULER_AGENT",  # Oracle scheduler agent
            "SYS$UMF",  # System unified messaging framework
            "SYSBACKUP",  # Oracle backup system
            "SYSDG",  # Oracle Data Guard system
            "SYSKM",  # Oracle Key Management system
            "SYSRAC",  # Oracle RAC system
        ]
        return [schema for schema in schemas if schema.upper() not in [s.upper() for s in exclude_schemas]]

    def discover_catalog_entry(
        self,
        engine: Engine,  # noqa: ARG002
        inspected: Inspector,  # noqa: ARG002
        schema_name: str | None,
        table_name: str,
        is_view: bool,  # noqa: FBT001
        *,
        reflected_columns: list[reflection.ReflectedColumn] | None = None,
        reflected_pk: reflection.ReflectedPrimaryKeyConstraint | None = None,
        reflected_indices: list[reflection.ReflectedIndex] | None = None,
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table or a view.

        For Oracle views, uses Oracle data dictionary views instead of DESCRIBE.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect
            table_name: Name of the table or a view
            is_view: Flag whether this object is a view, returned by `get_object_names`

        Returns:
            `CatalogEntry` object for the given table or a view
        """
        if not is_view:
            return super().discover_catalog_entry(
                engine,
                inspected,
                schema_name,
                table_name,
                is_view,
                reflected_columns=reflected_columns,
                reflected_pk=reflected_pk,
                reflected_indices=reflected_indices,
            )

        unique_stream_id = self.get_fully_qualified_name(
            db_name=None,
            schema_name=schema_name,
            table_name=table_name,
            delimiter="-",
        )

        # Initialize columns list for Oracle views
        table_schema = th.PropertiesList()
        with self._connect() as conn:
            # Use Oracle data dictionary to get column information for views
            oracle_query = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    NULLABLE,
                    DATA_LENGTH,
                    DATA_PRECISION,
                    DATA_SCALE
                FROM ALL_TAB_COLUMNS 
                WHERE OWNER = :schema_name 
                  AND TABLE_NAME = :table_name 
                ORDER BY COLUMN_ID
            """
            columns_result = conn.execute(sa.text(oracle_query), {"schema_name": schema_name or "PUBLIC", "table_name": table_name})
            for column in columns_result:
                column_name = column[0]  # COLUMN_NAME
                data_type = column[1]  # DATA_TYPE
                nullable = column[2]  # NULLABLE
                data_length = column[3]  # DATA_LENGTH
                data_precision = column[4]  # DATA_PRECISION
                data_scale = column[5]  # DATA_SCALE

                # Build Oracle-specific type string
                if data_type == "NUMBER":
                    if data_precision and data_scale:
                        type_str = f"NUMBER({data_precision},{data_scale})"
                    elif data_precision:
                        type_str = f"NUMBER({data_precision})"
                    else:
                        type_str = "NUMBER"
                elif data_type in ["VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"] and data_length:
                    type_str = f"{data_type}({data_length})"
                else:
                    type_str = data_type

                is_nullable = nullable == "Y"
                jsonschema_type: dict = self.to_jsonschema_type(type_str)
                table_schema.append(
                    th.Property(
                        name=column_name,
                        wrapped=th.CustomType(jsonschema_type),
                        required=not is_nullable,
                    ),
                )
        schema = table_schema.to_dict()

        # Initialize available replication methods
        addl_replication_methods: list[str] = [""]  # By default an empty list.
        # Notes regarding replication methods:
        # - 'INCREMENTAL' replication must be enabled by the user by specifying
        #   a replication_key value.
        # - 'LOG_BASED' replication must be enabled by the developer, according
        #   to source-specific implementation capabilities.
        replication_method = next(reversed(["FULL_TABLE", *addl_replication_methods]))

        # Create the catalog entry object
        return CatalogEntry(
            tap_stream_id=str(unique_stream_id),
            stream=str(unique_stream_id),
            table=table_name,
            key_properties=None,
            schema=Schema.from_dict(schema),
            is_view=is_view,
            replication_method=replication_method,
            metadata=MetadataMapping.get_standard_metadata(
                schema_name=schema_name,
                schema=schema,
                replication_method=replication_method,
                key_properties=None,
                valid_replication_keys=None,  # Must be defined by user
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

    def get_sqlalchemy_type(self, col_meta_type: str) -> sa.types.TypeEngine:
        """Return a SQLAlchemy type object for the given Oracle SQL type.

        Used Oracle dialect ischema_names for proper type mapping.
        """
        # Get Oracle dialect from oracledb - this provides the correct type mappings
        try:
            from sqlalchemy.dialects.oracle.oracledb import OracleDialect_oracledb

            dialect = OracleDialect_oracledb()
        except ImportError:
            # Fallback to creating a dummy engine to get the dialect
            dummy_engine = sa.create_engine("oracle+oracledb://user:pass@host:1521/service")
            dialect = dummy_engine.dialect

        ischema_names = dialect.ischema_names

        # Example: NUMBER(10,2), VARCHAR2(100), TIMESTAMP(6)
        type_info = col_meta_type.split("(")
        base_type_name = type_info[0].strip().upper()
        type_args = type_info[1].split(" ")[0].rstrip(")") if len(type_info) > 1 else None

        # Try to get the Oracle type directly from the dialect
        # Most Oracle types in the dialect match the Oracle data dictionary names
        type_class = ischema_names.get(base_type_name)

        # Handle special cases where dialect name differs from Oracle name
        if type_class is None and base_type_name == "UROWID":
            type_class = ischema_names.get("ROWID")  # UROWID maps to ROWID

        # Debug logging
        self.logger.debug(f"Oracle type lookup: {base_type_name} -> {type_class is not None}")

        if type_class is None:
            # Fallback for unmapped types
            self.logger.warning("Unknown Oracle type '%s', falling back to VARCHAR2.", col_meta_type)
            type_class = ischema_names.get("VARCHAR2")
            type_args = None

            # If still None, use SQLAlchemy String as final fallback
            if type_class is None:
                self.logger.warning("Could not find VARCHAR2 type in dialect, using SQLAlchemy String")
                return sa.types.String()

        try:
            # Create an instance of the type class with parameters if they exist
            if type_args:
                # Handle different argument patterns for Oracle types
                if "," in type_args:
                    args = list(map(int, type_args.split(",")))
                    return type_class(*args)
                else:
                    return type_class(int(type_args))
            return type_class()
        except Exception:
            self.logger.exception("Error creating SQLAlchemy type for Oracle col_meta_type=%s", col_meta_type)
            # Return a safe default
            return sa.types.String()

    def get_table_columns(
        self,
        full_table_name: str,
        column_names: list[str] | None = None,
    ) -> dict[str, sa.Column]:
        """Return a dictionary of table columns using Oracle data dictionary.

        Args:
            full_table_name: Fully qualified table name.
            column_names: A list of column names to filter to.

        Returns:
            An ordered dictionary of column objects.
        """
        if full_table_name not in self._table_cols_cache:
            _, schema_name, table_name = self.parse_full_table_name(full_table_name)
            with self._connect() as conn:
                # Use Oracle data dictionary for column metadata
                oracle_query = """
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        NULLABLE,
                        DATA_LENGTH,
                        DATA_PRECISION,
                        DATA_SCALE
                    FROM ALL_TAB_COLUMNS 
                    WHERE OWNER = :schema_name 
                      AND TABLE_NAME = :table_name 
                    ORDER BY COLUMN_ID
                """
                # Oracle stores names in uppercase in data dictionary
                schema_upper = (schema_name or "PUBLIC").upper()
                table_upper = table_name.upper()
                columns_result = conn.execute(sa.text(oracle_query), {"schema_name": schema_upper, "table_name": table_upper})

                columns_dict = {}
                for col_meta in columns_result:
                    column_name = col_meta[0]  # COLUMN_NAME
                    data_type = col_meta[1]  # DATA_TYPE
                    nullable = col_meta[2]  # NULLABLE
                    data_length = col_meta[3]  # DATA_LENGTH
                    data_precision = col_meta[4]  # DATA_PRECISION
                    data_scale = col_meta[5]  # DATA_SCALE

                    # Skip if filtering by column names
                    if column_names and column_name.lower() not in {col.lower() for col in column_names}:
                        continue

                    # Build Oracle-specific type string
                    if data_type == "NUMBER":
                        if data_precision and data_scale:
                            type_str = f"NUMBER({data_precision},{data_scale})"
                        elif data_precision:
                            type_str = f"NUMBER({data_precision})"
                        else:
                            type_str = "NUMBER"
                    elif data_type in ["VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"] and data_length:
                        type_str = f"{data_type}({data_length})"
                    else:
                        type_str = data_type

                    # Normalize column name to lowercase for consistency with discovery
                    normalized_column_name = column_name.lower()
                    columns_dict[normalized_column_name] = sa.Column(
                        normalized_column_name,
                        self.get_sqlalchemy_type(type_str),
                        nullable=nullable == "Y",
                    )

                self._table_cols_cache[full_table_name] = columns_dict

        return self._table_cols_cache[full_table_name]

    def prepare_timestamp_param(self, timestamp_value: Any) -> Any:  # noqa: ANN401
        """Prepare timestamp parameters for Oracle compatibility.

        Oracle expects datetime objects, not strings, for timestamp comparisons.
        This method converts timestamp strings to proper datetime objects using Pendulum for robust parsing.
        """
        if isinstance(timestamp_value, str):
            parsed = pendulum.parse(timestamp_value)
            # Convert to naive datetime for Oracle compatibility
            return parsed.naive()

        elif isinstance(timestamp_value, datetime.datetime):
            # Convert timezone-aware datetime to naive datetime
            if timestamp_value.tzinfo is not None:
                return timestamp_value.replace(tzinfo=None)
            return timestamp_value

        return timestamp_value

    def create_raw_oracle_connection(self):
        """Create a raw Oracle connection for LogMiner operations.

        This method creates a direct Oracle connection using oracledb,
        bypassing SQLAlchemy for operations that require raw Oracle functionality.
        """
        if not oracledb:
            raise ImportError("oracledb is required for raw Oracle connections")

        # Ensure Oracle thick mode is initialized if requested
        if self.config.get("thick_mode", True):
            self._ensure_oracle_thick_mode()

        # Parse the existing SQLAlchemy URL to get connection parameters
        url_obj = make_url(self.sqlalchemy_url)

        # Log connection parameters for debugging (without password)
        internal_logger.debug(
            "Creating raw Oracle connection with: host=%s, port=%s, user=%s, database=%s", url_obj.host, url_obj.port, url_obj.username, url_obj.database
        )

        # Build Oracle connection string
        if url_obj.password:
            connection_string = f"{url_obj.username}/{url_obj.password}@{url_obj.host}:{url_obj.port}/{url_obj.database}"
        else:
            # Handle case where password might be in config
            password = self.config.get("password", "")
            connection_string = f"{url_obj.username}/{password}@{url_obj.host}:{url_obj.port}/{url_obj.database}"

        internal_logger.debug("Oracle connection string: %s", connection_string.replace(url_obj.password or password, "***"))

        # Create raw Oracle connection
        try:
            connection = oracledb.connect(connection_string)
            internal_logger.info("Successfully created raw Oracle connection for LogMiner")
        except Exception as e:
            internal_logger.error("Failed to create Oracle connection: %s", e)
            internal_logger.error("Connection string: %s", connection_string.replace(url_obj.password or password, "***"))
            raise

        # Set session parameters for LogMiner compatibility
        cursor = connection.cursor()
        try:
            cursor.execute("ALTER SESSION SET TIME_ZONE = '00:00'")
            cursor.execute("""ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS."00+00:00"'""")
            cursor.execute("""ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD"T"HH24:MI:SSXFF"+00:00"'""")
            cursor.execute("""ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT  = 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'""")
            internal_logger.debug("Set LogMiner-compatible session parameters")
        except Exception as e:
            internal_logger.warning("Failed to set some session parameters: %s", e)
        finally:
            cursor.close()

        return connection

    def _ensure_oracle_thick_mode(self):
        """Ensure Oracle thick mode is initialized before creating connections.

        This method must be called before any Oracle connections are made
        to ensure consistent thick mode usage throughout the process.
        """
        if not oracledb:
            return

        try:
            # Check if thick mode is already initialized
            if hasattr(oracledb, "_thick_mode_initialized"):
                return

            # Initialize Oracle thick mode
            lib_dir = self._get_oracle_lib_dir()
            if lib_dir:
                oracledb.init_oracle_client(lib_dir=lib_dir)
                internal_logger.info("Oracle thick mode initialized with lib_dir: %s", lib_dir)
            else:
                # Try to initialize without specifying lib_dir (let oracledb find it)
                oracledb.init_oracle_client()
                internal_logger.info("Oracle thick mode initialized (auto-detected lib_dir)")

            # Mark as initialized to prevent duplicate calls
            oracledb._thick_mode_initialized = True

        except Exception as e:
            internal_logger.warning("Failed to initialize Oracle thick mode: %s", e)
            # Don't raise - allow thin mode to be used as fallback

    def _get_oracle_lib_dir(self) -> str | None:
        """Get Oracle library directory from environment variables."""
        # Check common environment variables for Oracle libraries
        for env_var in ["LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH"]:
            lib_paths = os.environ.get(env_var, "").split(":")
            for lib_path in lib_paths:
                if lib_path:
                    # Look for Oracle client library
                    for lib_name in ["libclntsh.so", "libclntsh.dylib"]:
                        lib_file = os.path.join(lib_path, lib_name)
                        if os.path.exists(lib_file):
                            return lib_path
        return None

    def create_engine(self) -> Engine:
        """Create Oracle database engine with appropriate connection parameters."""

        # Initialize Oracle thick mode if requested and oracledb is available
        if self.config.get("thick_mode", True) and oracledb:
            self._ensure_oracle_thick_mode()

        try:
            # Create engine without thick_mode parameter (handled by init_oracle_client)

            return sa.create_engine(
                self.sqlalchemy_url,
                echo=False,
                poolclass=QueuePool,
                pool_size=self.pool_size,
                max_overflow=self.pool_size * 2,
                pool_recycle=300,
                pool_pre_ping=True,
            )
        except TypeError:
            # Fall back without thick_mode if not supported by oracledb version
            internal_logger.info(
                "Creating Oracle engine without thick_mode parameter (older oracledb version).",
            )
            try:
                return sa.create_engine(
                    self.sqlalchemy_url,
                    echo=False,
                    json_serializer=self.serialize_json,
                    json_deserializer=self.deserialize_json,
                    poolclass=QueuePool,
                    pool_size=self.pool_size,
                    max_overflow=self.pool_size * 2,
                    pool_recycle=300,
                    pool_pre_ping=True,
                )
            except TypeError:
                # Final fallback with minimal parameters
                internal_logger.exception(
                    "Creating Oracle engine with minimal parameters due to TypeError.",
                )
                return sa.create_engine(
                    self.sqlalchemy_url,
                    echo=False,
                )
