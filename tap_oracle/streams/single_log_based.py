"""SQL client handling."""

from __future__ import annotations

import functools
import sys
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generator

from nekt_singer_sdk import SQLStream, metrics
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk.helpers._state import increment_state
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel

from tap_oracle.connector import OracleConnector

if TYPE_CHECKING:
    from collections.abc import Iterable

    from nekt_singer_sdk.helpers import types
    from nekt_singer_sdk.tap_base import Tap

    from tap_oracle.streams import OracleLogBasedStream


class OracleSingleLogBasedStream(SQLStream):
    """Stream class for Oracle streams."""

    connector_class = OracleConnector
    replication_key = "_sdc_lsn"  # Use _sdc_lsn as replication key for Oracle log mining
    log_based_streams: list["OracleLogBasedStream"] = []

    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    def __init__(
        self,
        tap: "Tap",
        connector: OracleConnector | None = None,
        log_based_streams: list["OracleLogBasedStream"] = [],
    ):
        super().__init__(
            tap=tap,
            catalog_entry={},
            connector=connector,
        )
        self.log_based_streams = log_based_streams

    @functools.cached_property
    def schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
            },
            "required": ["name"],
        }

    @property
    def tap_stream_id(self) -> str:
        return "single_log_based"

    @property
    def selected(self) -> bool:
        return True

    def write_all_schema_messages(self) -> None:
        for stream in self.log_based_streams:
            if stream.selected:
                stream._write_schema_message()

    def write_all_replication_key_signposts(self, context: types.Context | None = None) -> None:
        for stream in self.log_based_streams:
            signpost = stream.get_replication_key_signpost(context)
            if signpost:
                stream._write_replication_key_signpost(context, signpost)

    def handle_record(
        self,
        record: dict,
        stream_name: str,
        current_context: types.Context | None = None,
        record_index: int = 0,
        write_messages: bool = True,
        record_counter: metrics.RecordCounter | None = None,
    ) -> Generator[dict]:
        stream = [stream for stream in self.log_based_streams if stream.name == stream_name][0]
        if stream.selected:
            if write_messages:
                stream._write_record_message(record)

            self._increment_stream_state(record, context=current_context)
            if (record_index + 1) % self.STATE_MSG_FREQUENCY == 0 and write_messages:
                self._write_state_message()

            record_counter.increment()
            yield record

    def sync(self, context: types.Context | None = None) -> None:
        """Sync this stream.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            context: Stream partition or context dictionary.
        """
        msg = f"Beginning LOG_BASED syncs for {len(self.log_based_streams)} streams"
        if context:
            msg += f" with context: {context}"
        internal_logger.info("%s...", msg)
        self.context = MappingProxyType(context) if context else None

        # Use a replication signpost, if available
        self.write_all_replication_key_signposts(context)

        # Send a SCHEMA message to the downstream target:
        self.write_all_schema_messages()

        try:
            # Sync the records themselves:
            for _ in self._sync_records(context=context):
                pass
        except Exception:
            user_logger.exception("An unhandled error occurred while syncing log-based streams")
            sys.exit(1)

    def fast_forward_to_latest_scn(self):
        """Fast-forward the stream state to the latest SCN position."""
        internal_logger.info("Fast-forwarding stream state to the latest SCN position.")
        current_context = None
        state = self.get_context_state(current_context)
        self._get_state_partition_context(
            current_context,
        )
        self._write_starting_replication_value(current_context)

        current_scn = self._fetch_current_scn()
        user_logger.info("Fast-forwarding to latest SCN: %s", current_scn)

        fake_record = {self.replication_key: current_scn}

        treat_as_sorted = self.is_sorted

        increment_state(
            state,
            replication_key=self.replication_key,
            latest_record=fake_record,
            is_sorted=treat_as_sorted,
            check_sorted=self.check_sorted,
        )

        self._finalize_state(state)
        self._write_state_message()
        user_logger.info(f"State fast-forwarded to SCN {current_scn}.")

    def _sync_records(  # noqa: C901
        self,
        context: types.Context | None = None,
        *,
        write_messages: bool = True,
    ) -> Generator[dict, Any, Any]:
        # Initialize metrics
        record_counter = metrics.record_counter(self.name)
        timer = metrics.sync_timer(self.name)

        record_index = 0
        context_element: types.Context | None
        context_list: list[types.Context] | list[dict] | None = None

        with record_counter, timer:
            for context_element in context_list or [{}]:
                record_counter.context = context_element
                timer.context = context_element

                current_context = context_element or None
                state = self.get_context_state(current_context)
                state_partition_context = self._get_state_partition_context(
                    current_context,
                )
                self._write_starting_replication_value(current_context)

                for _, record_result in enumerate(self.get_records(current_context)):
                    record, stream_name = record_result
                    yield from self.handle_record(record, stream_name, current_context, record_index, write_messages, record_counter)
                    record_index += 1

                if current_context == state_partition_context:
                    # Finalize per-partition state only if 1:1 with context
                    self._finalize_state(state)

        if not context:
            # Finalize total stream only if we have the full context.
            # Otherwise will be finalized by tap at end of sync.
            self._finalize_state(self.stream_state)

        if write_messages:
            # Write final state message if we haven't already
            self._write_state_message()

    def get_records(self, context: dict | None) -> Iterable[tuple[dict, str]]:
        """Get records from Oracle LogMiner for all log-based streams.

        This method:
        1. Fetches the current SCN from the database
        2. Starts LogMiner from the last SCN to current SCN
        3. Queries log contents for all streams
        4. Yields (record, stream_name) tuples
        """
        # Get the starting SCN from state (stored as _sdc_lsn but represents Oracle SCN)
        start_scn = self.get_starting_replication_key_value(context=context)
        if start_scn is None:
            # First run - start from current SCN
            start_scn = self._fetch_current_scn()
            user_logger.info("First run, starting from current SCN: %s", start_scn)
        else:
            user_logger.info("Starting log mining from SCN: %s", start_scn)

        # Get current SCN
        current_scn = self._fetch_current_scn()
        user_logger.info("Current SCN: %s", current_scn)

        if start_scn >= current_scn:
            user_logger.info("No new changes to process (start_scn >= current_scn)")
            return

            # Get database connection for LogMiner operations
        try:
            connection = self.connector.create_raw_oracle_connection()
        except Exception as e:
            user_logger.error("Failed to create Oracle connection for LogMiner: %s", e)
            raise

        try:
            # Start LogMiner
            self._start_logminer(connection, start_scn, current_scn)

            # Process changes for all streams
            for stream in self.log_based_streams:
                if not stream.selected:
                    continue

                user_logger.info("Processing log changes for stream: %s", stream.name)

                # Get the stream's schema and table info from metadata
                # Access metadata directly from the stream object
                schema_name = stream.metadata.get(()).get("schema-name") if stream.metadata else None
                table_name = stream.table

                if not schema_name:
                    user_logger.warning("Could not determine schema name for stream %s", stream.name)
                    continue

                # Get desired columns for this stream
                desired_columns = list(stream.schema.properties.keys())

                # Build the LogMiner query
                mine_sql = self._build_logminer_query(desired_columns)

                # Execute the query
                cursor = connection.cursor()
                try:
                    cursor.execute(mine_sql, {"table_name": table_name, "schema_name": schema_name})

                    for row in cursor:
                        operation, sql_redo, scn, cscn, commit_timestamp, *col_vals = row

                        # Split column values into redo and undo
                        redo_vals = col_vals[: len(desired_columns)]
                        undo_vals = col_vals[len(desired_columns) :]

                        # Create record based on operation type
                        if operation in ("INSERT", "UPDATE"):
                            # Use redo values (new values)
                            record = dict(zip(desired_columns, redo_vals))
                            record["_sdc_lsn"] = cscn  # Use commit SCN as LSN
                            record["_sdc_deleted_at"] = None
                        elif operation == "DELETE":
                            # Use undo values (old values) and mark as deleted
                            record = dict(zip(desired_columns, undo_vals))
                            record["_sdc_lsn"] = cscn
                            record["_sdc_deleted_at"] = commit_timestamp.isoformat() if commit_timestamp else None
                        else:
                            user_logger.warning("Unknown operation type: %s", operation)
                            continue

                        # Add metadata
                        record["_sdc_operation"] = operation
                        record["_sdc_scn"] = scn
                        record["_sdc_commit_timestamp"] = commit_timestamp.isoformat() if commit_timestamp else None

                        yield record, stream.name

                finally:
                    cursor.close()

        finally:
            # Stop LogMiner
            self._stop_logminer(connection)
            connection.close()

    def _fetch_current_scn(self) -> int:
        """Fetch the current SCN from the database."""
        connection = self.connector.create_raw_oracle_connection()
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT current_scn FROM V$DATABASE")
            current_scn = cursor.fetchone()[0]
            cursor.close()
            return current_scn
        except Exception as e:
            user_logger.error("Failed to fetch current SCN: %s", e)
            raise
        finally:
            connection.close()

    def _start_logminer(self, connection, start_scn: int, end_scn: int) -> None:
        """Start Oracle LogMiner for the specified SCN range."""
        start_logmnr_sql = """BEGIN
                             DBMS_LOGMNR.START_LOGMNR(
                                     startScn => :start_scn,
                                     endScn => :end_scn,
                                     OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                                                DBMS_LOGMNR.COMMITTED_DATA_ONLY +
                                                DBMS_LOGMNR.CONTINUOUS_MINE);
                             END;"""

        user_logger.info("Starting LogMiner from SCN %s to %s", start_scn, end_scn)
        cursor = connection.cursor()
        try:
            cursor.execute(start_logmnr_sql, {"start_scn": start_scn, "end_scn": end_scn})
        finally:
            cursor.close()

    def _stop_logminer(self, connection) -> None:
        """Stop Oracle LogMiner."""
        cursor = connection.cursor()
        try:
            cursor.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); END;")
            user_logger.info("LogMiner stopped")
        finally:
            cursor.close()

    def _build_logminer_query(self, desired_columns: list[str]) -> str:
        """Build the LogMiner query to extract column values."""
        # Build clauses for extracting redo and undo values
        redo_clauses = []
        undo_clauses = []

        for col in desired_columns:
            redo_clauses.append(f"DBMS_LOGMNR.MINE_VALUE(REDO_VALUE, '{col}') as {col}_redo")
            undo_clauses.append(f"DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE, '{col}') as {col}_undo")

        redo_clause = ",\n ".join(redo_clauses)
        undo_clause = ",\n ".join(undo_clauses)

        query = f"""
        SELECT 
            OPERATION,
            SQL_REDO,
            SCN,
            CSCN,
            COMMIT_TIMESTAMP,
            {redo_clause},
            {undo_clause}
        FROM v$logmnr_contents 
        WHERE table_name = :table_name 
          AND seg_owner = :schema_name 
          AND operation IN ('INSERT', 'UPDATE', 'DELETE')
        ORDER BY SCN, CSCN
        """

        return query

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        if "_sdc_lsn" in row:
            row["_sdc_lsn"] = str(row["_sdc_lsn"])
        return row

    @property
    def is_sorted(self) -> bool:
        return True

    def _increment_stream_state(
        self,
        latest_record: types.Record,
        *,
        context: types.Context | None = None,
    ) -> None:
        # This also creates a state entry if one does not yet exist:
        state_dict = self.get_context_state(context)

        # Advance state bookmark values if applicable
        if latest_record:
            if not self.replication_key:
                msg = f"Could not detect replication key for '{self.name}' stream(replication method={self.replication_method})"
                raise ValueError(msg)

            if self.replication_key in latest_record:
                # Convert SCN to LSN format if needed
                lsn_value = latest_record[self.replication_key]
                if isinstance(lsn_value, str):
                    latest_record[self.replication_key] = int(lsn_value)

            treat_as_sorted = self.is_sorted
            if not treat_as_sorted and self.state_partitioning_keys is not None:
                # Streams with custom state partitioning are not resumable.
                treat_as_sorted = False
            increment_state(
                state_dict,
                replication_key=self.replication_key,
                latest_record=latest_record,
                is_sorted=treat_as_sorted,
                check_sorted=self.check_sorted,
            )
