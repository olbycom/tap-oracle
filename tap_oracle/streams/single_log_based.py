"""SQL client handling."""

from __future__ import annotations

import functools
import random
import re
import sys
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generator

from dateutil import parser
from nekt_singer_sdk import SQLStream, metrics
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk.helpers._state import increment_state
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel
from sqlalchemy import text
from sqlalchemy.engine.url import make_url

from tap_oracle.connector import OracleConnector

if TYPE_CHECKING:
    from collections.abc import Iterable

    from nekt_singer_sdk.helpers import types
    from nekt_singer_sdk.tap_base import Tap

    from tap_oracle.streams import OracleLogBasedStream


class OracleSingleLogBasedStream(SQLStream):
    """Stream class for Oracle streams."""

    connector_class = OracleConnector
    replication_key = "_sdc_lsn"
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

    def fast_forward_to_latest_log_position(self):
        """Fast-forward the stream state to the latest log position."""
        pass

    def get_min_server_log_file_and_pos(self):
        pass

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

    def create_unique_identifier(self, file_name: str, position: int, row_index: int = 0) -> int:
        """Create a unique 64-bit integer to represent the LSN.

        This is composed of:
        - binlog file number (top 16 bits)
        - binlog position (middle 32 bits)
        - row index within the event (bottom 16 bits)
        """
        match = re.search(r"\d+$", file_name)
        if not match:
            msg = f"Could not extract file number from binlog file name: {file_name}"
            raise ValueError(msg)

        file_number = int(match.group())

        if file_number >= (1 << 16):
            self.logger.warning("Binlog file number %s exceeds the 16-bit allocation.", file_number)
        if position >= (1 << 32):
            self.logger.warning("Binlog position %s exceeds the 32-bit allocation.", position)
        if row_index >= (1 << 16):
            self.logger.warning(
                "Row index %s exceeds the 16-bit allocation. LSN may not be unique for this event.",
                row_index,
            )

        # Compose the LSN by bit-shifting the components
        file_part = file_number << 48
        pos_part = position << 16
        row_part = row_index

        return file_part + pos_part + row_part

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        start_lsn = self.get_starting_replication_key_value(context=context)
        if start_lsn:
            log_file, log_pos = self.get_log_file_from_lsn(start_lsn)
        else:
            log_file, log_pos = self.get_min_server_log_file_and_pos()

        pass

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

            if self.replication_key in latest_record and isinstance(latest_record[self.replication_key], str):
                latest_record[self.replication_key] = int(latest_record[self.replication_key])

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
