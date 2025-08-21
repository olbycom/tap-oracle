"""SQL client handling."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from nekt_singer_sdk import SQLStream
from nekt_singer_sdk.custom_logger import user_logger
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel

from tap_oracle.connector import OracleConnector

if TYPE_CHECKING:
    from collections.abc import Iterable


class OracleStream(SQLStream):
    """Stream class for Oracle streams."""

    connector_class = OracleConnector

    # Oracle LOB and complex objects require ROOT_ONLY conformance level
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Get records from Oracle table with optimized execution.

        Args:
            context: Stream partition context (not supported for Oracle streams)

        Returns:
            Iterable of record dictionaries

        Raises:
            SystemExit: If partitioning context is provided (not supported)
        """
        if context:
            msg = f"Oracle stream '{self.name}' does not support partitioning."
            self._tap.user_logger.error(msg)
            sys.exit(1)

        # Oracle optimization: pull only selected columns to reduce network traffic
        selected_column_names = list(self.get_selected_schema()["properties"])

        # Fallback to all columns if none selected
        if not selected_column_names:
            user_logger.warning(f"No columns selected for {self.name}, falling back to all columns")
            selected_column_names = None

        table = self.connector.get_table(
            self.fully_qualified_name,
            column_names=selected_column_names,
        )

        # Log Oracle-specific table information for debugging
        col_count = len(selected_column_names) if selected_column_names else "all"
        user_logger.debug(f"Oracle stream '{self.name}': selecting {col_count} columns from {self.fully_qualified_name}")

        query = table.select()
        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]

            # Oracle-specific optimization: use index hints if available
            # This helps Oracle optimizer choose the right execution plan
            query = query.order_by(replication_key_col)

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                # For Oracle, consider using bind variables for better performance
                query = query.where(replication_key_col >= start_val)

        with self.connector._connect() as conn:  # noqa: SLF001
            user_logger.info(f"Getting records for Oracle query: '{query}'")

            # Oracle-specific execution options for optimal performance
            execution_options = {
                "stream_results": True,
                "compiled_cache": {},  # Enable statement caching for Oracle
            }

            # For Oracle, we can add arraysize for better fetch performance
            chunk_size = self.config.get("chunk_size", 0)
            if chunk_size > 0:
                execution_options["arraysize"] = min(chunk_size, 10000)  # Oracle optimal range
            else:
                execution_options["arraysize"] = 5000  # Oracle default optimization

            try:
                # For Oracle thick mode, we need to handle timestamp parameters specially
                if hasattr(self.connector, "prepare_timestamp_param") and hasattr(query, "compile"):
                    # Get compiled query with parameters
                    compiled = query.compile(compile_kwargs={"literal_binds": False})
                    if compiled.params:
                        # Process timestamp parameters for Oracle compatibility
                        processed_params = {}
                        for key, value in compiled.params.items():
                            if "timestamp" in key.lower() or "date" in key.lower():
                                processed_params[key] = self.connector.prepare_timestamp_param(value)
                            else:
                                processed_params[key] = value

                        # Execute with processed parameters
                        result = conn.execution_options(**execution_options).execute(query, processed_params)
                    else:
                        result = conn.execution_options(**execution_options).execute(query)
                else:
                    result = conn.execution_options(**execution_options).execute(query)

                if chunk_size > 0:
                    result = result.yield_per(chunk_size)

                for record in result.mappings():
                    # TODO: Standardize record mapping type
                    # https://github.com/meltano/sdk/issues/2096
                    transformed_record = self.post_process(dict(record))
                    if transformed_record is None:
                        # Record filtered out during post_process()
                        continue
                    yield transformed_record

            except Exception as e:
                # Enhanced error handling for Oracle-specific errors
                error_msg = str(e).lower()
                if "ora-" in error_msg:
                    # Oracle error codes for better debugging
                    if "ora-00942" in error_msg:
                        user_logger.error(f"Oracle table or view does not exist: {self.fully_qualified_name}")
                    elif "ora-00904" in error_msg:
                        user_logger.error(f"Oracle invalid column name in query for table: {self.fully_qualified_name}")
                    elif "ora-01722" in error_msg:
                        user_logger.error(f"Oracle invalid number conversion in table: {self.fully_qualified_name}")
                    elif "ora-12541" in error_msg or "ora-12170" in error_msg:
                        user_logger.error("Oracle connection timeout or network error")
                    else:
                        user_logger.error(f"Oracle database error: {e}")
                else:
                    user_logger.error(f"Error executing Oracle query: {e}")
                raise
