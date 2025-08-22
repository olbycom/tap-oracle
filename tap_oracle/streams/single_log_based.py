"""SQL client handling."""

from __future__ import annotations

import functools
import sys
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generator

import pendulum
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
        self._current_start_scn = None
        self._current_end_scn = None

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

        This method processes LogMiner data in batches to avoid infinite processing.
        It follows Oracle best practices for LogMiner SCN range management.
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
            
        # Find the actual available SCN range from logs
        connection = self.connector.create_raw_oracle_connection()
        try:
            available_start, available_end = self._get_available_scn_range(connection, start_scn, current_scn)
            if available_start is None:
                user_logger.warning("No archived logs available for processing. This may be normal for a new database.")
                return
                
            user_logger.info("Available SCN range in logs: %s-%s", available_start, available_end)
            
            # Adjust our range to what's actually available
            start_scn = max(start_scn, available_start)
            current_scn = min(current_scn, available_end)
            
            if start_scn >= current_scn:
                user_logger.info("No logs available for our SCN range")
                return
                
        finally:
            connection.close()

        # Process LogMiner data in batches for better performance and resource management
        # This follows Oracle LogMiner best practices
        batch_size = self.config.get("logminer_batch_size", 10000)  # Default 10000 SCNs per batch
        
        user_logger.info("Processing LogMiner in batches of %d SCNs until current SCN %s", batch_size, current_scn)
        
        batch_start = start_scn
        batch_count = 0
        
        # Process all batches until we reach the current SCN
        # Never stop early - Oracle's current_scn is the definitive end point
        
        while batch_start < current_scn:
            batch_end = min(batch_start + batch_size, current_scn)
            batch_count += 1
            
            user_logger.info("Processing LogMiner batch %d: SCN range %s-%s", 
                           batch_count, batch_start, batch_end)
            
            # Process this batch (even if empty - gaps are normal in Oracle)
            records_processed = 0
            for record_tuple in self._process_logminer_batch(batch_start, batch_end):
                records_processed += 1
                yield record_tuple
            
            user_logger.info("Batch %d completed: processed %d records", batch_count, records_processed)
            
            # Move to next batch - always continue to current_scn
            batch_start = batch_end
        
        # Ensure state is updated to the full range processed (current_scn)
        # This prevents reprocessing empty SCN ranges on next sync
        if batch_count > 0:  # Only if we processed at least one batch
            final_state_record = {self.replication_key: current_scn}
            self._increment_stream_state(final_state_record, context=None)
            user_logger.info("Updated state to processed SCN range end: %s", current_scn)
        
        user_logger.info("Completed processing all LogMiner data: %d batches, processed up to SCN %s", batch_count, current_scn)

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

    def _try_start_logminer_simple(self, connection, start_scn: int, end_scn: int) -> bool:
        """Try to start LogMiner with the simple approach (no log file management).

        This approach lets Oracle automatically find the necessary log files.
        Works for most Oracle environments including properly configured AWS RDS.
        """
        # First, check if we have logs available for this SCN range
        if not self._check_logs_available_for_scn_range(connection, start_scn, end_scn):
            user_logger.warning("No logs available for SCN range %s-%s", start_scn, end_scn)
            return False
            
        start_logmnr_sql = """BEGIN
                             DBMS_LOGMNR.START_LOGMNR(
                                     startScn => :start_scn,
                                     endScn => :end_scn,
                                     OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                                                DBMS_LOGMNR.COMMITTED_DATA_ONLY);
                             END;"""

        user_logger.info("Trying simple LogMiner start for SCN range %s-%s", start_scn, end_scn)
        cursor = connection.cursor()
        try:
            cursor.execute(start_logmnr_sql, {"start_scn": start_scn, "end_scn": end_scn})
            user_logger.info("Simple LogMiner start successful")
            return True
        except Exception as e:
            user_logger.info("Simple LogMiner start failed: %s", str(e))
            # Try to clean up in case LogMiner is in a partial state
            try:
                cursor.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); END;")
            except Exception:
                pass
            return False
        finally:
            cursor.close()

    def _try_start_logminer_with_fallbacks(self, connection, start_scn: int, end_scn: int) -> bool:
        """Try fallback strategies for starting LogMiner.

        For AWS RDS, we need to explicitly add archived log files.
        This method finds and adds the appropriate log files.
        """
        cursor = connection.cursor()

        try:
            # AWS RDS Strategy: Find and explicitly add archived log files
            user_logger.info("AWS RDS Fallback: Finding archived logs that contain our SCN range...")

            # Find archived logs that overlap with our SCN range, but limit to recent logs
            cursor.execute(
                """
                SELECT sequence#, first_change#, next_change#, name, thread#,
                       TO_CHAR(first_time, 'YYYY-MM-DD HH24:MI:SS') as first_time_str
                FROM v$archived_log 
                WHERE status = 'A'  -- Available
                  AND first_change# IS NOT NULL
                  AND next_change# IS NOT NULL
                  AND first_time > SYSDATE - 7  -- Only consider logs from last 7 days
                  AND (
                    (first_change# <= :start_scn AND next_change# > :start_scn) OR  -- Contains start SCN
                    (first_change# < :end_scn AND next_change# >= :end_scn) OR     -- Contains end SCN  
                    (first_change# >= :start_scn AND next_change# <= :end_scn) OR  -- Fully within range
                    (first_change# <= :start_scn AND next_change# >= :end_scn)     -- Fully contains range
                  )
                ORDER BY first_change# ASC
            """,
                {"start_scn": start_scn, "end_scn": end_scn},
            )

            archived_logs = cursor.fetchall()

            if not archived_logs:
                user_logger.info("No archived logs found for requested SCN range, trying most recent logs...")
                # Try to find the most recent archived logs available
                cursor.execute("""
                    SELECT sequence#, first_change#, next_change#, name, thread#,
                           TO_CHAR(first_time, 'YYYY-MM-DD HH24:MI:SS') as first_time_str
                    FROM v$archived_log 
                    WHERE status = 'A'  -- Available
                      AND first_time > SYSDATE - 1  -- Last 24 hours
                      AND first_change# IS NOT NULL
                      AND next_change# IS NOT NULL
                    ORDER BY first_change# DESC
                    FETCH FIRST 5 ROWS ONLY
                """)
                archived_logs = cursor.fetchall()

                if archived_logs:
                    # Use the SCN range from the most recent logs
                    most_recent = archived_logs[0]
                    new_start_scn = most_recent[1]  # first_change#
                    new_end_scn = most_recent[2]  # next_change#
                    user_logger.info("Adjusting to available archived log SCN range: %s-%s (was %s-%s)", 
                                   new_start_scn, new_end_scn, start_scn, end_scn)
                    start_scn = new_start_scn
                    end_scn = new_end_scn

            if archived_logs:
                user_logger.info("Found %d archived logs for LogMiner:", len(archived_logs))
                for log in archived_logs:
                    seq, first_scn, next_scn, name, thread, time_str = log
                    user_logger.info("  Seq %s: SCN %s-%s, Thread %s, Time %s", seq, first_scn, next_scn, thread, time_str)

                # Try to add archived logs and start LogMiner
                try:
                    # Add the first log file with NEW option
                    first_log = archived_logs[0]
                    user_logger.info("Adding first archived log file: %s", first_log[3])

                    add_logfile_sql = """BEGIN
                                        DBMS_LOGMNR.ADD_LOGFILE(
                                            LOGFILENAME => :filename,
                                            OPTIONS => DBMS_LOGMNR.NEW);
                                        END;"""
                    cursor.execute(add_logfile_sql, {"filename": first_log[3]})

                    # Add additional log files with ADDFILE option
                    for log in archived_logs[1:]:
                        user_logger.info("Adding additional archived log file: %s", log[3])
                        add_logfile_sql = """BEGIN
                                            DBMS_LOGMNR.ADD_LOGFILE(
                                                LOGFILENAME => :filename,
                                                OPTIONS => DBMS_LOGMNR.ADDFILE);
                                            END;"""
                        cursor.execute(add_logfile_sql, {"filename": log[3]})

                    # Now try to start LogMiner
                    start_logmnr_sql = """BEGIN
                                         DBMS_LOGMNR.START_LOGMNR(
                                                 startScn => :start_scn,
                                                 endScn => :end_scn,
                                                 OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                                                            DBMS_LOGMNR.COMMITTED_DATA_ONLY);
                                         END;"""

                    cursor.execute(start_logmnr_sql, {"start_scn": start_scn, "end_scn": end_scn})
                    user_logger.info("AWS RDS LogMiner started successfully with archived logs!")

                    # Update the SCN range in case we adjusted it
                    self._current_start_scn = start_scn
                    self._current_end_scn = end_scn
                    return True

                except Exception as e:
                    user_logger.error("Failed to start LogMiner with archived logs: %s", str(e))
                    # Clean up - try to stop LogMiner in case it's in a partial state
                    try:
                        cursor.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); END;")
                    except Exception:
                        pass
                    return False
            else:
                user_logger.error("No archived logs available for LogMiner")
                return False

        except Exception as e:
            user_logger.error("Error in AWS RDS fallback strategy: %s", e)
            return False
        finally:
            cursor.close()

    def _provide_logminer_guidance(self) -> None:
        """Provide guidance for LogMiner configuration issues."""
        user_logger.error("=== LogMiner Configuration Issues ===\n")
        user_logger.error("LogMiner failed to start. Common solutions:\n")
        user_logger.error("1. SUPPLEMENTAL LOGGING: Ensure it's enabled")
        user_logger.error("   ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n")
        user_logger.error("2. ARCHIVE LOG MODE: Database must be in archivelog mode")
        user_logger.error("   ALTER DATABASE ARCHIVELOG;\n")
        user_logger.error("3. LOG RETENTION: Ensure archive logs are retained long enough\n")
        user_logger.error("4. PERMISSIONS: User needs LogMiner privileges")
        user_logger.error("   GRANT EXECUTE ON DBMS_LOGMNR TO username;\n")

        # Detect if this might be AWS RDS and provide specific guidance
        try:
            connection = self.connector.create_raw_oracle_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT value FROM v$parameter WHERE name = 'db_unique_name'")
            db_name = cursor.fetchone()[0]

            if db_name and ("ORCL_A" in db_name or "RDS" in db_name.upper()):
                user_logger.error("AWS RDS SPECIFIC STEPS:")
                user_logger.error("1. Enable supplemental logging:")
                user_logger.error("   exec rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action=>'ADD');\n")
                user_logger.error("2. Enable force logging:")
                user_logger.error("   exec rdsadmin.rdsadmin_util.force_logging(p_enable => true);\n")
                user_logger.error("3. Set backup retention > 0:")
                user_logger.error("   aws rds modify-db-instance --backup-retention-period 1\n")
                user_logger.error("4. Grant LogMiner access:")
                user_logger.error("   exec rdsadmin.rdsadmin_util.grant_sys_object('DBMS_LOGMNR', 'username', 'EXECUTE', true);")

            cursor.close()
            connection.close()
        except Exception:
            pass  # Ignore errors in guidance detection

        user_logger.error("========================================")
        
    def _check_logs_available_for_scn_range(self, connection, start_scn: int, end_scn: int) -> bool:
        """Check if archived logs are available for the given SCN range."""
        cursor = connection.cursor()
        try:
            # Check archived logs
            cursor.execute(
                """
                SELECT COUNT(*) 
                FROM v$archived_log 
                WHERE status = 'A'  -- Available
                  AND first_change# IS NOT NULL
                  AND next_change# IS NOT NULL
                  AND (
                    (first_change# <= :start_scn AND next_change# > :start_scn) OR  -- Contains start SCN
                    (first_change# < :end_scn AND next_change# >= :end_scn) OR     -- Contains end SCN  
                    (first_change# >= :start_scn AND next_change# <= :end_scn) OR  -- Fully within range
                    (first_change# <= :start_scn AND next_change# >= :end_scn)     -- Fully contains range
                  )
                """,
                {"start_scn": start_scn, "end_scn": end_scn},
            )
            
            count = cursor.fetchone()[0]
            return count > 0
            
        except Exception as e:
            user_logger.warning("Error checking log availability: %s", e)
            return False
        finally:
            cursor.close()
            
    def _get_available_scn_range(self, connection, requested_start: int, requested_end: int) -> tuple[int | None, int | None]:
        """Get the actual SCN range available in archived logs."""
        cursor = connection.cursor()
        try:
            # Find the earliest and latest SCN available in archived logs
            cursor.execute(
                """
                SELECT MIN(first_change#), MAX(next_change#)
                FROM v$archived_log 
                WHERE status = 'A'  -- Available
                  AND first_change# IS NOT NULL
                  AND next_change# IS NOT NULL
                  AND first_time > SYSDATE - 7  -- Only consider logs from last 7 days
                """
            )
            
            result = cursor.fetchone()
            if result and result[0] is not None and result[1] is not None:
                min_scn, max_scn = result
                user_logger.info("Available SCN range in archived logs: %s-%s", min_scn, max_scn)
                return int(min_scn), int(max_scn)
            else:
                user_logger.warning("No available archived logs found")
                return None, None
                
        except Exception as e:
            user_logger.warning("Error getting available SCN range: %s", e)
            return None, None
        finally:
            cursor.close()

    def _stop_logminer(self, connection) -> None:
        """Stop Oracle LogMiner safely."""
        if not connection:
            return

        cursor = connection.cursor()
        try:
            cursor.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); END;")
            user_logger.info("LogMiner stopped successfully")
        except Exception as e:
            # ORA-01307 means no LogMiner session is active, which is fine
            if "ORA-01307" in str(e):
                user_logger.debug("LogMiner was not active (ORA-01307)")
            else:
                user_logger.warning("Error stopping LogMiner: %s", str(e))
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def _process_logminer_batch(self, start_scn: int, end_scn: int) -> Iterable[tuple[dict, str]]:
        """Process a single LogMiner batch with proper resource management."""
        connection = None
        try:
            connection = self.connector.create_raw_oracle_connection()

            # Initialize SCN range tracking
            self._current_start_scn = start_scn
            self._current_end_scn = end_scn

            # Try to start LogMiner with the simple approach first
            success = self._try_start_logminer_simple(connection, start_scn, end_scn)

            if not success:
                user_logger.warning("Simple LogMiner approach failed, trying fallback strategies...")
                # Try fallback strategies for different Oracle environments
                success = self._try_start_logminer_with_fallbacks(connection, start_scn, end_scn)

            if not success:
                user_logger.error("LogMiner could not be started with any available strategy")
                self._provide_logminer_guidance()
                sys.exit(1)

            # Process all streams for this batch
            yield from self._process_all_streams_logminer(connection, start_scn, end_scn)
            
        except Exception as e:
            user_logger.error("Error during LogMiner batch processing: %s", e)
            raise
        finally:
            # Stop LogMiner and close connection
            if connection:
                self._stop_logminer(connection)
                connection.close()

    def _process_all_streams_logminer(self, connection, start_scn: int, end_scn: int):
        """Process all streams in a single LogMiner query (similar to MySQL binlog approach).

        This is much more efficient than querying each stream separately.
        """
        # Build a map of stream info for fast lookup
        stream_map = {}  # (schema, table) -> stream
        all_columns = set()  # All columns across all streams

        for stream in self.log_based_streams:
            if not stream.selected:
                continue

            fully_qualified = str(stream.fully_qualified_name)
            if "." in fully_qualified:
                schema_name, table_name = fully_qualified.split(".", 1)
            else:
                user_logger.warning("Could not determine schema name for stream %s, skipping", stream.name)
                continue

            # Store stream info for lookup
            stream_key = (schema_name.upper(), table_name.upper())
            stream_map[stream_key] = {"stream": stream, "schema": schema_name, "table": table_name, "columns": list(stream.schema.get("properties").keys())}

            # Add columns to the global set (excluding _sdc columns)
            for col in stream.schema.get("properties").keys():
                if not col.startswith("_sdc_"):
                    all_columns.add(col)

        if not stream_map:
            user_logger.warning("No selected log-based streams found")
            return

        user_logger.info("Processing LogMiner for %d streams with %d total columns", len(stream_map), len(all_columns))
        user_logger.info("All columns found: %s", sorted(all_columns))

        # Build a single LogMiner query for all streams and columns
        mine_sql = self._build_unified_logminer_query(all_columns)
        
        # Debug: Log the generated SQL
        user_logger.info("Generated LogMiner SQL: %s", mine_sql)

        # Execute the unified query with row limit for batch processing
        cursor = connection.cursor()
        try:
            # Add SCN range filtering to the query for better performance
            bounded_sql = mine_sql.replace(
                "ORDER BY SCN, CSCN",
                f"  AND CSCN BETWEEN {start_scn} AND {end_scn}\nORDER BY CSCN, SCN"
            )
            
            # Also limit the result set size for better performance
            max_rows = self.config.get("logminer_max_rows_per_batch", 10000)
            if max_rows > 0:
                bounded_sql = f"""SELECT * FROM (
{bounded_sql}
) WHERE ROWNUM <= {max_rows}"""
            
            user_logger.debug("Executing bounded LogMiner query: %s", bounded_sql)
            cursor.execute(bounded_sql)
            
            # Quick check to see if there are any rows at all
            row_check_cursor = connection.cursor()
            try:
                check_sql = f"""
                SELECT COUNT(*) as total_rows,
                       COUNT(CASE WHEN OPERATION IN ('INSERT', 'UPDATE', 'DELETE') THEN 1 END) as dml_rows
                FROM v$logmnr_contents 
                WHERE CSCN BETWEEN {start_scn} AND {end_scn}
                """
                row_check_cursor.execute(check_sql)
                result = row_check_cursor.fetchone()
                total_rows, dml_rows = result if result else (0, 0)
                user_logger.info("LogMiner content check for SCN %s-%s: %d total rows, %d DML rows", 
                               start_scn, end_scn, total_rows, dml_rows)
            except Exception as e:
                user_logger.debug("Could not check LogMiner content: %s", e)
            finally:
                row_check_cursor.close()
            
            # Process results (row limit is now handled in SQL)
            row_count = 0

            for row in cursor:
                row_count += 1
                operation = row[0]
                table_name = row[1]
                schema_name = row[2]
                # scn = row[3]  # Not used, we use cscn instead
                cscn = row[4]
                commit_timestamp = row[5]

                # Double-check SCN range (database filtering should handle this, but be safe)
                if not (start_scn <= cscn <= end_scn):
                    continue

                # Find the matching stream
                stream_key = (schema_name.upper(), table_name.upper())
                if stream_key not in stream_map:
                    # This table/schema is not in our selected streams
                    continue

                stream_info = stream_map[stream_key]
                stream = stream_info["stream"]
                stream_columns = stream_info["columns"]

                # Get additional fields from the enhanced query 
                sql_redo = row[6]
                sql_undo = row[7]
                # row_id = row[8]  # Not used
                # rollback = row[9]  # Not used

                user_logger.debug("Processing LogMiner row %d: %s.%s %s (SCN: %s)", 
                               row_count, schema_name, table_name, operation, cscn)
                user_logger.debug("SQL_REDO: %s", sql_redo)
                user_logger.debug("SQL_UNDO: %s", sql_undo)
                
                # Extract column values and reconstruct full record
                record = self._reconstruct_full_record(
                    sql_redo, sql_undo, operation, stream_columns, 
                    schema_name, table_name
                )
                
                # Apply proper type conversions based on stream schema
                record = self._apply_schema_types(record, stream)
                
                # Add Singer CDC columns
                record["_sdc_lsn"] = int(cscn)  # Use commit SCN as LSN as integer
                
                if operation == "DELETE":
                    record["_sdc_deleted_at"] = commit_timestamp.isoformat() if commit_timestamp else None
                else:
                    record["_sdc_deleted_at"] = None
                
                # Check for missing columns - this should not happen for INSERT/DELETE
                expected_columns = set(stream_columns) - {"_sdc_lsn", "_sdc_deleted_at"}
                actual_columns = set(record.keys()) - {"_sdc_lsn", "_sdc_deleted_at"}
                missing_columns = expected_columns - actual_columns
                
                if missing_columns:
                    if operation in ("INSERT", "DELETE"):
                        user_logger.warning("Missing columns for %s operation on %s.%s (this should not happen): %s", 
                                         operation, schema_name, table_name, sorted(missing_columns))
                        user_logger.warning("SQL_REDO: %s", sql_redo)
                        user_logger.warning("SQL_UNDO: %s", sql_undo)
                    else:
                        user_logger.debug("Missing columns for %s operation on %s.%s (expected): %s", 
                                         operation, schema_name, table_name, sorted(missing_columns))
                    
                    # Only fill with null for UPDATE operations where we truly can't get the values
                    # For INSERT/DELETE, this indicates a parsing problem that needs investigation
                    if operation == "UPDATE":
                        for missing_col in missing_columns:
                            record[missing_col] = None
                

                yield record, stream.name

            if row_count == 0:
                user_logger.info("No matching DML operations found in SCN range %s-%s", start_scn, end_scn)
            else:
                user_logger.info("Processed %d LogMiner rows for SCN range %s-%s", row_count, start_scn, end_scn)

        except Exception as e:
            user_logger.error("Error processing LogMiner results: %s", e)
            raise
        finally:
            cursor.close()

    def _build_unified_logminer_query(self, all_columns: set[str]) -> str:
        """Build a unified LogMiner query following Oracle best practices.
        
        This implementation follows the Oracle LogMiner Utility documentation:
        https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html
        """
        # Sort columns for consistent ordering
        sorted_columns = sorted(all_columns)
        
        # Build table and schema filters from our selected streams
        table_conditions = []
        for stream in self.log_based_streams:
            if not stream.selected:
                continue
            fully_qualified = str(stream.fully_qualified_name)
            if "." in fully_qualified:
                schema_name, table_name = fully_qualified.split(".", 1)
                table_conditions.append(f"(SEG_OWNER = '{schema_name.upper()}' AND TABLE_NAME = '{table_name.upper()}')") 
        
        table_filter = " OR ".join(table_conditions) if table_conditions else "1=1"
        
        # Use Oracle LogMiner's standard approach with essential columns
        # We'll extract column values using SQL parsing since MINE_VALUE has issues
        user_logger.info("Using Oracle LogMiner standard query with %d columns", len(sorted_columns))
        
        query = f"""
        SELECT 
            OPERATION,
            TABLE_NAME,
            SEG_OWNER as SCHEMA_NAME,
            SCN,
            CSCN,
            COMMIT_TIMESTAMP,
            SQL_REDO,
            SQL_UNDO,
            ROW_ID,
            ROLLBACK
        FROM v$logmnr_contents 
        WHERE OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
          AND SEG_OWNER IS NOT NULL
          AND TABLE_NAME IS NOT NULL
          AND ROLLBACK = 0
          AND ({table_filter})
        ORDER BY SCN, CSCN
        """

        return query

    def _reconstruct_full_record(self, sql_redo: str, sql_undo: str, operation: str, 
                                stream_columns: list, schema_name: str, table_name: str) -> dict:
        """Reconstruct full record from LogMiner SQL statements.
        
        Following Oracle LogMiner best practices:
        - INSERT: SQL_REDO contains all column values
        - UPDATE: SQL_REDO has new values, SQL_UNDO has old values for changed columns
        - DELETE: SQL_UNDO contains all original column values (as INSERT statement)
        """
        record = {}
        
        try:
            if operation == "INSERT":
                # For INSERT, SQL_REDO contains the complete INSERT statement
                # Example: insert into "NEKT"."NEWTABLE"("ID","VALUE") values ('1','test_value');
                record = self._parse_insert_sql(sql_redo, stream_columns)
                
            elif operation == "UPDATE":
                # For UPDATE, we need both REDO and UNDO to get the complete record
                # SQL_REDO: update "NEKT"."NEWTABLE" set "VALUE" = 'new_value' where "ID" = '1';
                # SQL_UNDO: update "NEKT"."NEWTABLE" set "VALUE" = 'old_value' where "ID" = '1';
                
                # Get changed values from REDO (SET clause) - these are the new values
                changed_values = self._parse_update_sql(sql_redo, stream_columns)
                
                # Get key values from WHERE clause (these are unchanged)
                where_values = self._parse_update_where_clause(sql_redo, stream_columns)
                
                # Get old values from UNDO (SET clause) - these are the previous values for changed columns
                old_changed_values = self._parse_update_sql(sql_undo, stream_columns)
                
                # Start with key values from WHERE clause
                record = where_values.copy()
                
                # Add the new values from REDO SET clause (changed columns)
                record.update(changed_values)
                
                # For columns that weren't changed, we need to get them from somewhere
                # The issue is Oracle LogMiner doesn't provide unchanged column values
                # We'll need to accept that some columns may be missing for UPDATE operations
                # This is a known limitation of Oracle LogMiner
                
                user_logger.debug("UPDATE reconstruction - WHERE: %s, CHANGED: %s, OLD: %s", 
                               where_values, changed_values, old_changed_values)
                
            elif operation == "DELETE":
                # For DELETE, SQL_UNDO contains the INSERT that would restore the row
                # This gives us all the original column values
                record = self._parse_insert_sql(sql_undo, stream_columns)
                
        except Exception as e:
            user_logger.warning("Failed to reconstruct full record for %s.%s: %s", 
                              schema_name, table_name, e)
            user_logger.debug("SQL_REDO: %s", sql_redo)
            user_logger.debug("SQL_UNDO: %s", sql_undo)
            
            # Fallback: try to extract any values we can using simpler parsing
            record = self._extract_values_from_sql(sql_redo, sql_undo, operation, stream_columns)
        
        return record

    def _extract_values_from_sql(self, sql_redo: str, sql_undo: str, operation: str, stream_columns: list) -> dict:
        """Extract column values from SQL_REDO/SQL_UNDO statements using regex patterns."""
        record = {}
        
        try:
            if operation in ("INSERT", "UPDATE") and sql_redo:
                # Parse INSERT/UPDATE from REDO SQL
                # Example: insert into "NEKT"."NEWTABLE"("ID","VALUE","DATE_TEST") values ('1','test','2023-01-01');
                # Example: update "NEKT"."NEWTABLE" set "VALUE" = 'updated' where "ID" = '1';
                
                if operation == "INSERT" and "insert into" in sql_redo.lower():
                    record = self._parse_insert_sql(sql_redo, stream_columns)
                elif operation == "UPDATE" and "update" in sql_redo.lower():
                    record = self._parse_update_sql(sql_redo, stream_columns)
                    
            elif operation == "DELETE" and sql_undo:
                # Parse DELETE from UNDO SQL (which shows the original INSERT)
                # The UNDO for a DELETE is typically an INSERT with the original values
                if "insert into" in sql_undo.lower():
                    record = self._parse_insert_sql(sql_undo, stream_columns)
                    
        except Exception as e:
            user_logger.warning("Failed to parse SQL for column values: %s", e)
            user_logger.debug("SQL_REDO: %s", sql_redo)
            user_logger.debug("SQL_UNDO: %s", sql_undo)
        
        return record

    def _parse_insert_sql(self, sql: str, stream_columns: list) -> dict:
        """Parse INSERT SQL to extract column values."""
        import re
        record = {}
        
        # Extract the table columns part
        columns_match = re.search(r'insert into [^(]+\(([^)]+)\)\s*values', sql, re.IGNORECASE)
        if not columns_match:
            user_logger.warning("Failed to match INSERT SQL columns pattern: %s", sql)
            return record
            
        columns_str = columns_match.group(1)
        
        # Extract the values part by finding the VALUES keyword and getting everything after it
        values_match = re.search(r'\bvalues\s*\((.+)\)\s*;?\s*$', sql, re.IGNORECASE | re.DOTALL)
        if not values_match:
            user_logger.warning("Failed to match INSERT SQL values pattern: %s", sql)
            return record
            
        values_str = values_match.group(1)
        
        # Extract column names (remove quotes)
        columns = [col.strip().strip('"').lower() for col in columns_str.split(',')]
        
        # Extract values (handle quoted strings and Oracle functions)
        values = self._parse_sql_values(values_str)
        
        # Create case-insensitive lookup for stream columns
        stream_columns_lower = [sc.lower() for sc in stream_columns]
        
        # Map columns to values
        for i, col in enumerate(columns):
            if i < len(values) and col in stream_columns_lower:
                record[col] = values[i]
        
        return record

    def _parse_update_sql(self, sql: str, stream_columns: list) -> dict:
        """Parse UPDATE SQL to extract new column values."""
        import re
        record = {}
        
        # Pattern for UPDATE: update "SCHEMA"."TABLE" set "COL1" = 'val1', "COL2" = 'val2' where ...
        pattern = r'set\s+(.+?)\s+where'
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if match:
            set_clause = match.group(1)
            
            # Enhanced pattern to handle Oracle functions in assignments
            # Matches: "COL" = 'value' or "COL" = to_date(...) etc.
            assignments = re.findall(r'"([^"]+)"\s*=\s*((?:to_\w+\s*\([^)]*\))|(?:\'[^\']*\')|(?:[^,\s]+))', set_clause, re.IGNORECASE)
            
            for col_name, value_str in assignments:
                if col_name.lower() in [sc.lower() for sc in stream_columns]:
                    # Parse the value (handle quotes, nulls, and Oracle functions)
                    parsed_value = self._parse_single_sql_value(value_str.strip())
                    record[col_name.lower()] = parsed_value
        
        return record

    def _parse_sql_values(self, values_str: str) -> list:
        """Parse comma-separated SQL values, handling quotes, nulls, and Oracle functions."""
        values = []
        
        
        # Enhanced pattern to handle Oracle functions with nested parentheses and quoted strings
        # This pattern matches:
        # 1. Oracle functions: TO_DATE(...), TO_TIMESTAMP(...), etc. with nested quotes and parentheses
        # 2. Quoted strings: 'value'
        # 3. Other values: numbers, NULL, etc.
        
        # Use a more sophisticated approach to handle nested parentheses in Oracle functions
        tokens = []
        current_token = ""
        paren_count = 0
        in_quotes = False
        i = 0
        
        while i < len(values_str):
            char = values_str[i]
            
            if char == "'" and not in_quotes:
                in_quotes = True
                current_token += char
            elif char == "'" and in_quotes:
                # Check for escaped quote
                if i + 1 < len(values_str) and values_str[i + 1] == "'":
                    current_token += "''"  # Escaped quote
                    i += 1  # Skip next quote
                else:
                    in_quotes = False
                    current_token += char
            elif in_quotes:
                current_token += char
            elif char == "(":
                paren_count += 1
                current_token += char
            elif char == ")":
                paren_count -= 1
                current_token += char
            elif char == "," and paren_count == 0:
                # End of current token
                if current_token.strip():
                    tokens.append(current_token.strip())
                current_token = ""
            else:
                current_token += char
            
            i += 1
        
        # Add the last token
        if current_token.strip():
            tokens.append(current_token.strip())
        
        
        # Parse each token
        for token in tokens:
            parsed_value = self._parse_single_sql_value(token.strip())
            values.append(parsed_value)
        return values

    def _parse_single_sql_value(self, value_str: str) -> str | int | float | None:
        """Parse a single SQL value, handling quotes, nulls, Oracle functions, and type conversion."""
        value_str = value_str.strip()
        
        if value_str.upper() == 'NULL':
            return None
        elif value_str.startswith("'") and value_str.endswith("'"):
            # Remove quotes and handle escaped quotes
            return value_str[1:-1].replace("''", "'")
        elif (value_str.lower().startswith(('to_date(', 'to_timestamp(', 'to_number(', 'to_clob(', 'to_blob(', 'to_char(', 'hextoraw(', 'rawtohex(')) or 
              ('yyyy-mm-dd' in value_str.lower() and 'hh24:mi:ss' in value_str.lower()) or
              (value_str.lower().startswith('to_date(') and not value_str.endswith(')'))):
            # Handle Oracle type conversion functions, incomplete functions, and format strings
            return self._parse_oracle_function(value_str)
        else:
            # Try to convert to numeric type if it looks like a number
            try:
                if '.' in value_str:
                    return float(value_str)
                else:
                    return int(value_str)
            except ValueError:
                # Not a number, return as string
                return value_str
            
    def _parse_oracle_function(self, func_str: str) -> str | int | float | None:
        """Parse Oracle function calls from LogMiner SQL and convert to proper Python types."""
        import re
        
        func_str = func_str.strip()
        
        # Handle incomplete TO_DATE function calls (missing closing quotes/parentheses)
        # Example: "TO_DATE('2024-03-06T00:00:00.00+00:00'" (incomplete)
        if func_str.lower().startswith('to_date(') and not func_str.endswith(')'):
            # Try to extract date string from incomplete function call
            match = re.search(r"to_date\s*\(\s*'([^']*)", func_str, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                if date_str:  # Only process if we got a non-empty date string
                    try:
                        # Parse with pendulum and return datetime format to match full table sync
                        if 't' in date_str.lower():
                            # Format: 2024-03-05t00:00:00.00+00:00
                            clean_date = date_str.replace('t', 'T')
                            if '+00:00' in clean_date:
                                clean_date = clean_date.replace('+00:00', 'Z')
                            parsed = pendulum.parse(clean_date)
                            return parsed.format('YYYY-MM-DDTHH:mm:ss')  # Match full table format
                        else:
                            parsed = pendulum.parse(date_str)
                            return parsed.format('YYYY-MM-DDTHH:mm:ss')
                    except Exception as e:
                        user_logger.warning("Could not parse incomplete date '%s': %s", date_str, e)
                        return date_str
            return func_str
        
        # Handle TO_DATE function: to_date('2024-03-05t00:00:00.00+00:00', 'format')
        if func_str.lower().startswith('to_date('):
            # Extract the date string from to_date('date_string', ...)
            match = re.match(r"to_date\s*\(\s*'([^']+)'.*\)", func_str, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                try:
                    # Parse with pendulum and return datetime format to match full table sync
                    if 't' in date_str.lower():
                        # Format: 2024-03-05t00:00:00.00+00:00
                        clean_date = date_str.replace('t', 'T')
                        if '+00:00' in clean_date:
                            clean_date = clean_date.replace('+00:00', 'Z')
                        parsed = pendulum.parse(clean_date)
                        return parsed.format('YYYY-MM-DDTHH:mm:ss')  # Match full table format
                    else:
                        parsed = pendulum.parse(date_str)
                        return parsed.format('YYYY-MM-DDTHH:mm:ss')
                except Exception as e:
                    user_logger.warning("Could not parse date '%s': %s", date_str, e)
                    return date_str
            return func_str
            
        # Handle timestamp format strings that are not actual timestamps
        # Example: "YYYY-MM-DD\"T\"HH24:MI:SS.\"00+00:00\""
        if 'yyyy-mm-dd' in func_str.lower() and 'hh24:mi:ss' in func_str.lower():
            # This looks like a timestamp format string rather than an actual timestamp
            # Return None or a default value since we can't extract a meaningful timestamp
            user_logger.warning("Got timestamp format string instead of actual timestamp: %s", func_str)
            return None
        
        # Handle TO_TIMESTAMP function: to_timestamp('2024-05-23t14:30:15.123000+00:00', 'format')
        elif func_str.lower().startswith('to_timestamp('):
            # Extract the timestamp string
            match = re.match(r"to_timestamp\s*\(\s*'([^']+)'.*\)", func_str, re.IGNORECASE)
            if match:
                timestamp_str = match.group(1)
                try:
                    # Parse with pendulum and return format matching full table sync
                    if 't' in timestamp_str.lower():
                        # Format: 2024-05-23t14:30:15.123000+00:00
                        clean_timestamp = timestamp_str.replace('t', 'T')
                        if '+00:00' in clean_timestamp:
                            clean_timestamp = clean_timestamp.replace('+00:00', 'Z')
                        parsed = pendulum.parse(clean_timestamp)
                        return parsed.format('YYYY-MM-DDTHH:mm:ss.SSSSSS')  # Match full table format (no timezone)
                    else:
                        parsed = pendulum.parse(timestamp_str)
                        return parsed.format('YYYY-MM-DDTHH:mm:ss.SSSSSS')
                except Exception as e:
                    user_logger.warning("Could not parse timestamp '%s': %s", timestamp_str, e)
                    return timestamp_str
            return func_str
            
        # Handle TO_NUMBER function: to_number('123.45')
        elif func_str.lower().startswith('to_number('):
            match = re.match(r"to_number\s*\(\s*'([^']+)'.*\)", func_str, re.IGNORECASE)
            if match:
                number_str = match.group(1)
                try:
                    # Convert to proper numeric type
                    if '.' in number_str:
                        return float(number_str)
                    else:
                        return int(number_str)
                except ValueError:
                    user_logger.warning("Could not convert '%s' to number", number_str)
                    return number_str
            return func_str
            
        # Handle TO_CLOB/TO_BLOB - just extract the string value
        elif func_str.lower().startswith(('to_clob(', 'to_blob(')):
            match = re.match(r"to_[cb]lob\s*\(\s*'([^']+)'.*\)", func_str, re.IGNORECASE)
            if match:
                return match.group(1)
            return func_str
            
        # Handle TO_CHAR function: to_char(date_val, 'format') or to_char(number_val)
        elif func_str.lower().startswith('to_char('):
            # Extract the first parameter (the value being converted)
            match = re.match(r"to_char\s*\(\s*'([^']+)'.*\)", func_str, re.IGNORECASE)
            if match:
                return match.group(1)
            # If not quoted, might be a number or date literal
            match = re.match(r"to_char\s*\(\s*([^,)]+).*\)", func_str, re.IGNORECASE)
            if match:
                return match.group(1).strip()
            return func_str
            
        # Handle HEXTORAW function: hextoraw('48656C6C6F')
        elif func_str.lower().startswith('hextoraw('):
            match = re.match(r"hextoraw\s*\(\s*'([^']+)'.*\)", func_str, re.IGNORECASE)
            if match:
                hex_str = match.group(1)
                # Convert hex to bytes, then to string if possible
                try:
                    return bytes.fromhex(hex_str).decode('utf-8', errors='ignore')
                except Exception:
                    return hex_str
            return func_str
            
        # Handle RAWTOHEX function: rawtohex(raw_value)
        elif func_str.lower().startswith('rawtohex('):
            match = re.match(r"rawtohex\s*\(\s*'([^']+)'.*\)", func_str, re.IGNORECASE)
            if match:
                return match.group(1)  # Return the hex representation as-is
            return func_str
            
        # Unknown function, return as-is
        return func_str

    def _parse_update_where_clause(self, sql: str, stream_columns: list) -> dict:
        """Parse UPDATE WHERE clause to extract column values (these are unchanged values)."""
        import re
        record = {}
        
        try:
            # Extract WHERE clause from UPDATE statement
            # Pattern: update table set col1='val1' where col2='val2' and col3='val3'
            if 'where' in sql.lower():
                where_part = sql.lower().split('where')[1].strip()
                # Remove any trailing semicolon
                where_part = where_part.rstrip(';')
                
                # Enhanced pattern to handle Oracle functions in WHERE clause
                # Matches: "COL" = 'value' or "COL" = to_date(...) etc.
                conditions = re.findall(r'"([^"]+)"\s*=\s*((?:to_\w+\s*\([^)]*\))|(?:\'[^\']*\')|(?:[^\s\)]+))', where_part, re.IGNORECASE)
                
                for col_name, value_str in conditions:
                    if col_name.lower() in [sc.lower() for sc in stream_columns]:
                        parsed_value = self._parse_single_sql_value(value_str.strip())
                        record[col_name.lower()] = parsed_value
                        
        except Exception as e:
            user_logger.debug("Could not parse UPDATE WHERE clause: %s", e)
            
        return record
    
    def _apply_schema_types(self, record: dict, stream) -> dict:
        """Apply proper type conversions based on the stream schema."""
        if not record:
            return record
            
        schema_properties = stream.schema.get("properties", {})
        typed_record = {}
        
        for column, value in record.items():
            if value is None:
                typed_record[column] = None
                continue
                
            # Get the column schema
            column_schema = schema_properties.get(column, {})
            column_type = column_schema.get("type", [])
            column_format = column_schema.get("format")
            
            # Handle nullable types (e.g., ["string", "null"])
            if isinstance(column_type, list):
                non_null_types = [t for t in column_type if t != "null"]
                column_type = non_null_types[0] if non_null_types else "string"
            
            try:
                if column_type == "integer":
                    if isinstance(value, str):
                        typed_record[column] = int(float(value))  # Handle "7.0" -> 7
                    else:
                        typed_record[column] = int(value)
                        
                elif column_type == "number":
                    if isinstance(value, str):
                        typed_record[column] = float(value)
                    else:
                        typed_record[column] = float(value)
                        
                elif column_type == "string":
                    if column_format == "date":
                        # Match full table format: YYYY-MM-DDTHH:MM:SS (with time component)
                        if isinstance(value, str):
                            try:
                                parsed = pendulum.parse(value)
                                # Format as datetime to match full table sync
                                typed_record[column] = parsed.format('YYYY-MM-DDTHH:mm:ss')
                            except Exception:
                                typed_record[column] = str(value)
                        else:
                            typed_record[column] = str(value)
                            
                    elif column_format == "date-time":
                        # Match full table format: YYYY-MM-DDTHH:MM:SS.ssssss (no timezone)
                        if isinstance(value, str):
                            try:
                                parsed = pendulum.parse(value)
                                # Format without timezone to match full table sync
                                typed_record[column] = parsed.format('YYYY-MM-DDTHH:mm:ss.SSSSSS')
                            except Exception:
                                typed_record[column] = str(value)
                        else:
                            typed_record[column] = str(value)
                    else:
                        typed_record[column] = str(value)
                        
                else:
                    # Default: keep as-is
                    typed_record[column] = value
                    
            except (ValueError, TypeError) as e:
                user_logger.warning("Type conversion failed for column %s (value: %s, type: %s): %s", 
                                  column, value, column_type, e)
                typed_record[column] = value  # Keep original value on error
                
        return typed_record

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Post-process record to ensure _sdc_lsn is integer format."""
        _ = context  # Unused parameter but required by interface
        if "_sdc_lsn" in row:
            # Ensure _sdc_lsn is an integer (SCN value)
            try:
                row["_sdc_lsn"] = int(row["_sdc_lsn"])
            except (ValueError, TypeError):
                # Keep as-is if conversion fails
                pass
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
                # Ensure SCN/LSN is integer format
                lsn_value = latest_record[self.replication_key]
                if not isinstance(lsn_value, int):
                    try:
                        latest_record[self.replication_key] = int(lsn_value)
                    except (ValueError, TypeError):
                        # Keep original value if conversion fails
                        pass

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

