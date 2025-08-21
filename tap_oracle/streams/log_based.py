"""SQL client handling."""

from __future__ import annotations

import functools
from typing import cast

from nekt_singer_sdk import SQLStream
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel

from tap_oracle.connector import OracleConnector


class OracleLogBasedStream(SQLStream):
    """Stream class for Oracle streams."""

    connector_class = OracleConnector
    replication_key = "_sdc_lsn"

    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    @functools.cached_property
    def schema(self) -> dict:
        """Override schema for log-based replication adding _sdc columns."""
        schema_dict = cast(dict, self._singer_catalog_entry.schema.to_dict())
        for property in schema_dict["properties"].values():
            # Ensure nullability
            if isinstance(property["type"], list):
                if "null" not in property["type"]:
                    property["type"].append("null")
            else:
                property["type"] = [property["type"], "null"]
        if "required" in schema_dict:
            schema_dict.pop("required")
        schema_dict["properties"].update({"_sdc_deleted_at": {"type": ["string"], "format": "date-time"}})
        schema_dict["properties"].update({"_sdc_lsn": {"type": ["string"]}})
        return schema_dict
