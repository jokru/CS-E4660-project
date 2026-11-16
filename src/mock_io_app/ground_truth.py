import json
import os
import time

from opentelemetry.trace import Span
from opentelemetry.util.types import AttributeValue

GROUND_TRUTH_FILE_ENV = "MOCK_IO_GROUND_TRUTH_FILE"
ANOMALY_ATTRIBUTE_ENV = "MOCK_IO_ANOMALY_ATTRIBUTE"


def anomaly_attributes(labels: tuple[str, ...]) -> dict[str, AttributeValue]:
    """`mock_io.anomaly` span attribute; disabled with MOCK_IO_ANOMALY_ATTRIBUTE=0."""
    if not labels or os.environ.get(ANOMALY_ATTRIBUTE_ENV, "1") == "0":
        return {}
    return {"mock_io.anomaly": ",".join(labels)}


def record_anomalies(span: Span, name: str, labels: tuple[str, ...], call_index: int) -> None:
    """Append one JSON line per anomalous call to MOCK_IO_GROUND_TRUTH_FILE (if set).

    Independent of the exported spans, so it stays usable when a collector drops them or the
    attribute is hidden. Needs an enabled trace exporter, otherwise spans have no valid IDs.
    """
    path = os.environ.get(GROUND_TRUTH_FILE_ENV)
    context = span.get_span_context()
    if not path or not labels or not context.is_valid:
        return
    line = json.dumps(
        {
            "trace_id": f"{context.trace_id:032x}",
            "span_id": f"{context.span_id:016x}",
            "span_name": name,
            "call_index": call_index,
            "anomalies": list(labels),
            "timestamp": time.time(),
        }
    )
    # A single O_APPEND write per line stays intact across the multiprocess executor.
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, f"{line}\n".encode())
    finally:
        os.close(fd)
