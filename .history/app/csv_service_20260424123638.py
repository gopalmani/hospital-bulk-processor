import csv
import io
import logging
from typing import List

from .config import MAX_HOSPITALS
from .models import HospitalCSVRow

logger = logging.getLogger(__name__)


class CSVValidationError(ValueError):
    """Raised when an uploaded CSV fails validation."""


def parse_csv(content: bytes) -> List[HospitalCSVRow]:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CSVValidationError("CSV must be UTF-8 encoded") from exc

    reader = csv.DictReader(io.StringIO(text))
    required_columns = {"name", "address"}
    fieldnames = set(reader.fieldnames or [])

    if not fieldnames:
        raise CSVValidationError("CSV must include a header row")

    missing_columns = sorted(required_columns - fieldnames)
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise CSVValidationError(f"CSV is missing required column(s): {missing}")

    rows: List[HospitalCSVRow] = []
    for line_number, row in enumerate(reader, start=2):
        values = {key: (value or "").strip() for key, value in row.items()}
        if not any(values.values()):
            logger.debug("csv_empty_row_ignored", extra={"line_number": line_number})
            continue

        name = values.get("name", "")
        address = values.get("address", "")
        if not name:
            raise CSVValidationError(f"Row {line_number}: name is required")
        if not address:
            raise CSVValidationError(f"Row {line_number}: address is required")

        if len(rows) >= MAX_HOSPITALS:
            raise CSVValidationError(f"CSV can include at most {MAX_HOSPITALS} hospitals")

        rows.append(
            HospitalCSVRow(
                name=name,
                address=address,
                phone=values.get("phone") or None,
            )
        )

    if not rows:
        raise CSVValidationError("CSV must include at least one hospital row")

    logger.info("csv_parsed", extra={"hospital_count": len(rows)})
    return rows
