import csv
import io
import logging
from typing import List

from .models import HospitalCSVRow
from .config import MAX_HOSPITALS

logger = logging.getLogger(__name__)


class CSVValidationError(Exception):
    """Custom exception for CSV validation errors."""
    pass


def parse_csv(content: bytes) -> List[HospitalCSVRow]:
    """
    Parse CSV content with validation.
    
    Validation rules:
    - Required columns: name, address
    - Max 20 hospitals
    - Empty rows ignored
    - Phone is optional
    """
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    # Check required columns
    required = {"name", "address"}
    if not reader.fieldnames:
        raise CSVValidationError("CSV file is empty or has no headers")
    
    missing_cols = required - set(reader.fieldnames)
    if missing_cols:
        raise CSVValidationError(f"Missing required columns: {', '.join(missing_cols)}")

    rows = []
    for idx, row in enumerate(reader, start=1):
        # Skip empty rows
        if not row.get("name", "").strip() and not row.get("address", "").strip():
            logger.debug(f"Skipping empty row {idx}")
            continue

        # Validate required fields are not empty
        name = row.get("name", "").strip()
        address = row.get("address", "").strip()

        if not name:
            raise CSVValidationError(f"Row {idx}: 'name' is required")
        if not address:
            raise CSVValidationError(f"Row {idx}: 'address' is required")

        rows.append(
            HospitalCSVRow(
                name=name,
                address=address,
                phone=row.get("phone", "").strip() or None
            )
        )

    if len(rows) == 0:
        raise CSVValidationError("CSV contains no valid data rows")

    if len(rows) > MAX_HOSPITALS:
        raise CSVValidationError(f"Maximum {MAX_HOSPITALS} hospitals allowed per upload")

    logger.info(f"Successfully parsed {len(rows)} hospitals from CSV")
    return rows