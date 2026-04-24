import csv
import io
from .models import HospitalCSVRow
from .config import MAX_HOSPITALS

def parse_csv(content: bytes):
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    required = {"name", "address"}
    if not required.issubset(reader.fieldnames):
        raise ValueError("CSV must contain name,address columns")

    rows = []

    for row in reader:
        rows.append(
            HospitalCSVRow(
                name=row["name"].strip(),
                address=row["address"].strip(),
                phone=row.get("phone", "").strip() or None
            )
        )

    if len(rows) == 0:
        raise ValueError("CSV empty")

    if len(rows) > MAX_HOSPITALS:
        raise ValueError(f"Max {MAX_HOSPITALS} hospitals allowed")

    return rows