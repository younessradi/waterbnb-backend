"""
Load the list of WaterBnB clients from usersM1_2026.csv into MongoDB.

Usage:
    python scripts/load_users.py

Environment:
    MONGODB_URI must point to the Mongo Atlas cluster (same as the Flask app).
"""

import csv
import os
from pathlib import Path

from pymongo import MongoClient


def main() -> None:
    mongo_uri = os.environ.get("MONGODB_URI")
    if not mongo_uri:
        raise SystemExit("Set MONGODB_URI before running this script.")

    csv_path = Path(__file__).resolve().parent.parent / "usersM1_2026.csv"
    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh, delimiter=";")
        docs = []
        for row in reader:
            if len(row) < 2:
                continue
            name = row[0].strip()
            student_id = row[1].strip()
            if not name or not student_id:
                continue
            docs.append({"name": name, "studentId": student_id})

    if not docs:
        raise SystemExit("No records were parsed from the CSV.")

    client = MongoClient(mongo_uri)
    collection = client["WaterBnB"]["users"]

    inserted = updated = 0
    for doc in docs:
        result = collection.update_one(
            {"name": doc["name"]},
            {"$set": doc},
            upsert=True,
        )
        if result.matched_count:
            updated += 1
        elif result.upserted_id:
            inserted += 1

    print(
        f"Processed {len(docs)} CSV rows -> "
        f"{inserted} inserted, {updated} updated/upserted."
    )


if __name__ == "__main__":
    main()
