#!/usr/bin/env python
import json, os, sys
import pandas as pd
from dotenv import load_dotenv
from src.db import get_collection

load_dotenv()

def main():
    col = get_collection()
    path = sys.argv[1] if len(sys.argv) > 1 else "data/sample_books.json"
    if not os.path.exists(path):
        print(f"File not found: {path}")
        sys.exit(2)

    if path.endswith(".json"):
        raw = open(path, "r", encoding="utf-8").read().strip()
        if raw.startswith("["):
            docs = json.loads(raw)
        else:
            docs = [json.loads(line) for line in raw.splitlines() if line.strip()]
    elif path.endswith(".csv"):
        docs = pd.read_csv(path).to_dict(orient="records")
    else:
        print("Supported: .json (array or ndjson) or .csv")
        sys.exit(2)

    if not docs:
        print("No docs to insert.")
        return

    res = col.insert_many(docs, ordered=False)
    print(f"Inserted {len(res.inserted_ids)} documents into {col.full_name}")

if __name__ == "__main__":
    main()
