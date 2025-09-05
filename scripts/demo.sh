# Demo script: JSON first, then OL dumps (.gz)

set -Eeuo pipefail

# ---------------- Config ----------------
: "${MONGO_URI:=mongodb://127.0.0.1:27017}"
DB="library_db"
COLL="books"

# JSON demo file (array or JSONL; plain or .gz). Will be created if missing.
JSON_PATH="data/sample_books.json"

# Open Library dumps (.gz)
AUTH_PATH="data/openlibrary/ol_dump_authors_2025-07-31.txt.gz"
WORKS_PATH="data/openlibrary/ol_dump_works_2025-07-31.txt.gz"

# Ingest tuning
AUTH_SCAN=300000
WORKS_LIMIT=50000
BATCH=1000
# ----------------------------------------

YELLOW=$'\033[33m'; CYAN=$'\033[36m'; GREEN=$'\033[32m'; BOLD=$'\033[1m'; RESET=$'\033[0m'

say() { printf "%s\n" "$*"; }
hr()  { printf "%s\n" "------------------------------------------------------------"; }
print_cmd() { printf "%s$ %s%s\n" "$CYAN" "$(printf '%q ' "$@")" "$RESET"; }

# Always read from the terminal...
press_enter() { read -r -p "Press Enter to run..." </dev/tty; }
press_next()  { read -r -p "Press Enter to continue..." </dev/tty; }

run() {
  echo
  print_cmd "$@"
  press_enter
  "$@"
  status=$?
  echo
  if [[ $status -ne 0 ]]; then
    echo "❌ Command exited with status $status" >&2
    exit $status
  fi
  press_next
}

resolve_python() {
  if command -v python >/dev/null 2>&1; then echo "python";
  elif command -v python3 >/dev/null 2>&1; then echo "python3";
  else echo ""; fi
}

# Make a temp JS file for queries (no eval quoting problems)
QFILE="$(mktemp -t demo_queries.XXXXXX.js)"
cleanup() { rm -f "$QFILE"; }
trap cleanup EXIT

cat >"$QFILE" <<'JS'
const d=db.getSiblingDB("library_db");
print("count =", d.books.countDocuments());
print("indexes =", d.books.getIndexes().map(i=>i.name).join(", "));
printjson(d.books.findOne({}, {title:1, authors:1, subjects:1, _id:0}));
d.books.createIndex({title:"text"});
print("\nTop matches for Sherlock Holmes:");
printjson(
  d.books.find({ $text:{ $search:"Sherlock Holmes" } },
               { score:{ $meta:"textScore" }, title:1, authors:1, _id:0 })
        .sort({ score:{ $meta:"textScore" } })
        .limit(5)
        .toArray()
);
print("\nFantasy examples:");
printjson(d.books.find({ subjects:"Fantasy" }, {title:1, authors:1, _id:0}).limit(5).toArray());
print("\nTop 10 authors:");
printjson(
  d.books.aggregate([
    { $unwind:"$authors" },
    { $group:{ _id:"$authors", n:{ $sum:1 } } },
    { $sort:{ n:-1 } },
    { $limit:10 }
  ]).toArray()
);
print("\nTop 10 subjects:");
printjson(
  d.books.aggregate([
    { $unwind:"$subjects" },
    { $group:{ _id:"$subjects", n:{ $sum:1 } } },
    { $sort:{ n:-1 } },
    { $limit:10 }
  ]).toArray()
);
JS

hr
say "${BOLD}Mini NoSQL Library Catalog — Guided Demo (JSON → OL dumps)${RESET}"
hr
say "MONGO_URI = ${MONGO_URI}"
say "Target     = ${DB}.${COLL}"
hr

# Step 1: Environment checks
say "${YELLOW}Step 1: Checking tools & files...${RESET}"
PYBIN=$(resolve_python)
[[ -n "${PYBIN}" ]] || { say "❌ No python/python3 found in PATH."; exit 1; }
for bin in mongosh gzip "$PYBIN"; do
  command -v "$bin" >/dev/null 2>&1 || { say "❌ Missing dependency: $bin"; exit 1; }
done
[[ -f "scripts/ingest_openlibrary_dump.py" ]] || { say "❌ Missing scripts/ingest_openlibrary_dump.py"; exit 1; }
say "✅ Tools OK."
press_next

# Step 2: Ping MongoDB
say "${YELLOW}Step 2: Verify MongoDB connection${RESET}"
run mongosh --quiet "$MONGO_URI" --eval 'db.runCommand({ping:1})'

# Step 3: Clean start (drop collection)
say "${YELLOW}Step 3: Start clean (drop target collection)${RESET}"
run mongosh --quiet "$MONGO_URI" --eval "db.getSiblingDB('${DB}').${COLL}.drop()"

# Step 4: Ensure JSON sample exists
say "${YELLOW}Step 4: Prepare JSON sample (if missing)${RESET}"
if [[ ! -f "$JSON_PATH" ]]; then
  say "⚠️  $JSON_PATH not found. Creating a tiny sample (2 books)."
  mkdir -p "$(dirname "$JSON_PATH")"
  cat > "$JSON_PATH" <<'JSON'
[
  {"title":"A Study in Scarlet","authors":["Arthur Conan Doyle"],"subjects":["Detective"]},
  {"title":"Harry Potter and the Philosopher's Stone","authors":["J. K. Rowling"],"subjects":["Fantasy"]}
]
JSON
else
  say "✅ Found $JSON_PATH"
fi
press_next

# Step 5: Ingest JSON sample first
say "${YELLOW}Step 5: Ingest JSON sample first${RESET}"
run "$PYBIN" -u scripts/ingest_openlibrary_dump.py \
  --json-sample "$JSON_PATH" \
  --db "$DB" --collection "$COLL" \
  --batch "$BATCH" --verify-after

# Step 6: Showcase queries (after JSON)
say "${YELLOW}Step 6: Showcase queries (after JSON ingest)${RESET}"
run mongosh --quiet "$MONGO_URI" --file "$QFILE"

# Step 7: Ingest Open Library dumps (.gz) — append
say "${YELLOW}Step 7: Ingest Open Library dumps (.gz) — append to same collection${RESET}"
[[ -f "$AUTH_PATH" ]] || { say "❌ Missing dump file: $AUTH_PATH"; exit 1; }
[[ -f "$WORKS_PATH" ]] || { say "❌ Missing dump file: $WORKS_PATH"; exit 1; }
run "$PYBIN" -u scripts/ingest_openlibrary_dump.py \
  --authors "$AUTH_PATH" \
  --works   "$WORKS_PATH" \
  --db "$DB" --collection "$COLL" \
  --authors-scan-limit "$AUTH_SCAN" \
  --works-limit "$WORKS_LIMIT" \
  --batch "$BATCH" --verify-after

# Step 8: Showcase queries again (after OL append)
say "${YELLOW}Step 8: Showcase queries (after OL append)${RESET}"
run mongosh --quiet "$MONGO_URI" --file "$QFILE"

say "${GREEN}${BOLD}Demo complete!${RESET} 🎉"
