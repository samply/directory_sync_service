#!/bin/sh
set -eu

BASE_URL="${BASE_URL:-http://emx2:8080}"
DIRECTORY_DATABASES="${DIRECTORY_DATABASES:-/directory_databases}"

ADMIN_EMAIL="${ADMIN_EMAIL:-admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin}"

COOKIE_JAR="/tmp/emx2.cookies"

api_gql="$BASE_URL/api/graphql"

echo "Waiting for EMX2 at $BASE_URL ..."
until curl -fsS "$BASE_URL/" >/dev/null 2>&1; do
  sleep 2
done

echo "Logging in as $ADMIN_EMAIL ..."
# GraphQL signin mutation (same shape as EMX2 pyclient) :contentReference[oaicite:2]{index=2}
signin_payload="$(cat <<'JSON'
{"query":"mutation($email:String,$password:String){ signin(email:$email,password:$password){ status message token } }","variables":{"email":"__EMAIL__","password":"__PASSWORD__"}}
JSON
)"
signin_payload="${signin_payload/__EMAIL__/$ADMIN_EMAIL}"
signin_payload="${signin_payload/__PASSWORD__/$ADMIN_PASSWORD}"

resp="$(curl -fsS -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
  -H "Content-Type: application/json" \
  --data "$signin_payload" \
  "$api_gql")"

echo "$resp" | grep -q '"status"[[:space:]]*:[[:space:]]*"SUCCESS"' || {
  echo "ERROR: login failed. Response:"
  echo "$resp"
  exit 1
}
echo "Login OK."

# Put DirectoryOntologies.zip first, then everything else, to get dependencies right.
ZIPS="$(find "$DIRECTORY_DATABASES" -maxdepth 1 -type f -name '*.zip' | sort | awk '
  /\/DirectoryOntologies\.zip$/ { print "0 " $0; next }
  { print "1 " $0 }
' | sort | cut -d" " -f2-)"
[ -n "$ZIPS" ] || { echo "ERROR: No .zip files found in $DIRECTORY_DATABASES"; exit 1; }

echo "Found seed archives:"
echo "$ZIPS" | sed 's|^|  - |'

# Helpers
schema_id_from_filename() {
  name="$(echo "$1" | sed 's/\.zip$//')"

  echo "$name"
}

graphql_post() {
  curl -fsS -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
    -H "Content-Type: application/json" \
    --data "$1" \
    "$api_gql"
}

wait_schema_exists() {
  sid="$1"
  i=1
  while [ "$i" -le 90 ]; do
    # list schemas: { _schemas { id name ... } } :contentReference[oaicite:3]{index=3}
    list_payload='{"query":"{ _schemas { id name } }"}'
    out="$(graphql_post "$list_payload" || true)"
    echo "$out" | grep -q "\"name\"[[:space:]]*:[[:space:]]*\"$sid\"" && return 0
    echo "Schema '$sid' not ready yet (attempt $i/90)..."
    i=$((i+1))
    sleep 2
  done
  return 1
}

create_schema_if_missing() {
  sid="$1"
  # createSchema mutation (same as pyclient) :contentReference[oaicite:4]{index=4}
  create_payload="$(cat <<'JSON'
{"query":"mutation($name:String,$description:String,$template:String,$includeDemoData:Boolean,$parentJob:String){ createSchema(name:$name,description:$description,template:$template,includeDemoData:$includeDemoData,parentJob:$parentJob){ status message taskId } }","variables":{"name":"__NAME__","description":null,"template":null,"includeDemoData":false,"parentJob":null}}
JSON
)"
  create_payload="${create_payload/__NAME__/$sid}"

  out="$(graphql_post "$create_payload" || true)"

  # If it already exists, EMX2 may return an error; we treat that as OK and just continue.
  # If it succeeds, status is SUCCESS.
  if echo "$out" | grep -q '"status"[[:space:]]*:[[:space:]]*"SUCCESS"'; then
    echo "createSchema: SUCCESS ($sid)"
    return 0
  fi

  # If it's "already exists" (message varies by version), proceed anyway.
  echo "createSchema response (non-success, will continue and verify existence):"
  echo "$out"
  return 0
}

wait_task_done() {
  task_id="$1"
  i=1
  while [ "$i" -le 180 ]; do
    q="$(printf '{ "query": "{ _tasks(id:\\"%s\\") { id description status subTasks { id description status } } }" }' "$task_id")"
    out="$(graphql_post "$q" || true)"

    # Extract status of the FIRST object in the _tasks array
    status="$(
      echo "$out" | awk '
        BEGIN{inTasks=0; gotStatus=0}
        /"_tasks"[[:space:]]*:/ {inTasks=1}
        inTasks && /"status"[[:space:]]*:/ && !gotStatus {
          match($0, /"status"[[:space:]]*:[[:space:]]*"[^"]*"/)
          if (RSTART>0) {
            s=substr($0, RSTART, RLENGTH)
            gsub(/.*"status"[[:space:]]*:[[:space:]]*"/,"",s)
            gsub(/".*/,"",s)
            print s
            gotStatus=1
          }
        }
      ' | head -n1
    )"

    [ -n "$status" ] || status="UNKNOWN"

    case "$status" in
      SUCCESS|SUCCEEDED|COMPLETED|DONE)
        echo "Task $task_id finished with status=$status"
        return 0
        ;;
      FAILED|ERROR)
        echo "ERROR: Task $task_id failed. Response:"
        echo "$out"
        return 1
        ;;
      *)
        echo "Task $task_id status=$status (attempt $i/180)..."
        ;;
    esac

    i=$((i+1))
    sleep 2
  done

  echo "ERROR: Task $task_id did not finish in time."
  return 1
}

for zip in $ZIPS; do
  base="$(basename "$zip")"
  sid="$(schema_id_from_filename "$base")"

  echo ""
  echo "NOTE: Using schema id '$sid' (from '$base')"
  echo "==> Ensuring schema '$sid' exists"
  create_schema_if_missing "$sid"

  wait_schema_exists "$sid" || {
    echo "ERROR: schema '$sid' never appeared in _schemas"
    exit 1
  }

  echo "==> Uploading '$base' into schema '$sid'"
  # ZIP upload endpoint as used by the EMX2 pyclient :contentReference[oaicite:6]{index=6}
  upload_out="$(curl -fsS -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
    -F "file=@$zip" \
    "$BASE_URL/$sid/api/zip?async=true")"

  task_id="$(echo "$upload_out" | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1 || true)"
  if [ -z "$task_id" ]; then
    echo "ERROR: Upload did not return a task id. Response:"
    echo "$upload_out"
    exit 1
  fi

  echo "Upload accepted, task id: $task_id"
  wait_task_done "$task_id"
done

echo ""
echo "All imports completed."
