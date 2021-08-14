#!/bin/bash
set -euo pipefail

cd /tmp

DB_PATH="blogs.sqlite3"
DB_BACKUP_ARCHIVE="$DB_PATH.gz"
DROPBOX_PATH="/blog-notifier/$(date +'%Y%m%d_%H%M%S').sql.zip"

gzip --keep "$DB_PATH"

curl -X POST https://content.dropboxapi.com/2/files/upload \
    --header "Authorization: Bearer $DB_BACKUP_TOKEN" \
    --header "Content-Type: application/octet-stream" \
    --header "Dropbox-API-Arg: {\"path\": \"$DROPBOX_PATH\",\"mode\": \"add\",\"autorename\": true,\"mute\": false,\"strict_conflict\": false}" \
    --data-binary @"$DB_BACKUP_ARCHIVE"

rm "$DB_BACKUP_ARCHIVE"
