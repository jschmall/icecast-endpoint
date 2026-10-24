#!/bin/sh
# A local Icecast server for the live tests (`cargo test -- --ignored`), in Docker:
#   scripts/icecast.sh up [version]   start it on 127.0.0.1:8000 (version: 2.4.4, the default, or 2.5.0)
#   scripts/icecast.sh down           stop and remove it
# Source password: hackme. Admin: admin / hackme. Listen at http://127.0.0.1:8000/<mount>.
set -eu
name=icecast-endpoint-test
case "${1:-up}" in
up)
    version="${2:-2.4.4}"
    docker rm -f "$name" >/dev/null 2>&1 || true
    docker run -d --name "$name" -p 127.0.0.1:8000:8000 \
        -e ICECAST_SOURCE_PASSWORD=hackme -e ICECAST_ADMIN_PASSWORD=hackme -e ICECAST_ADMIN_USERNAME=admin \
        "libretime/icecast:$version" >/dev/null
    for _ in $(seq 50); do
        curl -fs http://127.0.0.1:8000/status-json.xsl >/dev/null && { echo "Icecast $version on 127.0.0.1:8000"; exit 0; }
        sleep 0.2
    done
    echo "Icecast didn't come up:" >&2
    docker logs "$name" >&2
    exit 1
    ;;
down) docker rm -f "$name" >/dev/null ;;
*) echo "usage: $0 up [version] | down" >&2; exit 2 ;;
esac
