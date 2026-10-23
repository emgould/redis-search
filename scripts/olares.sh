#!/usr/bin/env bash
# Deploy and operate redis-search on an Olares box.
#
# Usage: scripts/olares.sh <command>
#
# Commands (first deploy runs them in this order):
#   config        Render secrets (env, GCP key, Redis password) into the app Data folder on the box
#   seed          Snapshot local Docker Redis into the box (before install, or with the app stopped)
#   build         Build linux/amd64 web + ETL images locally
#   import        Import the images into the box's containerd and restart running pods
#   iap-image     Build + import the gcloud image for the public Redis IAP tunnel (rarely changes)
#   install       Package the chart, upload it to Olares Market, and install it
#   etl-catchup   Run one ETL on the box from YYYY-MM-DD; records the first olares resume state
#   upgrade       Package the chart, re-upload it, and upgrade the installed app
#   status        Show pods and services
#
# Code changes (after first deploy):
#   deploy        Build + import the working tree, restart web + ETL, verify health
#   rollback      Point the live images back at an earlier deployed tag (no arg: list tags)
#
# Lifecycle (data persists in the app Data folder):
#   stop          Suspend the app (all pods); refuses during the ETL window like deploy
#   start         Resume the app and wait for Redis to load and web /health to pass
#
# Local clients:
#   tunnel        Serve the box's web API on localhost:9001 (TUNNEL_PORT overrides) until Ctrl-C
#
# Images are tagged :olares (what the pods run) and :<git sha>[-dirty-<hash>] (for rollback).
# deploy/rollback refuse to run 2-5 AM Eastern or while an ETL run is active (ALLOW_ETL_WINDOW=1 overrides).
#
# Environment overrides:
#   OLARES_SSH       SSH host alias for the box (default: olares)
#   OLARES_USER      Olares username / namespace suffix (default: 2019065722)
#   OLARES_MM_URL    Media Manager URL reachable from the box (default: http://192.168.4.210:8098)
#   LOCAL_REDIS      Local Docker Redis container to snapshot (default: redis-search-redis-1)
#   FORCE=1          Allow `seed` to replace existing Redis data on the box (old AOF is kept as a .bak dir)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

APP="redissearch"
OLARES_SSH="${OLARES_SSH:-olares}"
OLARES_USER="${OLARES_USER:-2019065722}"
OLARES_MM_URL="${OLARES_MM_URL:-http://192.168.4.210:8098}"
LOCAL_REDIS="${LOCAL_REDIS:-redis-search-redis-1}"
NAMESPACE="${APP}-${OLARES_USER}"
CHART_DIR="deploy/olares/${APP}"
LOCAL_ENV="config/local.env"
GCP_KEY_FILE="config/media-circle-3db28338ecf4.json"
WEB_REPO="redissearch-web"
ETL_REPO="redissearch-etl"
IAP_REPO="redissearch-iap"
LIVE_TAG="olares"
ETL_PREFIX_OLARES="redis-search/etl/olares"

remote() {
    ssh -o BatchMode=yes "$OLARES_SSH" "$@"
}

die() {
    echo "❌ $*" >&2
    exit 1
}

app_data_dir() {
    local base
    base="$(remote "ls -d /olares/rootfs/userspace/pvc-userspace-${OLARES_USER}-*/Data" 2>/dev/null | head -1)"
    [[ -n "$base" ]] || die "Could not find the Olares Data folder for user ${OLARES_USER}"
    echo "${base}/${APP}"
}

redis_running_on_box() {
    local replicas
    replicas="$(remote "kubectl get deploy redis -n ${NAMESPACE} -o jsonpath='{.status.replicas}' 2>/dev/null" || true)"
    [[ -n "$replicas" && "$replicas" != "0" ]]
}

cmd_config() {
    [[ -f "$LOCAL_ENV" ]] || die "$LOCAL_ENV not found (run: make decrypt ENV=local)"
    [[ -f "$GCP_KEY_FILE" ]] || die "$GCP_KEY_FILE not found"

    local data_dir cfg password
    data_dir="$(app_data_dir)"
    cfg="${data_dir}/config"
    echo "📁 App data: ${data_dir}"

    remote "sudo install -d -o 1000 -g 1000 -m 755 '${data_dir}' && sudo install -d -o root -g root -m 700 '${cfg}'"

    # Keep the existing Redis password so clients and the AOF-backed instance stay in sync.
    password="$(remote "sudo cat '${cfg}/redis_password' 2>/dev/null" || true)"
    if [[ -z "$password" ]]; then
        password="$(openssl rand -hex 32)"
        echo "🔑 Generated new Redis password"
    else
        echo "🔑 Reusing existing Redis password"
    fi

    printf '%s\n' "$password" | remote "sudo sh -c 'umask 077; cat > \"${cfg}/redis_password\"'"
    remote "sudo sh -c 'umask 077; cat > \"${cfg}/gcp-key.json\"'" < "$GCP_KEY_FILE"
    {
        grep -vE '^(ENVIRONMENT|REDIS_HOST|REDIS_PORT|REDIS_PASSWORD|GCS_ETL_PREFIX|GOOGLE_APPLICATION_CREDENTIALS|MEDIA_MANAGER_API_URL|ETL_CONFIG_PATH)=' "$LOCAL_ENV"
        echo ""
        echo "# --- Olares overrides (scripts/olares.sh) ---"
        echo "ENVIRONMENT=local"
        echo "REDIS_HOST=redis"
        echo "REDIS_PORT=6379"
        echo "REDIS_PASSWORD=${password}"
        echo "GCS_ETL_PREFIX=${ETL_PREFIX_OLARES}"
        echo "GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp-key.json"
        echo "MEDIA_MANAGER_API_URL=${OLARES_MM_URL}"
        echo "ETL_CONFIG_PATH=/app/config/etl_jobs.yaml"
    } | remote "sudo sh -c 'umask 077; cat > \"${cfg}/env\"'"

    echo "✅ Wrote ${cfg}/{env,gcp-key.json,redis_password}"
    echo "   Pods read these at startup; restart them to pick up changes (make olares-import restarts web + ETL)."
}

cmd_seed() {
    local data_dir rdb_dst before after status newest local_sum remote_sum
    data_dir="$(app_data_dir)"
    rdb_dst="${data_dir}/redis/dump.rdb"

    if redis_running_on_box; then
        die "Redis is running on the box. Stop the app first: make olares-stop"
    fi
    if remote "test -d '${data_dir}/redis/appendonlydir'" && [[ "${FORCE:-0}" != "1" ]]; then
        die "Box already has Redis data (${data_dir}/redis/appendonlydir). Re-run with FORCE=1 to replace it."
    fi

    echo "📸 Snapshotting local Redis (${LOCAL_REDIS})..."
    before="$(docker exec "$LOCAL_REDIS" redis-cli LASTSAVE)"
    docker exec "$LOCAL_REDIS" redis-cli BGSAVE >/dev/null
    while :; do
        sleep 2
        after="$(docker exec "$LOCAL_REDIS" redis-cli LASTSAVE)"
        [[ "$after" != "$before" ]] && break
        status="$(docker exec "$LOCAL_REDIS" redis-cli INFO persistence | tr -d '\r' | awk -F: '/^rdb_bgsave_in_progress/{print $2}')"
        [[ "$status" == "1" ]] || die "BGSAVE is not in progress and LASTSAVE did not change; check local Redis logs"
    done
    status="$(docker exec "$LOCAL_REDIS" redis-cli INFO persistence | tr -d '\r' | awk -F: '/^rdb_last_bgsave_status/{print $2}')"
    [[ "$status" == "ok" ]] || die "Local BGSAVE failed (rdb_last_bgsave_status=${status})"

    newest="$(docker exec "$LOCAL_REDIS" redis-cli FT.SEARCH idx:media '*' SORTBY modified_at DESC LIMIT 0 1 RETURN 1 modified_at | tail -1)"

    echo "🚚 Streaming dump.rdb to the box..."
    remote "sudo install -d -o root -g root -m 755 '${data_dir}/redis'"
    if remote "test -d '${data_dir}/redis/appendonlydir'"; then
        local stamp
        stamp="$(date +%Y%m%d-%H%M%S)"
        echo "   Moving existing appendonlydir to appendonlydir.bak-${stamp}"
        remote "sudo mv '${data_dir}/redis/appendonlydir' '${data_dir}/redis/appendonlydir.bak-${stamp}'"
    fi
    docker exec "$LOCAL_REDIS" cat /data/dump.rdb | remote "sudo sh -c 'cat > \"${rdb_dst}\"'"

    local_sum="$(docker exec "$LOCAL_REDIS" sha256sum /data/dump.rdb | awk '{print $1}')"
    remote_sum="$(remote "sudo sha256sum '${rdb_dst}'" | awk '{print $1}')"
    [[ "$local_sum" == "$remote_sum" ]] || die "Checksum mismatch after copy (local ${local_sum}, box ${remote_sum})"
    echo "✅ Seeded ${rdb_dst} (sha256 ${local_sum:0:12}…). Redis loads it on next start, then enables AOF."
    echo "   Newest media document in this snapshot: $(date -r "$newest" '+%Y-%m-%d %H:%M %Z')"
    echo "   After install, backfill from the day before that: scripts/olares.sh etl-catchup YYYY-MM-DD"
}

cmd_etl_catchup() {
    local start_date="${1:-}"
    [[ "$start_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "Usage: scripts/olares.sh etl-catchup YYYY-MM-DD"
    echo "▶️  Running ETL on the box from ${start_date} (records resume state under ${ETL_PREFIX_OLARES})..."
    remote "kubectl exec -n ${NAMESPACE} deploy/etl -- bash -c 'set -a && . /secrets/env && set +a && cd /app && python -m etl.run_nightly_etl --start-date ${start_date}'"
}

# Same working tree always yields the same tag, so build and import can run as separate steps.
build_tag() {
    local sha dirty
    sha="$(git rev-parse --short=12 HEAD)"
    if [[ -n "$(git status --porcelain)" ]]; then
        dirty="$({ git diff HEAD; git status --porcelain; } | shasum | cut -c1-7)"
        echo "${sha}-dirty-${dirty}"
    else
        echo "$sha"
    fi
}

# Script goes over stdin so the matcher never sees its own command line. Kept out of $(...)
# because macOS bash 3.2 mis-parses case patterns in a heredoc inside command substitution.
etl_active_pids() {
    remote "kubectl exec -i -n ${NAMESPACE} deploy/etl -- sh -s" <<'SCRIPT'
for p in /proc/[0-9]*; do
    case "$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null)" in
        (*etl.run_nightly_etl*) echo "${p#/proc/}" ;;
    esac
done
SCRIPT
}

guard_etl_window() {
    [[ "${ALLOW_ETL_WINDOW:-0}" == "1" ]] && return 0
    local hour active
    hour=$((10#$(TZ=America/New_York date +%H)))
    if (( hour >= 2 && hour < 5 )); then
        die "It is ${hour}:xx Eastern, inside the nightly MM warmup/ETL window (2-5 AM). Set ALLOW_ETL_WINDOW=1 to override."
    fi
    active="$(etl_active_pids)" || die "Could not check the etl pod for an active ETL run. Set ALLOW_ETL_WINDOW=1 to skip the check."
    if [[ -n "$active" ]]; then
        die "An ETL run is active in the etl pod (pids: $(echo "$active" | tr '\n' ' ')). Wait for it, or set ALLOW_ETL_WINDOW=1."
    fi
}

restart_and_verify() {
    echo "🔄 Restarting web and ETL..."
    remote "kubectl rollout restart deploy/${APP} deploy/etl -n ${NAMESPACE}"
    remote "kubectl rollout status deploy/${APP} -n ${NAMESPACE} --timeout=180s && kubectl rollout status deploy/etl -n ${NAMESPACE} --timeout=180s"
    verify_web_health
}

verify_web_health() {
    echo "🩺 Checking web /health..."
    remote "kubectl exec -i -n ${NAMESPACE} deploy/${APP} -c web -- python -" <<'PY'
import time
import urllib.request

for attempt in range(30):
    try:
        with urllib.request.urlopen("http://127.0.0.1:8080/health", timeout=5) as resp:
            print(f"web /health: {resp.status}")
            raise SystemExit(0 if resp.status == 200 else 1)
    except OSError:
        time.sleep(2)
print("web /health: no response after 60s")
raise SystemExit(1)
PY
}

record_deploy() {
    local tag="$1" action="$2" data_dir
    data_dir="$(app_data_dir)"
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') ${action} ${tag}" \
        | remote "sudo sh -c 'cat >> \"${data_dir}/deploy-history\"'"
}

cmd_build() {
    local tag
    tag="$(build_tag)"
    echo "🔨 Building ${WEB_REPO}:${tag} (linux/amd64)..."
    docker buildx build --platform linux/amd64 --load -f docker/cloudrun.Dockerfile \
        -t "${WEB_REPO}:${LIVE_TAG}" -t "${WEB_REPO}:${tag}" .
    echo "🔨 Building ${ETL_REPO}:${tag} (linux/amd64)..."
    docker buildx build --platform linux/amd64 --load -f docker/etl.Dockerfile \
        -t "${ETL_REPO}:${LIVE_TAG}" -t "${ETL_REPO}:${tag}" .
}

import_images() {
    local tag
    tag="$(build_tag)"
    for image in "${WEB_REPO}:${tag}" "${ETL_REPO}:${tag}"; do
        docker image inspect "$image" >/dev/null 2>&1 || die "${image} is not built; the working tree changed since the last build. Run build first."
    done
    # Olares K3s uses the system containerd (/run/containerd), not the K3s-embedded one.
    echo "📦 Importing ${tag} into containerd on ${OLARES_SSH}..."
    docker save --platform linux/amd64 \
        "${WEB_REPO}:${LIVE_TAG}" "${WEB_REPO}:${tag}" "${ETL_REPO}:${LIVE_TAG}" "${ETL_REPO}:${tag}" \
        | remote "sudo ctr -n k8s.io images import -" >/dev/null
}

box_has_image() {
    remote "sudo ctr -n k8s.io images ls -q 2>/dev/null | grep -qx 'docker.io/library/$1'"
}

cmd_iap_image() {
    echo "🔨 Building ${IAP_REPO}:${LIVE_TAG} (linux/amd64)..."
    docker buildx build --platform linux/amd64 --load -f docker/iap-tunnel.Dockerfile \
        -t "${IAP_REPO}:${LIVE_TAG}" docker
    echo "📦 Importing ${IAP_REPO}:${LIVE_TAG} into containerd on ${OLARES_SSH}..."
    docker save --platform linux/amd64 "${IAP_REPO}:${LIVE_TAG}" \
        | remote "sudo ctr -n k8s.io images import -" >/dev/null
    echo "✅ Imported. Running pods pick it up on restart (make olares-upgrade applies chart changes)."
}

cmd_import() {
    import_images
    if remote "kubectl get ns ${NAMESPACE}" >/dev/null 2>&1; then
        restart_and_verify
    fi
}

cmd_deploy() {
    local tag
    guard_etl_window
    tag="$(build_tag)"
    [[ "$tag" == *-dirty-* ]] && echo "⚠️  Deploying uncommitted changes as ${tag}"
    cmd_build
    import_images
    restart_and_verify
    record_deploy "$tag" deploy
    echo "✅ Deployed ${tag}. Roll back with: make olares-rollback TAG=<tag>"
}

cmd_rollback() {
    local tag="${1:-}" data_dir
    data_dir="$(app_data_dir)"
    if [[ -z "$tag" ]]; then
        echo "Deploy history (newest last):"
        remote "sudo tail -n 15 '${data_dir}/deploy-history' 2>/dev/null || echo '  (none yet)'"
        echo "Tags present on the box:"
        remote "sudo ctr -n k8s.io images ls -q 2>/dev/null | grep '/${WEB_REPO}:' | sed 's|.*:||' | grep -vx '${LIVE_TAG}' | sed 's/^/  /'"
        echo "Usage: make olares-rollback TAG=<tag>"
        return 0
    fi
    guard_etl_window
    for repo in "$WEB_REPO" "$ETL_REPO"; do
        remote "sudo ctr -n k8s.io images ls -q 2>/dev/null | grep -qx 'docker.io/library/${repo}:${tag}'" \
            || die "docker.io/library/${repo}:${tag} is not on the box"
    done
    echo "⏪ Pointing :${LIVE_TAG} at ${tag}..."
    for repo in "$WEB_REPO" "$ETL_REPO"; do
        remote "sudo ctr -n k8s.io images tag --force docker.io/library/${repo}:${tag} docker.io/library/${repo}:${LIVE_TAG} 2>/dev/null" >/dev/null
    done
    restart_and_verify
    record_deploy "$tag" rollback
    echo "✅ Rolled back to ${tag}"
}

package_chart() {
    local out="$1"
    helm lint "$CHART_DIR" --set userspace.appData=/lint >/dev/null
    helm package "$CHART_DIR" -d "$out" | awk '{print $NF}'
}

upload_chart() {
    local tmp tgz
    # imagePullPolicy: Never, so a missing image leaves the web pod unready.
    box_has_image "${IAP_REPO}:${LIVE_TAG}" || die "${IAP_REPO}:${LIVE_TAG} is not on the box. Run: make olares-iap-image"
    tmp="$(mktemp -d)"
    tgz="$(package_chart "$tmp")"
    remote "mkdir -p /tmp/${APP}-chart"
    scp -q "$tgz" "${OLARES_SSH}:/tmp/${APP}-chart/"
    remote "olares-cli market upload /tmp/${APP}-chart/$(basename "$tgz")"
}

cmd_install() {
    upload_chart
    remote "olares-cli market install ${APP} -s upload --watch --watch-timeout 20m"
}

cmd_upgrade() {
    upload_chart
    remote "olares-cli market upgrade ${APP} -s upload --watch --watch-timeout 20m"
}

cmd_status() {
    remote "kubectl get pods,svc -n ${NAMESPACE} -o wide"
}

app_state() {
    remote "olares-cli market status ${APP} -o json 2>/dev/null" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["state"])'
}

cmd_stop() {
    local state
    state="$(app_state)" || die "Could not read ${APP} state (olares-cli profile login on the box?)"
    # The backend rejects stop on a stopped app (HTTP 500), despite the CLI's idempotency claim.
    if [[ "$state" != "stopped" ]]; then
        [[ "$state" == "running" ]] && guard_etl_window
        echo "⏹️  Stopping ${APP} (was: ${state})..."
        remote "olares-cli market stop ${APP} --watch --watch-timeout 5m"
    fi
    # Olares reports stopped before pods exit; Redis gets up to 120s to flush, and seed must not race it.
    echo "⏳ Waiting for pods to exit..."
    remote "kubectl wait --for=delete pod --all -n ${NAMESPACE} --timeout=180s" >/dev/null
    echo "✅ ${APP} stopped"
}

# Redis readiness waits for the AOF load (redis-cli ping answers LOADING until done), so allow 10m.
cmd_start() {
    local state
    state="$(app_state)" || die "Could not read ${APP} state (olares-cli profile login on the box?)"
    if [[ "$state" != "running" ]]; then
        echo "▶️  Starting ${APP} (was: ${state})..."
        remote "olares-cli market resume ${APP} --watch --watch-timeout 10m"
    fi
    remote "kubectl rollout status deploy/redis -n ${NAMESPACE} --timeout=600s \
        && kubectl rollout status deploy/${APP} -n ${NAMESPACE} --timeout=180s \
        && kubectl rollout status deploy/etl -n ${NAMESPACE} --timeout=180s"
    verify_web_health
}

# kubectl port-forward rather than the Service IP: Olares' auth proxy rejects host-to-pod traffic (401).
# The pty ties the remote port-forward to this session so Ctrl-C or a dropped connection stops it.
# port-forward exits when its pod is replaced (deploy/rollback), so the loop reconnects to the new pod.
cmd_tunnel() {
    local port="${TUNNEL_PORT:-9001}"
    local box_port=$((port + 10000))
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        die "localhost:${port} is in use (local web container? make web-docker-down). Or pick another: TUNNEL_PORT=<port>"
    fi
    if remote "ss -Hltn 'sport = :${box_port}'" | grep -q .; then
        die "Port ${box_port} is in use on the box (stale tunnel? kubectl hides its args, so check: sudo ss -ltnp 'sport = :${box_port}')"
    fi
    echo "🚇 localhost:${port} -> ${APP} web on ${OLARES_SSH} (Ctrl-C to stop)"
    ssh -tt -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
        -L "${port}:127.0.0.1:${box_port}" "$OLARES_SSH" \
        "while :; do kubectl port-forward -n ${NAMESPACE} svc/${APP} ${box_port}:8080; sleep 2; done"
}

case "${1:-}" in
    config) cmd_config ;;
    seed) cmd_seed ;;
    build) cmd_build ;;
    import) cmd_import ;;
    iap-image) cmd_iap_image ;;
    install) cmd_install ;;
    etl-catchup) cmd_etl_catchup "${2:-}" ;;
    upgrade) cmd_upgrade ;;
    deploy) cmd_deploy ;;
    rollback) cmd_rollback "${2:-}" ;;
    status) cmd_status ;;
    tunnel) cmd_tunnel ;;
    stop) cmd_stop ;;
    start) cmd_start ;;
    *)
        sed -n '2,37p' "$0"
        exit 1
        ;;
esac
