#!/bin/sh
# Prepare /etc/easy_proxies (auto-generate missing files, guard against
# Docker bind-mount foot-guns), then exec easy_proxies.

set -eu
umask 077

CONFIG_DIR="${EASY_PROXIES_CONFIG_DIR:-/etc/easy_proxies}"
CONFIG_FILE="$CONFIG_DIR/config.yaml"
NODES_FILE="$CONFIG_DIR/nodes.txt"
EXAMPLE_CONFIG="${EASY_PROXIES_EXAMPLE_CONFIG:-/app/config.example.yaml}"
BINARY="${EASY_PROXIES_BINARY:-/usr/local/bin/easy_proxies}"

# When a host bind-mount targets a single file that does not exist yet
# (e.g. -v ./data/nodes.txt:/etc/easy_proxies/nodes.txt), Docker creates a
# HOST DIRECTORY at that path and mounts it. The container then sees a
# directory where a file is expected and the app fails opaquely. Detect
# this and exit with a concrete fix instead of a confusing runtime crash.
die() {
    echo "[easy_proxies] ERROR: $1" >&2
    echo "$2" >&2
    exit 1
}

# Respect the config selected by Go flags. A custom config must already exist;
# do not prepare unrelated default mounts when the caller chooses another path.
selected_config="$CONFIG_FILE"
expect_config=false
for arg in "$@"; do
    if [ "$expect_config" = true ]; then
        selected_config="$arg"
        expect_config=false
        continue
    fi
    case "$arg" in
        --config|-config) expect_config=true ;;
        --config=*|-config=*) selected_config="${arg#*=}" ;;
        --help|-help|-h) exec "$BINARY" "$@" ;;
    esac
done
[ "$expect_config" = false ] || die "--config requires a path." "Example: --config /etc/easy_proxies/config.yaml"
[ -n "$selected_config" ] || die "The config path is empty." "Specify an existing config file."
if [ "$selected_config" != "$CONFIG_FILE" ]; then
    [ ! -d "$selected_config" ] || die "$selected_config is a directory, not a file." "Mount an existing config file at the selected path."
    [ -f "$selected_config" ] && [ -r "$selected_config" ] || die "Cannot read selected config: $selected_config" "Create the file and check its mount permissions."
    exec "$BINARY" "$@"
fi
if [ "$#" -eq 0 ]; then set -- --config "$CONFIG_FILE"; fi

fix_perm_hint="Ensure the mounted directory is writable by the container user:
    mkdir -p data && chown -R $(id -u):$(id -g) data"

# --- config.yaml: reject directory, generate if absent ---
if [ -d "$CONFIG_FILE" ]; then
    die "$CONFIG_FILE is a directory, not a file." \
"This happens when you bind-mount a host file that does not exist yet; Docker
creates a directory instead. Fix depends on your mount configuration:

  Directory mount (-v ./data:/etc/easy_proxies):
    rm -rf ./data/config.yaml
    cp config.example.yaml ./data/config.yaml
    docker compose up -d

  File mount (-v ./config.yaml:/etc/easy_proxies/config.yaml):
    rm -rf ./config.yaml
    cp config.example.yaml ./config.yaml
    docker compose up -d"
fi
if [ ! -e "$CONFIG_FILE" ]; then
    [ -w "$CONFIG_DIR" ] || die "Cannot create $CONFIG_FILE ($CONFIG_DIR not writable)." "$fix_perm_hint"
    cp "$EXAMPLE_CONFIG" "$CONFIG_FILE" || die "Failed to create $CONFIG_FILE." "$fix_perm_hint"
    echo "[easy_proxies] Generated default config from $EXAMPLE_CONFIG"
fi

# --- nodes.txt: reject directory, create empty file if absent ---
if [ -d "$NODES_FILE" ]; then
    die "$NODES_FILE is a directory, not a file." \
"This happens when you bind-mount a host file that does not exist yet; Docker
creates a directory instead. Fix depends on your mount configuration:

  Directory mount (-v ./data:/etc/easy_proxies):
    rm -rf ./data/nodes.txt
    touch ./data/nodes.txt
    docker compose up -d

  File mount (-v ./nodes.txt:/etc/easy_proxies/nodes.txt):
    rm -rf ./nodes.txt
    touch ./nodes.txt
    docker compose up -d"
fi
if [ ! -e "$NODES_FILE" ]; then
    [ -w "$CONFIG_DIR" ] || die "Cannot create $NODES_FILE ($CONFIG_DIR not writable)." "$fix_perm_hint"
    touch "$NODES_FILE" || die "Failed to create $NODES_FILE." "$fix_perm_hint"
    echo "[easy_proxies] Created empty nodes.txt"
fi

# --- final readability check ---
if [ ! -r "$CONFIG_FILE" ]; then
    die "Cannot read $CONFIG_FILE (permission denied)." \
"Fix on the host:
    chown -R $(id -u):$(id -g) data
    chmod 644 data/config.yaml data/nodes.txt"
fi

exec "$BINARY" "$@"
