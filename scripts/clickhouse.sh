#!/bin/bash
# ClickHouse server management script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CLICKHOUSE_BIN="$PROJECT_DIR/clickhouse"
CLICKHOUSE_DIR="$PROJECT_DIR/.clickhouse"
CONFIG_FILE="$CLICKHOUSE_DIR/config.xml"
PID_FILE="$CLICKHOUSE_DIR/clickhouse.pid"

# ClickHouse version - use a specific stable version for reproducible test output
# Update this when regenerating test expectations
CLICKHOUSE_VERSION="${CLICKHOUSE_VERSION:-25.12.1.646}"

# Download ClickHouse if not present
download() {
    if [ -f "$CLICKHOUSE_BIN" ]; then
        echo "ClickHouse binary already exists"
        "$CLICKHOUSE_BIN" --version
        return 0
    fi

    echo "Downloading ClickHouse v$CLICKHOUSE_VERSION..."

    # Use stable release URL format
    DOWNLOAD_URL="https://github.com/ClickHouse/ClickHouse/releases/download/v${CLICKHOUSE_VERSION}-stable/clickhouse-linux-amd64"

    if ! curl -k -L -f -o "$CLICKHOUSE_BIN" "$DOWNLOAD_URL"; then
        echo "Failed to download from releases, trying builds.clickhouse.com..."
        # Fallback to builds server with version tag
        DOWNLOAD_URL="https://builds.clickhouse.com/master/amd64/clickhouse"
        curl -k -L -o "$CLICKHOUSE_BIN" "$DOWNLOAD_URL"
    fi

    chmod +x "$CLICKHOUSE_BIN"
    echo "Downloaded ClickHouse"
    "$CLICKHOUSE_BIN" --version
}

# Initialize directories
init() {
    mkdir -p "$CLICKHOUSE_DIR"/{data,logs,tmp,user_files,format_schemas}

    if [ ! -f "$CONFIG_FILE" ]; then
        cat > "$CONFIG_FILE" << 'EOF'
<?xml version="1.0"?>
<clickhouse>
    <logger>
        <level>information</level>
        <log>/home/user/doubleclick/.clickhouse/logs/clickhouse-server.log</log>
        <errorlog>/home/user/doubleclick/.clickhouse/logs/clickhouse-server.err.log</errorlog>
        <size>100M</size>
        <count>3</count>
    </logger>

    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <mysql_port>9004</mysql_port>

    <listen_host>127.0.0.1</listen_host>

    <path>/home/user/doubleclick/.clickhouse/data/</path>
    <tmp_path>/home/user/doubleclick/.clickhouse/tmp/</tmp_path>
    <user_files_path>/home/user/doubleclick/.clickhouse/user_files/</user_files_path>
    <format_schema_path>/home/user/doubleclick/.clickhouse/format_schemas/</format_schema_path>

    <mark_cache_size>5368709120</mark_cache_size>

    <users>
        <default>
            <password></password>
            <networks>
                <ip>::1</ip>
                <ip>127.0.0.1</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
        </default>
    </users>

    <profiles>
        <default>
            <max_memory_usage>10000000000</max_memory_usage>
            <load_balancing>random</load_balancing>
        </default>
    </profiles>

    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</clickhouse>
EOF
        echo "Created config file"
    fi
}

# Start the server
start() {
    if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "ClickHouse is already running (PID: $(cat "$PID_FILE"))"
        return 0
    fi

    download
    init

    echo "Starting ClickHouse server..."
    "$CLICKHOUSE_BIN" server --config-file="$CONFIG_FILE" --daemon --pid-file="$PID_FILE"
    sleep 2

    if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "ClickHouse started (PID: $(cat "$PID_FILE"))"
    else
        echo "Failed to start ClickHouse. Check logs at $CLICKHOUSE_DIR/logs/"
        return 1
    fi
}

# Stop the server
stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "PID file not found. ClickHouse may not be running."
        # Try to kill by process name
        pkill -f "clickhouse server" 2>/dev/null
        return 0
    fi

    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Stopping ClickHouse (PID: $PID)..."
        kill "$PID"
        sleep 2
        if kill -0 "$PID" 2>/dev/null; then
            echo "Force killing..."
            kill -9 "$PID"
        fi
    fi
    rm -f "$PID_FILE"
    echo "ClickHouse stopped"
}

# Check status
status() {
    if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "ClickHouse is running (PID: $(cat "$PID_FILE"))"
        "$CLICKHOUSE_BIN" client --query "SELECT version()"
    else
        echo "ClickHouse is not running"
        return 1
    fi
}

# Run client
client() {
    "$CLICKHOUSE_BIN" client "$@"
}

# Force download (remove existing binary first)
force_download() {
    if [ -f "$CLICKHOUSE_BIN" ]; then
        echo "Removing existing ClickHouse binary..."
        rm -f "$CLICKHOUSE_BIN"
    fi
    download
}

# Show version info
version() {
    echo "Configured version: $CLICKHOUSE_VERSION"
    echo "Override with: CLICKHOUSE_VERSION=X.Y.Z.W $0 download"
    if [ -f "$CLICKHOUSE_BIN" ]; then
        echo "Installed:"
        "$CLICKHOUSE_BIN" --version
    else
        echo "Binary not installed yet"
    fi
}

case "$1" in
    download)
        download
        ;;
    force-download)
        force_download
        ;;
    init)
        init
        ;;
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        start
        ;;
    status)
        status
        ;;
    version)
        version
        ;;
    client)
        shift
        client "$@"
        ;;
    *)
        echo "Usage: $0 {download|force-download|init|start|stop|restart|status|version|client}"
        echo ""
        echo "Environment variables:"
        echo "  CLICKHOUSE_VERSION  - Override the ClickHouse version (default: $CLICKHOUSE_VERSION)"
        echo ""
        echo "Examples:"
        echo "  $0 download                           # Download default version"
        echo "  CLICKHOUSE_VERSION=24.3.1.5 $0 force-download  # Download specific version"
        exit 1
        ;;
esac
