#!/usr/bin/env bash
DROOP_MAX_DIRECT_MEMORY="1G"
DROOP_HEAP="512M"

export DROOP_JAVA_OPTS="-Xms$DROOP_HEAP -Xmx$DROOP_HEAP -XX:MaxDirectMemorySize=$DROOP_MAX_DIRECT_MEMORY -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=1G"

export DROOP_LOG_DIR="/home/mapr/droop/droop-1.0/logs"