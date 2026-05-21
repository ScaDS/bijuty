#!/bin/bash
set -e

ACTION=$1
CONF_DIR=$2
NUM_WORKERS_PER_HOST=${3:-1} # Defaults to 1 if not provided

# Validate required parameters
if [ -z "$ACTION" ] || [ -z "$CONF_DIR" ]; then
    echo "Usage: $0 {start|stop} <spark_conf_dir> [number_of_workers]"
    exit 1
fi

ENV_FILE="$CONF_DIR/spark-env.sh"
WORKERS_FILE="$CONF_DIR/workers"

# Source the environment variables
if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
else
    echo "Error: Cannot find $ENV_FILE. Please verify the <spark_conf_dir> parameter."
    exit 1
fi

if [ -f "$WORKERS_FILE" ]; then
    # Count lines that are not empty and don't start with a #
    NUM_WORKERS=$(awk 'NF && $1 !~ /^#/' "$WORKERS_FILE" | wc -l)
else
    echo "Warning: $WORKERS_FILE not found. Defaulting to 1 worker."
    NUM_WORKERS=1
fi

# Ensure SPARK_HOME was populated by the env file
if [ -z "$SPARK_HOME" ]; then
    echo "Error: SPARK_HOME is not set inside $ENV_FILE"
    exit 1
fi

case "$ACTION" in
    start)
        echo "Starting Spark Master..."
        SPARK_CONF_DIR="${CONF_DIR}" "$SPARK_HOME/sbin/start-master.sh"
        
        # Give the master a moment to initialize before workers connect
        sleep 2 

        echo "Starting $NUM_WORKERS Spark Worker(s)..."
        
        grep -v '^#' "$WORKERS_FILE" | grep -v '^[[:space:]]*$' | while read -r HOST; do
            for ((i=1; i<=NUM_WORKERS_PER_HOST; i++)); do
                echo "Starting worker #$i on $HOST..."
                #SPARK_CONF_DIR="${CONF_DIR}" "${SPARK_HOME}/sbin/start-worker.sh" "spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
                ssh "$HOST" "export SPARK_HOME=$SPARK_HOME; SPARK_CONF_DIR=${CONF_DIR} ${SPARK_HOME}/sbin/start-worker.sh spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT" || true
            done
        done
        
        #SPARK_CONF_DIR="${CONF_DIR}" "${SPARK_HOME}/sbin/start-all.sh"
        
        echo "Spark cluster started successfully."
        ;;
        
    stop)
        
        echo "Stopping Spark Worker(s)..."
        grep -v '^#' "$WORKERS_FILE" | grep -v '^[[:space:]]*$' | while read -r HOST; do
            for ((i=1; i<=NUM_WORKERS_PER_HOST; i++)); do
                echo "Stopping #$i worker on $HOST..."
                ssh "$HOST" "export SPARK_HOME=$SPARK_HOME; SPARK_CONF_DIR=${CONF_DIR} ${SPARK_HOME}/sbin/stop-worker.sh spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT" || true
            done
        done
        
        echo "Stopping Spark Master..."
        SPARK_CONF_DIR="${CONF_DIR}" "$SPARK_HOME/sbin/stop-master.sh" || true
        
        echo "Spark cluster stopped successfully."
        ;;
        
    *)
        echo "Invalid action: $ACTION"
        echo "Usage: $0 {start|stop} <spark_conf_dir> [number_of_workers]"
        exit 1
        ;;
esac