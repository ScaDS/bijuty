#!/bin/bash
set -e

ACTION=$1
CONF_DIR=$2
NUM_WORKERS_PER_HOST=${3:-1} # Defaults to 1 if not provided

in_slurm_job() {
	local job_id="${SLURM_JOB_ID:-$SLURM_JOBID}"
	[[ -n "$job_id" && "$job_id" =~ ^[0-9]+$ ]]
}

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
	START_ALL_FAILED=0
	LOCAL_RUN=0
	if in_slurm_job; then
		if SPARK_CONF_DIR="${CONF_DIR}" "$SPARK_HOME/sbin/start-all.sh"; then
			sleep 5s
			# Verify master port is open if host/port are known
			if [ -n "$SPARK_MASTER_HOST" ] && [ -n "$SPARK_MASTER_PORT" ]; then
				if ! bash -c "exec 3<>/dev/tcp/${SPARK_MASTER_HOST}/${SPARK_MASTER_PORT}" >/dev/null 2>&1; then
					echo "Warning: Master does not appear to be listening on ${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
					START_ALL_FAILED=1
				fi
			fi
		else
			START_ALL_FAILED=1
		fi
	else
		LOCAL_RUN=1
	fi

	if [[ "$START_ALL_FAILED" -eq 1 ]] || [[ "$LOCAL_RUN" -eq 1 ]]; then
		echo "Falling back to manual master/worker start..."

		# Clean up any partial starts before retrying
		SPARK_CONF_DIR="${CONF_DIR}" "$SPARK_HOME/sbin/stop-all.sh" >/dev/null 2>&1 || true

		echo "Starting Spark Master..."
		SPARK_CONF_DIR="${CONF_DIR}" "$SPARK_HOME/sbin/start-master.sh"

		# Give the master a moment to initialize before workers connect
		sleep 2s

		echo "Starting $NUM_WORKERS Spark Worker(s)..."

		grep -v '^#' "$WORKERS_FILE" | grep -v '^[[:space:]]*$' | while read -r HOST; do
			for ((i = 1; i <= NUM_WORKERS_PER_HOST; i++)); do
				echo "Starting worker #$i on $HOST..."
				ssh "$HOST" "export SPARK_HOME=$SPARK_HOME; SPARK_CONF_DIR=${CONF_DIR} ${SPARK_HOME}/sbin/start-worker.sh spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT" || true
			done
		done
	fi

	echo "Spark cluster started successfully."
	;;

stop)
	STOP_ALL_FAILED=0
	LOCAL_RUN=0
	if in_slurm_job; then
		if SPARK_CONF_DIR="${CONF_DIR}" "$SPARK_HOME/sbin/stop-all.sh"; then
			sleep 5s
			# Verify master port is open if host/port are known
			if [ -n "$SPARK_MASTER_HOST" ] && [ -n "$SPARK_MASTER_PORT" ]; then
				if bash -c "exec 3<>/dev/tcp/${SPARK_MASTER_HOST}/${SPARK_MASTER_PORT}" >/dev/null 2>&1; then
					echo "Warning: Master still appear to be listening on ${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
					STOP_ALL_FAILED=1
				fi
			fi
		else
			STOP_ALL_FAILED=1
		fi
	else
		LOCAL_RUN=1
	fi

	if [[ "$STOP_ALL_FAILED" -eq 1 ]] || [[ "$LOCAL_RUN" -eq 1 ]]; then
		echo "Using fallback method to stop cluster."
		echo "Stopping Spark Worker(s)..."
		grep -v '^#' "$WORKERS_FILE" | grep -v '^[[:space:]]*$' | while read -r HOST; do
			for ((i = 1; i <= NUM_WORKERS_PER_HOST; i++)); do
				echo "Stopping #$i worker on $HOST..."
				ssh "$HOST" "export SPARK_HOME=$SPARK_HOME; SPARK_CONF_DIR=${CONF_DIR} ${SPARK_HOME}/sbin/stop-worker.sh spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT" || true
			done
		done

		echo "Stopping Spark Master..."
		SPARK_CONF_DIR="${CONF_DIR}" "$SPARK_HOME/sbin/stop-master.sh" || true
	fi

	echo "Spark cluster stopped successfully."
	;;

*)
	echo "Invalid action: $ACTION"
	echo "Usage: $0 {start|stop} <spark_conf_dir> [number_of_workers]"
	exit 1
	;;
esac
