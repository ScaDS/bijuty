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
	echo "Usage: $0 {start|stop} <flink_conf_dir> [number_of_workers]"
	exit 1
fi

WORKERS_FILE="$CONF_DIR/workers"

if [ -f "$WORKERS_FILE" ]; then
	# Count lines that are not empty and don't start with a #
	NUM_WORKERS=$(awk 'NF && $1 !~ /^#/' "$WORKERS_FILE" | wc -l)
else
	echo "Warning: $WORKERS_FILE not found. Defaulting to 1 worker."
	NUM_WORKERS=1
fi

if [ -z "$FLINK_HOME" ]; then
	echo "Error: FLINK_HOME is not set in the environment."
	exit 1
fi

case "$ACTION" in
start)
	echo "Starting Flink cluster..."
	FLINK_CONF_DIR="${CONF_DIR}" "${FLINK_HOME}/bin/start-cluster.sh"

	echo "Flink cluster started successfully."
	;;

stop)

	FLINK_CONF_DIR="${CONF_DIR}" "${FLINK_HOME}/bin/stop-cluster.sh" || true
	echo "Flink cluster stopped successfully."
	;;

*)
	echo "Invalid action: $ACTION"
	echo "Usage: $0 {start|stop} <flink_conf_dir> [number_of_workers]"
	exit 1
	;;
esac
