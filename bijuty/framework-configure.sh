#!/bin/bash

#Check if the flags "u" or "e" is already set
RESET_FLAG_U_REQUIRED=false; RESET_FLAG_E_REQUIRED=false;
if [[ ! "$-" =~ u ]]; then RESET_FLAG_U_REQUIRED=true;  set -u; fi
if [[ ! "$-" =~ e ]]; then RESET_FLAG_E_REQUIRED=true;  set -e; fi

script=${BASH_SOURCE[0]}
bin=`dirname "$script"`
bin=`cd "$bin"; pwd`

function myExitHandler () {
  exit_code=$?
  if [ $exit_code -ne 0 ]; then
    echo "Script finished with non-zero exit code: $exit_code"
  fi
  # Prevent leaking of set options
  if $RESET_FLAG_U_REQUIRED; then set +u; fi
  if $RESET_FLAG_E_REQUIRED; then set +e; fi
  trap - EXIT
}

# Prevent leaking of set options
trap myExitHandler EXIT

function err_pr {
  echo "[ERROR] $*" >&2
}
function print_nodelist {
  scontrol show hostname $SLURM_NODELIST
}
function get_config_keys {
  set | sed -n -e 's/^\(FRAMEWORK_[^=]*\).*/\1/p'
}
function expand {
  echo ${!1}
}
function create_configuration_dir {
  local FW_NAME=$1
  local FW_TEMPLATE_CONF_DIR=$2
  local FRAMEWORK_CONF_DIR=$3
  FRAMEWORK_CONF_DIR="$(realpath $FRAMEWORK_CONF_DIR)/${FW_NAME}"
  # include framework specific stuff here, such as files to modify (FRAMEWORK_CONF_FILES), file to enumerate slaves (FRAMEWORK_SLAVE_FILE), file to enumerate masters (FRAMEWORK_MASTER_FILE), path to scripts (FRAMEWORK_BIN_DIR)
  local META_CONF=$FW_TEMPLATE_CONF_DIR/meta.conf
  if [ ! -e "$META_CONF" ]; then
    err_pr "No file 'meta.conf' in $FW_TEMPLATE_CONF_DIR"
    return 1
  else
    . "$META_CONF"

    # Check if correct framework template is being used or not
    if [[ "${FRAMEWORK_NAME:-not_set}" == "not_set" ]]; then
      err_pr "Definition of variable FRAMEWORK_NAME is missing in \"$META_CONF\"."
      return 1
    elif [[ "$FRAMEWORK_NAME" != "${FW_NAME^^}" ]]; then
      err_pr "Framework name mismatch: Template uses \"$FRAMEWORK_NAME\", but you want to configure framework \"${FW_NAME^^}\" with it."
      return 1
    else
      local NODES=$SLURM_JOB_NUM_NODES
      local FRAMEWORK_CPUS_PER_NODE=$(scontrol show jobid $SLURM_JOBID | sed -n 's!.*MinCPUsNode=\([0-9]\+\).*!\1!p')
      local FRAMEWORK_MEM_PER_NODE=${SLURM_MEM_PER_NODE:-61440}m
      local FRAMEWORK_CONF_BASE=`dirname $FRAMEWORK_CONF_DIR`
      mkdir -p "$FRAMEWORK_CONF_BASE"
      cp -Tr "$FW_TEMPLATE_CONF_DIR" "$FRAMEWORK_CONF_DIR"
      chmod -R u+w "$FRAMEWORK_CONF_BASE"
      local FRAMEWORK_LOG_DIR=${FRAMEWORK_LOG_DIR:-$FRAMEWORK_CONF_DIR/log}
      local FRAMEWORK_PID_DIR=${FRAMEWORK_PID_DIR:-$FRAMEWORK_CONF_DIR/pid}
      local FRAMEWORK_LOCAL_DIR=/tmp/$USER/cluster-conf-$SLURM_JOB_ID
      local FRAMEWORK_DATA_DIR=${FRAMEWORK_DATA_DIR:-$FRAMEWORK_LOCAL_DIR}
      mkdir -p "$FRAMEWORK_LOG_DIR"
      mkdir -p "$FRAMEWORK_PID_DIR"
      local FRAMEWORK_MASTER_NODE=$(print_nodelist | awk '{print $1}' | sort -u | head -n 1)
      if [ 1 -eq $NODES ]; then
        local FRAMEWORK_SLAVE_NODES=$FRAMEWORK_MASTER_NODE
        echo "$FRAMEWORK_SLAVE_NODES" > "$FRAMEWORK_CONF_DIR/$FRAMEWORK_SLAVE_FILE"
      else
        local FRAMEWORK_SLAVE_NODES=$(print_nodelist | awk '{print $1}' | sort -u | sed ":a;N;s/\\n/,/g;ba")
        print_nodelist | awk '{print $1}' | sort -u > "$FRAMEWORK_CONF_DIR/$FRAMEWORK_SLAVE_FILE"
      fi
      if [ -f "${FRAMEWORK_SLAVE_FILE_ALTS+/does/not/exist}" ]; then
        for altSlave in $FRAMEWORK_SLAVE_FILE_ALTS; do
          [ -f "$FRAMEWORK_CONF_DIR/$altSlave" ] && rm "$FRAMEWORK_CONF_DIR/$altSlave"
          ln -s "$FRAMEWORK_CONF_DIR/$FRAMEWORK_SLAVE_FILE" "$FRAMEWORK_CONF_DIR/$altSlave"
        done
      fi
      #persist keyword replacements:
      cat <<EOF > "$FRAMEWORK_CONF_DIR/config-subs.txt"
NODES=$NODES
FRAMEWORK_MASTER_NODE="$FRAMEWORK_MASTER_NODE"
FRAMEWORK_SLAVE_NODES="$FRAMEWORK_SLAVE_NODES"
FRAMEWORK_CONF_DIR="$FRAMEWORK_CONF_DIR"
FRAMEWORK_LOG_DIR="$FRAMEWORK_LOG_DIR"
FRAMEWORK_PID_DIR="$FRAMEWORK_PID_DIR"
FRAMEWORK_CONF_FILES="$FRAMEWORK_CONF_FILES"
FRAMEWORK_JAVA_HOME="$JAVA_HOME"
FRAMEWORK_CPUS_PER_NODE="$FRAMEWORK_CPUS_PER_NODE"
FRAMEWORK_MEM_PER_NODE="$FRAMEWORK_MEM_PER_NODE"
FRAMEWORK_BIN_DIR="$FRAMEWORK_BIN_DIR"
FRAMEWORK_DATA_DIR="$FRAMEWORK_DATA_DIR"
FRAMEWORK_LOCAL_DIR="$FRAMEWORK_LOCAL_DIR"
EOF
      if [ -n "$FRAMEWORK_MASTER_FILE" ]; then
        echo "$FRAMEWORK_MASTER_NODE" > "$FRAMEWORK_CONF_DIR/$FRAMEWORK_MASTER_FILE"
      fi
      #read keyword replacements
      . $FRAMEWORK_CONF_DIR/config-subs.txt
      #replace keywords:
      for FRAMEWORK_FILE in `echo $FRAMEWORK_CONF_FILES`
      do
        if [ -f "$FRAMEWORK_CONF_DIR/$FRAMEWORK_FILE" ]; then
          for KEY in `get_config_keys`; do
            sed -i "s#$KEY#`expand $KEY`#g" "$FRAMEWORK_CONF_DIR/$FRAMEWORK_FILE"
          done
        fi
      done
      local i=0
      if [ "$FRAMEWORK_DATA_DIR" = "$FRAMEWORK_LOCAL_DIR" ]; then
        for node in $FRAMEWORK_MASTER_NODE $(cat $FRAMEWORK_CONF_DIR/$FRAMEWORK_SLAVE_FILE)
        do
          srun --overlap -N1 -n1 --nodelist=$node bash -c "mkdir -p $FRAMEWORK_LOCAL_DIR"
        done
      else
        for node in $FRAMEWORK_MASTER_NODE $(cat $FRAMEWORK_CONF_DIR/$FRAMEWORK_SLAVE_FILE)
        do
          if [ ! -d $FRAMEWORK_DATA_DIR/$i ]; then
            srun --overlap -N1 -n1 --nodelist=$node bash -c "mkdir -p $(dirname $FRAMEWORK_LOCAL_DIR); mkdir -p $FRAMEWORK_DATA_DIR/$i; if [ -d $FRAMEWORK_LOCAL_DIR ]; then rm -rf $FRAMEWORK_LOCAL_DIR; fi ; ln -s -T $FRAMEWORK_DATA_DIR/$i $FRAMEWORK_LOCAL_DIR"
          fi
          let ++i #post-increment does not work here, as it returns 1, which, with set -e, let's everything fail
        done
      fi
      export ${FW_NAME^^}_CONF_DIR=${FRAMEWORK_CONF_DIR}
    fi
  fi
}

print_usage() {
  cat <<-EOF
Usage: source framework-configure.sh --framework FRAMEWORK_NAME [--template CONF_TEMPLATE_DIR] [--destination CONF_DESTINATION]
  FRAMEWORK_NAME      : Name of the framework to be used.
                        Possible values:
                          spark  : for using Apache Spark
                          flink  : for using Apache Flink
                          hadoop : for using Apache Hadoop
                          kafka  : for using Apache Kafka
  CONF_TEMPLATE_DIR   : Configuration template directory.
  CONF_DESTINATION    : Custom location for configuration
                        files. If not provided, files will
                        be placed in home directory.
EOF
}

####################################################################
# Main method
####################################################################
function main {
  if [[ "$#" -lt "1" ]]; then
    err_pr "Not enough arguments."
    return 1
  else
    FW_TEMPLATE_CONF_DIR=unknown
    while [ $# -gt 0 ]; do
      arg="$1"
      case "$arg" in
          '--help' | '-h') print_usage; return 0 ;;
          '--framework' | '-f')
                if [[ "${2:-not_set}" == "not_set" ]]; then
                  err_pr "No framework specified."
                  return 1
                fi
                case "$2" in
                    'spark'|'hadoop'|'flink'|'kafka') export FW_NAME=$2; shift ;;
                    *) err_pr "Unsupported value ($2) used for parameter \"$arg\"."; return 1 ;;
                esac
                #set default template configuration directory here
                if [[ ! -d "$FW_TEMPLATE_CONF_DIR" ]]; then
                  local FW_TEMPLATE_CONF_DIR_VAR="${FW_NAME^^}_CONF_TEMPLATE"
                  FW_TEMPLATE_CONF_DIR="${!FW_TEMPLATE_CONF_DIR_VAR}"
                fi
                ;;
          '--template' | '-t')
                if [ ! -d "${2:-/does/not/exist}" ]; then
                  err_pr "Please provide a valid directory name with parameter \"$arg\"."
                  return 1
                fi
                FW_TEMPLATE_CONF_DIR=$2;
                shift
                ;;
          '--destination' | '-d')
                if [[ "${2:-not_set}" == "not_set" ]]; then
                  err_pr "Please provide a destination with parameter \"$arg\"."
                  return 1
                fi
                FW_CONFIG_DST=$2
                shift
                ;;
          -*) err_pr "Unsupported parameter \"$arg\" used."; return 1 ;;
          *) print_usage; return 1 ;;
      esac
      shift
    done

    # at least a framework must be specified
    if [[ "${FW_NAME:-not_set}" == "not_set" ]]; then
      err_pr "No framework specified."
      return 1
    else
      create_configuration_dir "$FW_NAME" "$FW_TEMPLATE_CONF_DIR" "${FW_CONFIG_DST:-$HOME/cluster-conf-$SLURM_JOB_ID}"
    fi
  fi
}

main "$@"

# Do not remove following lines, otherwise flags remain set outside
# of the script which e. g. makes tab completion unusable.
if $RESET_FLAG_U_REQUIRED; then set +u; fi
if $RESET_FLAG_E_REQUIRED; then set +e; fi
