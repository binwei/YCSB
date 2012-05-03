#!/bin/bash
BIN_HOME=`dirname $0`

. $BIN_HOME/utils.sh
. $BIN_HOME/workload-env.sh

POM=../pom.xml
ITERATIONS=5

EXIT_OK=0
EXIT_INVALID_OPTION=1
EXIT_ARGUMENT_REQUIRED=2

# Regular expressions have to be unquoted
# Quoting the string argument to the [[ command's =~ operator forces string matching, as with the other pattern-matching operators.
LOAD_PHASE_REGEX=^profile\=\([^\ ]*\)$
TRANSACTION_PHASE_REGEX=^profile\=\([^\ ]*\)[\ ]*target\=\([0-9]*\)[\ ]*operationcount\=\([0-9]*\)$

function show_usage() {
    if [ -n "$1" ]
    then
        echo "$(basename $0): $1"
    fi
    echo -e "usage: $(basename $0) [-p pom.xml] [-w workloads] [-i iterations]"
}

function run_workload() {
    local pom=$1
    local workload=$2
    local iterations=$3
    if [[ $workload =~ $TRANSACTION_PHASE_REGEX ]]
    then
        profile=${BASH_REMATCH[1]}
        target=${BASH_REMATCH[2]}
        operationcount=${BASH_REMATCH[3]}
        for ((iteration=0; iteration < iterations; iteration++ ));
        do
            process="mvn -f ${pom} install -P${profile} -Dycsb.target=${target} -Dycsb.operationcount=${operationcount} > ${profile}-target-${target}-${iteration}.txt"
            (log_message "Running workload: $process")
            eval "$process"
        done
    elif [[ $workload =~ $LOAD_PHASE_REGEX ]]
    then
        profile=${BASH_REMATCH[1]}
        for ((iteration=0; iteration < iterations; iteration++ ));
        do
            process="mvn -f ${pom} install -P${profile} > ${profile}.txt"
            (log_message "Running workload: $process")
            eval "$process"
        done
    else
        (log_message "Workload do not match neither load nor transaction phase: \"$workload\"")
    fi
}

# Process supplied parameters
while getopts ":p:w:i:h" opt; do
  case $opt in
    i)
        iterations=$OPTARG
    ;;
    w)
        workloads=$OPTARG
    ;;
    h)
        (show_usage)
        exit $EXIT_OK
    ;;
    p)
        pom=$OPTARG
    ;;
    \?)
        (show_usage "invalid option -$OPTARG")
        exit $EXIT_INVALID_OPTION
    ;;
    :)
        (show_usage "option -$OPTARG requires an argument")
        exit $EXIT_ARGUMENT_REQUIRED
    ;;
  esac
done

if [ -z "$pom" ]
then
  pom=$POM
fi

if [ -z "$iterations" ]
then
  iterations=$ITERATIONS
fi

# Split workloads string into an array
IFS=$SEPARATOR read -r -a workloads <<< "$workloads"

for (( i=0; i < ${#workloads[@]}; i++ ));
do
  workload=${workloads[$i]}
  (run_workload "$pom" "$workload" "$iterations")
done