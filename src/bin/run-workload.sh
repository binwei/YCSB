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

# Transaction phase regex groups
PROFILE_REGEX=profile\=\([^\ ]*\)[\ ]

function numeric_value_regex() {
    local property=$1
    echo $property\=\([0-9]*\)
}

OPERATIONCOUNT_REGEX=operationcount\=\([0-9]*\)
INSERTSTART_REGEX=insertstart\=\([0-9]*\)

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
    if [[ $workload =~ $LOAD_PHASE_REGEX ]]
    then
        profile=${BASH_REMATCH[1]}
        local sequence=0;
        for ((iteration=0; iteration < iterations; iteration++ ));
        do
            local sequence_incremented=0
            local file="${profile}-${sequence}.txt"
            while [ -e $file ]; do
                ((sequence++))
                sequence_incremented=1
                file="${profile}-${sequence}.txt"
            done
            if (( $sequence_incremented == 0 ))
            then
                ((sequence++))
            fi
            local target_process="mvn -f ${pom} install -P${profile} > ${file}"
            (log_message "Running workload: $target_process")
            eval "$target_process"
        done
    elif [[ $workload =~ $PROFILE_REGEX ]]
        then
            profile=${BASH_REMATCH[1]}
            target=0
            insertstart=0
            if [[ $workload =~ $(numeric_value_regex "target") ]]; then
                target=${BASH_REMATCH[1]}
            fi
            local process="mvn -f ${pom} install -P${profile} -Dycsb.target=${target}"

            if [[ $workload =~ $(numeric_value_regex "operationcount") ]]; then
                local operationcount=${BASH_REMATCH[1]}
                process="$process -Dycsb.operationcount=${operationcount}"
            fi
            if [[ $workload =~ $(numeric_value_regex "insertstart") ]]; then
                local insertstart=${BASH_REMATCH[1]}
                process="$process -Dycsb.insertstart=${insertstart}"
            fi
            if [[ $workload =~ $(numeric_value_regex "recordcount") ]]; then
                local recordcount=${BASH_REMATCH[1]}
                process="$process -Dycsb.recordcount=${recordcount}"
            fi
            local sequence=0;
            for ((iteration=0; iteration < iterations; iteration++ ));
            do
                local sequence_incremented=0
                local file="${profile}-target-${target}-${sequence}.txt"
                while [ -e $file ]; do
                    ((sequence++))
                    sequence_incremented=1
                    file="${profile}-target-${target}-${sequence}.txt"
                done
                if (( $sequence_incremented == 0 ))
                then
                    ((sequence++))
                fi
                local target_process="$process > $file"
                (log_message "Running workload: $target_process")
                eval "$target_process"
            done
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