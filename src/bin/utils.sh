#!/bin/bash
SEPARATOR=","

function array_join() {
    local array=("$@")
    if [ -z "$separator" ]
    then
        separator=$SEPARATOR
    fi
    local result=$(printf "$separator%s" "${array[@]}")
    echo ${result:1}
}

function log_message() {
    echo -e '['$(date +'%a %Y-%m-%d %H:%M:%S')']' $1
}