#!/bin/bash
cd "$(dirname "$0")"
for d in pbgate pbcluster pbrpc; do
    fbs_files=(./ptoflats/"$d"/*.fbs)
    if [ -f "${fbs_files[0]}" ]; then
        echo "Generating Go from ptoflats/$d/*.fbs"
        flatc --go -o "./ptoflats/$d" "${fbs_files[@]}"
    fi
done
