#!/bin/bash
for d in pbgate pbcluster pbrpc; do
    fbs_files=(./"$d"/*.fbs)
    if [ -f "${fbs_files[0]}" ]; then
        echo "Generating Go from $d/*.fbs"
        flatc --go -o "./$d" "${fbs_files[@]}"
    fi
done
