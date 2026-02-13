#!/bin/bash
for d in pbbase pbcluster pbrpc; do
    if ls ./"$d"/*.fbs 1>/dev/null 2>&1; then
        echo "Generating Go from $d/*.fbs"
        flatc --go -o "./$d" ./"$d"/*.fbs
    fi
done
