#!/bin/bash
cd "$(dirname "$0")/.."
echo "Converting .proto to .fbs (pbgate/pbcluster/pbrpc -> proto/ptoflats/...)..."
go run ./tools/cmd/fbspb proto2fbs ./proto
echo "Done."
