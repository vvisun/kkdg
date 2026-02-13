#!/bin/bash
cd "$(dirname "$0")/.."
echo "Generating fb struct from .fbs..."
[ -f proto/pbbase/base.fbs ] && go run ./tools/cmd/fbs2struct -o ./proto/pbbase/fbbase -pkg fbbase ./proto/pbbase/base.fbs
[ -f proto/pbbase/basic.fbs ] && go run ./tools/cmd/fbs2struct -o ./proto/pbbase/fbbase -pkg fbbase ./proto/pbbase/basic.fbs
[ -f proto/pbcluster/cluster.fbs ] && go run ./tools/cmd/fbs2struct -o ./proto/pbcluster/fbcluster -pkg fbcluster ./proto/pbcluster/cluster.fbs
[ -f proto/pbrpc/rpc.fbs ] && go run ./tools/cmd/fbs2struct -o ./proto/pbrpc/fbrpc -pkg fbrpc ./proto/pbrpc/rpc.fbs
echo "Done."
