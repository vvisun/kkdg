#!/bin/bash
cd "$(dirname "$0")/.."
echo "Generating fbt struct from .fbs..."
[ -f proto/pbbase/base.fbs ] && go run ./tools/cmd/fbs2struct -o ./proto/pbbase/fbtbase -pkg fbtbase -flatc-import github.com/vvisun/kkdg/proto/pbbase/fbbase ./proto/pbbase/base.fbs
[ -f proto/pbbase/basic.fbs ] && go run ./tools/cmd/fbs2struct -o ./proto/pbbase/fbtbase -pkg fbtbase -flatc-import github.com/vvisun/kkdg/proto/pbbase/fbbase ./proto/pbbase/basic.fbs
[ -f proto/pbcluster/cluster.fbs ] && go run ./tools/cmd/fbs2struct -o ./proto/pbcluster/fbtcluster -pkg fbtcluster -flatc-import github.com/vvisun/kkdg/proto/pbcluster/fbcluster ./proto/pbcluster/cluster.fbs
[ -f proto/pbrpc/rpc.fbs ] && go run ./tools/cmd/fbs2struct -o ./proto/pbrpc/fbtrpc -pkg fbtrpc -flatc-import github.com/vvisun/kkdg/proto/pbrpc/fbrpc ./proto/pbrpc/rpc.fbs
echo "Done."
