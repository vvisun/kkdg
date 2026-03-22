@echo off
cd /d %~dp0..
echo Generating fbt struct from .fbs...
if exist "proto\pbgate\gate.fbs" go run ./tools/cmd/fbs2struct -o ./proto/pbgate/fbtgate -pkg fbtgate -flatc-import github.com/vvisun/kkdg/proto/pbgate/fbgate ./proto/pbgate/gate.fbs
if exist "proto\pbcluster\cluster.fbs" go run ./tools/cmd/fbs2struct -o ./proto/pbcluster/fbtcluster -pkg fbtcluster -flatc-import github.com/vvisun/kkdg/proto/pbcluster/fbcluster ./proto/pbcluster/cluster.fbs
if exist "proto\pbrpc\rpc.fbs" go run ./tools/cmd/fbs2struct -o ./proto/pbrpc/fbtrpc -pkg fbtrpc -flatc-import github.com/vvisun/kkdg/proto/pbrpc/fbrpc ./proto/pbrpc/rpc.fbs
echo Done.
PAUSE
