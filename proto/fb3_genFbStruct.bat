@echo off
cd /d %~dp0..
echo Generating fbt struct from .fbs...
if exist "proto\ptoflats\pbgate\gate.fbs" go run ./tools/cmd/fbs2struct -o ./proto/ptoflats/pbgate/fbtgate -pkg fbtgate -flatc-import github.com/vvisun/kkdg/proto/ptoflats/pbgate/fbgate ./proto/ptoflats/pbgate/gate.fbs
if exist "proto\ptoflats\pbcluster\cluster.fbs" go run ./tools/cmd/fbs2struct -o ./proto/ptoflats/pbcluster/fbtcluster -pkg fbtcluster -flatc-import github.com/vvisun/kkdg/proto/ptoflats/pbcluster/fbcluster ./proto/ptoflats/pbcluster/cluster.fbs
if exist "proto\ptoflats\pbrpc\rpc.fbs" go run ./tools/cmd/fbs2struct -o ./proto/ptoflats/pbrpc/fbtrpc -pkg fbtrpc -flatc-import github.com/vvisun/kkdg/proto/ptoflats/pbrpc/fbrpc ./proto/ptoflats/pbrpc/rpc.fbs
echo Done.
PAUSE
