@echo off
cd /d %~dp0..
echo Generating fb struct from .fbs...
if exist "proto\pbbase\base.fbs" go run ./tools/cmd/fbs2struct -o ./proto/pbbase/fbbase -pkg fbbase ./proto/pbbase/base.fbs
if exist "proto\pbbase\basic.fbs" go run ./tools/cmd/fbs2struct -o ./proto/pbbase/fbbase -pkg fbbase ./proto/pbbase/basic.fbs
if exist "proto\pbcluster\cluster.fbs" go run ./tools/cmd/fbs2struct -o ./proto/pbcluster/fbcluster -pkg fbcluster ./proto/pbcluster/cluster.fbs
if exist "proto\pbrpc\rpc.fbs" go run ./tools/cmd/fbs2struct -o ./proto/pbrpc/fbrpc -pkg fbrpc ./proto/pbrpc/rpc.fbs
echo Done.
PAUSE
