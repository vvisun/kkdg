// Package ptoflats 下的子目录存放 FlatBuffers 的 .fbs 源文件及由 flatc / fbs2struct 生成的 Go 代码。
//
// 布局示例：
//
//	ptoflats/pbgate/gate.fbs — schema；fbgate — flatc --go；fbtgate — fbs2struct
//	ptoflats/pbcluster/cluster.fbs、fbcluster、fbtcluster
//	ptoflats/pbrpc/rpc.fbs、fbrpc、fbtrpc
//
// 生成脚本：proto/fb2_genFlatbuffer.bat、proto/fb3_genFbStruct.bat（或 .sh）。
package ptoflats
