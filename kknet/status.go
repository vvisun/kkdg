package kknet

type ConnStatus = int32 //连接状态

const (
	ConnStatusInit       ConnStatus = iota //初始状态
	ConnStatusConnecting                   //连接中
	ConnStatusConnected                    //已连接
	ConnStatusClosing                      //关闭中
	ConnStatusClosed                       //已关闭
)
