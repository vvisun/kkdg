package kkpacket

/*

整包格式：packet = [length,head,body]

[length]: 存放[message]的长度。占2或4个字节。默认占4个字节。
[head]: 存放消息头[head]。(mid,seq,...)
[body]: 存放消息体[body]。(object的二进制数据)

stream = [length,message]
message = [head,body]

*/
