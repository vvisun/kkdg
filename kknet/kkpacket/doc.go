package kkpacket

/*

整包格式：
stream = [length,message]
message = [head,body]

length = byte count of message
head = msgID + seq + ...
body = object(binary data of object)

*/
