// 设计思路：
//   - 客户端与网关之间建立WebSocket连接，每条连接一个读携程 + WriteGroupCnt个分组写携程。
//   - 逻辑服与网关之间建立TCP连接，每个逻辑服与网关之间建立BackendShardCnt条连接，组成一个LogicServer。
package extgate
