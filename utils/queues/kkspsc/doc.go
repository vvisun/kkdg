// Package kkspsc 提供单生产者单消费者（SPSC）无锁队列，基于链表，不限制容量。
//
// 仅允许一个 goroutine 调用 Push（生产者），一个 goroutine 调用 Pop（消费者）。
// 适用于写处理器等单生产者、单消费者场景，避免锁竞争。
package kkspsc
