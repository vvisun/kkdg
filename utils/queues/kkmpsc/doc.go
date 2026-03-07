// Package kkmpsc 提供多生产者单消费者（MPSC）无锁队列，基于链表，不限制容量。
//
// 多个 goroutine 可并发调用 Push（生产者），仅一个 goroutine 可调用 Pop（消费者）。
// 适用于多路写入、单路消费的场景。
package kkmpsc
