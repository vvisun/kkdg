// Package kkmpmc 提供多生产者多消费者（MPMC）无锁队列，基于链表，不限制容量。
//
// 多个 goroutine 可并发调用 Push（生产者）和 Pop（消费者）。
// 适用于多路写入、多路消费的通用场景。
package kkmpmc
