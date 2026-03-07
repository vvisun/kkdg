// Package kkspmc 提供单生产者多消费者（SPMC）无锁队列，基于链表，不限制容量。
//
// 仅一个 goroutine 可调用 Push（生产者），多个 goroutine 可并发调用 Pop（消费者）。
// 适用于单路写入、多路消费的场景。
package kkspmc
