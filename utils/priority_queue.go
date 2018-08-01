package utils

import (
    "container/heap";
)

type PriorityQueue interface {
    Len() int
    Push(float32, interface{})
    Pop() interface{}
}

type priorityQueue struct {
    queue heap.Interface
}

type priorityQueueItem struct {
    priority float32
    value interface{}
}

func NewMinPriorityQueue() *priorityQueue {
    queue := make(minPriorityQueue, 0)
    heap.Init(&queue)

    return &priorityQueue {
        queue: &queue,
    }
}

func NewMaxPriorityQueue() *priorityQueue {
    queue := make(maxPriorityQueue, 0)
    heap.Init(&queue)

    return &priorityQueue {
        queue: &queue,
    }
}

func (pq *priorityQueue) Len() int {
    return pq.queue.Len()
}

func (pq *priorityQueue) Push(priority float32, value interface{}) {
    if priority < 0 {
        panic("Negative priority")
    }
    
    heap.Push(pq.queue, &priorityQueueItem{priority, value})
}

func (pq *priorityQueue) Pop() interface{} {
    if pq.Len() == 0 {
        panic("Empty priority queue")
    }

    item := heap.Pop(pq.queue).(*priorityQueueItem)
    return item.value
}

type minPriorityQueue []*priorityQueueItem

type maxPriorityQueue []*priorityQueueItem

func (pq minPriorityQueue) Len() int { return len(pq) }

func (pq minPriorityQueue) Less(i, j int) bool {
    return pq[i].priority < pq[j].priority
}

func (pq minPriorityQueue) Swap(i, j int) {
    pq[i], pq[j] = pq[j], pq[i]
}

func (pq *minPriorityQueue) Pop() interface{} {
    queue := *pq
    item := queue[len(queue) - 1]
    *pq = queue[0:len(queue) - 1]
    return item
}

func (pq *minPriorityQueue) Push(val interface{}) {
    *pq = append(*pq, val.(*priorityQueueItem))
}

func (pq maxPriorityQueue) Len() int { return len(pq) }

func (pq maxPriorityQueue) Less(i, j int) bool {
    return pq[i].priority > pq[j].priority
}

func (pq maxPriorityQueue) Swap(i, j int) {
    pq[i], pq[j] = pq[j], pq[i]
}

func (pq *maxPriorityQueue) Pop() interface{} {
    queue := *pq
    item := queue[len(queue) - 1]
    *pq = queue[0:len(queue) - 1]
    return item
}

func (pq *maxPriorityQueue) Push(val interface{}) {
    *pq = append(*pq, val.(*priorityQueueItem))
}
