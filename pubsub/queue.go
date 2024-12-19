package pubsub

type Queue struct {
	ch chan interface{}
}

func NewQueue(size int) *Queue {
	return &Queue{
		ch: make(chan interface{}, size),
	}
}

func (q *Queue) Put(item interface{}) {
	q.ch <- item
}

func (q *Queue) Get() interface{} {
	return <-q.ch
}
