package scheduler

type Messenger interface {
  Pop() interface{}
  PopFuture() <-chan interface{}

  Push(interface{})
  PushFuture(interface{})
}

type CountingChan struct {
	Count int
	Queue chan interface{}

  FutureQueue *chan interface{}
}

func NewCountingChan() *CountingChan {
	return &CountingChan{
		Count: 0,
		Queue: make(chan interface{}, 1000),
	}
}

func (cc *CountingChan) Push(item interface{}) {
	cc.Queue <- item
	cc.Count += 1
}

func (cc *CountingChan) Pop() interface{} {
	item := <-cc.Queue
	cc.Count -= 1

	return item
}

func (cc *CountingChan) PopFuture() chan interface{} {
  if cc.FutureQueue == nil {
    q := make(chan interface{}, 1)
    cc.FutureQueue = &q
  }

  go func(queue chan interface{}) {
      queue <- cc.Pop()
  }(*cc.FutureQueue)

	return *(cc.FutureQueue)
}

func (cc *CountingChan) Flush() {
	for range cc.Queue {
	}
}
