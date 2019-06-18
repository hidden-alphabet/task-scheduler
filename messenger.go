package scheduler

type Messenger struct {
	Count        int
	Queue chan interface{}
}

func NewMessenger() *Messenger {
	return &Messenger{
		Count: 0,
		Queue: make(chan interface{}, 10),
	}
}

func (m *Messenger) Push(item interface{}) {
	m.Queue <- item
	m.Count += 1
}

func (m *Messenger) Pop() interface{} {
	item := <-m.Queue
	m.Count -= 1

	return item
}

func (m *Messenger) AsyncPop() chan interface{} {
	q := make(chan interface{})

	go func(q chan interface{}) {
		for item := range m.Queue {
			q <- item
			m.Count -= 1
		}
	}(q)

	return q
}

func (m *Messenger) Flush() {
	for range m.Queue {
	}
}
