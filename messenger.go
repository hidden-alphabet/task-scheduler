package scheduler

type Messenger struct {
  Count int
  Queue chan interface{}
}

func NewMessenger() *Messenger {
  return &Messenger{
    Count: 0,
    Queue: make(chan interface{}, 10000),
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
  tasks := make(chan interface{})

  go func(c chan interface{}) {
    t := <-m.Queue
    c <- t
    m.Count -= 1
  }(tasks)

  return tasks
}

func (m *Messenger) Flush() {
  for _ = range m.Queue {}
}
