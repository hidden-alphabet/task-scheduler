package scheduler_test

import (
	"reflect"
	"testing"

	"."
)

func TestPush(t *testing.T) {
	m := scheduler.NewMessenger()
	foo := 1
	bar := 2

	m.Push(&foo)
	m.Push(&bar)

	t.Run("Pushing from a messenger increments the count", func(t *testing.T) {
		if m.Count != 2 {
			t.Errorf("Expected: messenger.Count = %d, Got: messenger.Count = %d", 2, m.Count)
		}
	})
}

func TestPop(t *testing.T) {
	m := scheduler.NewMessenger()
	foo := 1
	bar := 2

	m.Push(&foo)
	m.Push(&bar)

	item := m.Pop()

	t.Run("Pop should take from the front of the queue", func(t *testing.T) {
		if num, ok := item.(*int); !ok {
			t.Errorf("Expected: num to be type *int, Got: num was of type %s", reflect.TypeOf(num).Name())
		} else {
			if *num != 1 {
				t.Errorf("Expected: num = %d, Got: num = %d", 1, num)
			}
		}
	})

	t.Run("Poping from a messenger decrements it's count", func(t *testing.T) {
		if m.Count != 1 {
			t.Errorf("Expected: Queue to be of length %d, Got: Queue was length %d", 1, m.Count)
		}
	})

}
