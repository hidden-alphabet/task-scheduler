package scheduler

func toTask(ptr interface{}) *Task {
	if t, ok := ptr.(*Task); ok {
		return t
	}

	return nil
}
