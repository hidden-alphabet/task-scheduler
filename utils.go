package scheduler

func toTask(ptr interface{}) *Task {
	if t, ok := ptr.(*Task); ok {
		return t
	}

	return nil
}

func toJob(ptr interface{}) *Job {
	if j, ok := ptr.(*Job); ok {
		return j
	}

	return nil
}
