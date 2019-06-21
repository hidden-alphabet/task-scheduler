package scheduler

import (
	"log"
	"os"
	"time"
)

/*
  A Result is the output of a worker function
*/
type Result struct {
	Jobs []*Job
	Err  error
}

/*
  A Worker is a function which carries out some task using
  a Job object as context, and returning a Result object.
*/
type Worker func(interface{}) *Result

/*
  A mapping from strings to functions
*/
type Registry map[string]Worker

/*
  A Scheduler manages the distribution of jobs to inactive tasks.
*/
type Scheduler struct {
	Jobs  *CountingChan
	Tasks *CountingChan

	Workers Registry

	Log     *log.Logger
	Verbose bool

	ShouldStop chan bool
}

/*
  Creates a scheduler.
*/
func NewScheduler(verbose bool) *Scheduler {
	var logger *log.Logger

	if verbose {
		logger = log.New(os.Stdout, "[Scheduler] ", LogFlags)
	}

	return &Scheduler{
		Jobs:  NewCountingChan(),
		Tasks: NewCountingChan(),

		Workers: make(Registry),

		Log:     logger,
		Verbose: verbose,
	}
}

/*
  Increase the number of tasks by n.
*/
func (s *Scheduler) ScaleUpByN(n int) error {
	taskCount := s.Tasks.Count

	for id := taskCount; id < taskCount+n; id++ {
		if s.Verbose {
			s.Log.Printf("Starting task.")
		}

		task := NewTask(*s)
		go task.Start()

		s.Tasks.Push(task)
		if s.Verbose {
			s.Log.Printf("Task added to inactive queue.")
		}
	}

	if s.Verbose {
		s.Log.Printf("Scaled available tasks from %d to %d", taskCount, taskCount+n)
	}

	return nil
}

/*
  Register a worker.
*/
func (s *Scheduler) Register(name string, worker Worker) {
	if s.Verbose {
		s.Log.Printf("Registering '%s' Worker.", name)
	}

	s.Workers[name] = worker
}

func (s *Scheduler) Stop() {
	s.ShouldStop <- true

	s.Jobs.Flush()
	s.Tasks.Flush()
}

/*
  Starts a scheduler
*/
func (s *Scheduler) Start() error {
	if s.Verbose {
		s.Log.Printf("Starting.")
	}

	err := s.ScaleUpByN(1)
	if err != nil {
		s.Stop()
		return err
	}

	for {
		select {
		/*
		   If no task has become available in the last second, and the number of waiting jobs
		   is non-zero, than there must not be enough of them, so increase the number of available
		   jobs by 1.
		*/
		case <-time.After(4 * time.Second):
			if s.Verbose {
				s.Log.Printf("No tasks have become available in the last %d seconds.", 4)
			}

			if s.Tasks.Count < s.Jobs.Count {
				err := s.ScaleUpByN(1)
				if err != nil {
					return err
				}
			}

		/*
		   1. Retrieve an inactive task, or fall through if one is not available
		   2. Retrieve an unprocessed job, or block until one is available
		   3. Send the unprocessed job to the task's job queue
		   4. Repeat
		*/
		case item, ok := <-s.Tasks.PopFuture():
			if s.Verbose && ok {
				if s.Verbose {
					s.Log.Printf("Waiting for job.")
				}

				job := s.Jobs.Pop()
				if s.Verbose {
					s.Log.Printf("Retrieved job.")
				}

				task := toTask(item)
				if task == nil {
					s.Log.Printf("Unable to coerce the received item into a task")
					break
				}

				if s.Verbose {
					s.Log.Printf("Sending job to task.")
				}
				task.Jobs.Push(job)
			}
		}
	}

	return nil
}
