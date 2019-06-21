package scheduler

import (
	"log"
	"os"
	"time"
)

/*
  A Task is an object for managings goroutines. It provides each
  goroutine with a jobs queue to execute upon and access to its
  supervising Scheduler object.
*/
type Task struct {
	Jobs *CountingChan

	SchedulerJobs     *CountingChan
	SchedulerTasks    *CountingChan
	SchedulerRegistry *Registry

	Log *log.Logger

	ShouldStop chan bool
}

/*
  Create a new task.
*/
func NewTask(s Scheduler) *Task {
	return &Task{
		Jobs: NewCountingChan(),

		SchedulerJobs:     s.Jobs,
		SchedulerTasks:    s.Tasks,
		SchedulerRegistry: &s.Workers,

		Log: log.New(os.Stdout, "[Task] ", LogFlags),

    ShouldStop: make(chan bool, 1),
	}
}

/*
  Start a worker to listen for available work to be done from
  a job queue.
*/
func (t *Task) Start() {
	t.Log.Printf("Started.")

	/*
	   1. Wait until a job becomes available
	   2. If the task has received a 'ShouldStop' signal while
	      waiting, kill the process.
	   3. Else, get the appropriate worker and yield the data to it
	      to process.
	*/
	for {
		t.Log.Printf("Waiting for jobs to become available.")

    select {
    case <-time.After(1 * time.Second):
      t.Log.Printf("Haven't received any jobs in the last second.")
      t.Log.Printf("Stopping.")
      goto Stop

    case item, ok := <-t.Jobs.PopFuture():
      if job := toJob(item); ok && job != nil {
        t.Log.Printf("Retrieved a job for a '%s' worker. Processing.", job.Name)

        worker := (*t.SchedulerRegistry)[job.Name]
        output := worker(job.Context)

        if output == nil {
          t.Log.Printf("No output from worker.")
        } else {
          if output.Err != nil {
            t.Log.Printf("Worker '%s' got an error while processing.", job.Name)
            t.Log.Printf("[Error] %s", output.Err)
          }

          if len(output.Jobs) != 0 {
            t.Log.Printf("Got %d job(s). Submitting.", len(output.Jobs))

            for _, job := range output.Jobs {
              t.SchedulerJobs.Push(job)
            }
          }
        }
      } else {
        t.Log.Printf("Unable to coerce popped object to a 'Job' struct")
      }

      t.Log.Printf("Completed Work.")
    }

		t.SchedulerTasks.Push(t)
    t.Log.Printf("Added Self Back To Task Queue.")
	}

Stop:
	t.Log.Printf("Exiting.")
}

func toJob(ptr interface{}) *Job {
	if j, ok := ptr.(*Job); ok {
		return j
	}

	return nil
}
