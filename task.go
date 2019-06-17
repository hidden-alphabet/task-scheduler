package scheduler

import (
  "os"
  "log"
)

/*
  A Task is an object for managings goroutines. It provides each 
  goroutine with a jobs queue to execute upon and access to its
  supervising Scheduler object.
*/
type Task struct {
  Jobs *Messenger

  SchedulerJobs *Messenger
  SchedulerTasks *Messenger
	SchedulerRegistry *Registry

  Log *log.Logger

  ShouldStop  chan bool
}

/*
  Create a new task.
*/
func NewTask(s Scheduler) *Task {
  return &Task{
    Jobs: NewMessenger(),

    SchedulerJobs: s.Jobs,
    SchedulerTasks: s.Tasks,
    SchedulerRegistry: &s.Workers,

    Log: log.New(os.Stdout, "[Task] ", LogFlags),
  }
}

/*
  Start a worker to listen for available work to be done from
  a job queue.
*/
func (t *Task) Start() {
  t.Log.Printf("Started.")
  t.Log.Printf("Waiting for jobs to become available.")

  /*
    1. Wait until a job becomes available
    2. If the task has received a 'ShouldStop' signal while
       waiting, kill the process. 
    3. Else, get the appropriate worker and yield the data to it
       to process.
  */
  for {
    item := t.Jobs.Pop()
    job := toJob(item)

    select {
    case <-t.ShouldStop:
      t.Log.Printf("Stopping.")
      goto Stop

    default:
      t.Log.Printf("Retrieved a job for a '%s' worker. Distributing.", job.Name)

      worker := (*t.SchedulerRegistry)[job.Name]
      output := worker(job.Context)

      if output == nil {
        t.Log.Printf("Output from worker was empty.")
      } else {
        t.Log.Printf("Received output from worker.")

        if output.Error != nil {
          t.Log.Printf("Worker '%s' generated an error during processing.", job.Name)
          t.Log.Fatal(output.Error)
        }

        if len(output.Jobs) != 0 {
          t.Log.Printf("Worker returned %d jobs. Submitting them for distribution.", len(output.Jobs))

          for _, job := range output.Jobs {
            t.SchedulerJobs.Push(job)
          }
        }
      }
    }
    t.Log.Printf("Completed Work.")
    t.Log.Printf("Adding Self Back To Task Queue.")

    t.SchedulerTasks.Push(t)
  }

  Stop:
  t.Log.Printf("Stopping.")
}

func (t *Task) Stop() {
  t.ShouldStop <- true
  t.Jobs.Flush()
}
