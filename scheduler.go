package scheduler

import (
  "os"
  "log"
  "time"
)

/*
  A WorkerOutput is the required output of a worker function 
*/
type WorkerOutput struct {
  Jobs []*Job
  Error error
}

/*
  A Worker is a function which carries out some task using 
  a Job object as context, and returning a WorkerOutput object. 
*/
type Worker func(interface{}) *WorkerOutput

/*
A mapping from strings to functions
*/
type Registry map[string]Worker

/*
  A Scheduler manages the distribution of jobs to inactive tasks. 
*/
type Scheduler struct {
  Jobs *Messenger
  Tasks *Messenger

  Workers Registry

  Log *log.Logger
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
    Jobs: NewMessenger(),
    Tasks: NewMessenger(),

    Workers: make(Registry),

    Log: logger,
    Verbose: verbose,
  }
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
    if s.Verbose {
      s.Log.Printf("Idle Jobs: %d", s.Jobs.Count)
      s.Log.Printf("Idle Tasks: %d", s.Tasks.Count)
    }


    select {
    /*
      If we receive a 'Done' signal, kill the scheduler.
    */
    case <-s.ShouldStop:
      goto Stop

    /* 
      1. Retrieve an inactive task, or fall through if one is not available
      2. Retrieve an unprocessed job, or block until one is available
      3. Send the unprocessed job to the task's job queue
      4. Repeat
    */
  case item := <-s.Tasks.AsyncPop():
      if s.Verbose {
        s.Log.Printf("Waiting for job.")
      }

      task := toTask(item)
      if task == nil {
        s.Log.Printf("Unable to coerce the received item into a task")
        break
      }
      task.Jobs.Push(s.Jobs.Pop())

    /*
      If no task has become available in the last second, and the number of waiting jobs
      is non-zero, than there must not be enough of them, so increase the number of available
      jobs by 1.
    */
    case <-time.After(1 * time.Second):
      if s.Verbose {
        s.Log.Printf("No tasks have become available in the last %d seconds.", 1)
      }

      var err error
      if s.Jobs.Count != 0 {
        err = s.ScaleUpByN(1)
      } else {
        err = s.ScaleDownByN(1)
      }

      if err != nil {
        return err
      }
    }
  }

  Stop:
  return nil
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
  }

  if s.Verbose {
    s.Log.Printf("Scaled available tasks from %d to %d", taskCount, taskCount+n)
  }

  return nil
}

/*
  Decrease the number of tasks by n
*/
func (s *Scheduler) ScaleDownByN(n int) error {
  taskCount := s.Tasks.Count

  for id := taskCount; id < taskCount+n; id++ {
    if s.Verbose {
      s.Log.Printf("Stopping task.")
    }

    item := s.Tasks.Pop()
    task := toTask(item)
    task.Stop()
  }

  if s.Verbose {
    s.Log.Printf("Scaled available tasks from %d to %d", taskCount, taskCount-n)
  }

  return nil
}

/*
  Register a worker.
*/
func (s *Scheduler) Register(name string, worker Worker) {
  if s.Verbose {
    s.Log.Printf("[Scheduler] Registering '%s' Worker.", name)
  }

  s.Workers[name] = worker
}
