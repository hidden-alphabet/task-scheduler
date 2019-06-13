package scheduler

import (
  "os"
  "log"
  "time"
)

var (
  LogFlags = log.Ldate | log.Lmicroseconds | log.Lshortfile
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
  A Job is an arbitrary object for providing context to each 
  worker during execution.
*/
type Job struct {
  Name string
  Context interface{}
}

/*
  A Task is an object for managings goroutines. It provides each 
  goroutine with a jobs queue to execute upon and access to its
  supervising Scheduler object.
*/
type Task struct {
  Done        chan bool
  Jobs        chan *Job
	Scheduler   *Scheduler

  Logger *log.Logger
}

/*
  Scheduler's manage a queue of goroutines, distributing work
  to them when they become available. It keeps a count of the
  number of total and active tasks at any given moment.
*/
type Scheduler struct {
  TotalTasks int
  ActiveTasks int
  WaitingJobs int

  Queue chan *Job
  Tasks chan *Task
  Registrar map[string]Worker

  Done chan bool

  Logger *log.Logger
}

/*
  Create a new scheduler. 
*/
func New() *Scheduler {
  return &Scheduler{
    TotalTasks: 0,
    ActiveTasks: 0,

    Queue: make(chan *Job, 10),
    Tasks: make(chan *Task, 1000),
    Registrar: make(map[string]Worker),

    Logger: log.New(os.Stdout, "[Scheduler] ", LogFlags),
  }
}

/*
  Starts a scheduler
*/
func (s *Scheduler) Start() error {
  s.Logger.Printf("Starting.")

  go s.Run()

  err := s.Scale(1)
  if err != nil {
    s.Shutdown()
    return err
  }

  return nil
}

/*
  Start delegating jobs out to workers.
*/
func (s *Scheduler) Run() error {
  for {
    s.Logger.Printf("Waiting Jobs: %d", s.WaitingJobs)
    s.Logger.Printf("Active Workers: %d/%d", s.ActiveTasks, s.TotalTasks)

    select {
    case <-s.Done:
      s.Logger.Printf("Stopping.")
      break
    case t := <-s.Tasks:
      s.Logger.Printf("Task became available for work.")
      s.Logger.Printf("Waiting for job to send to task.")

      job := <-s.Queue

      s.Logger.Printf("Retrieved job.")

      t.Jobs <- job

      s.ActiveTasks += 1
      s.WaitingJobs -= 1
    case <-time.After(1 * time.Second):
      s.Logger.Printf("No tasks have become available in the last %d seconds.", 3)
      s.Logger.Printf("Scaling up the number of available tasks by %d", 1)

      if s.WaitingJobs != 0 {
        err := s.Scale(1)
        if err != nil {
          return err
        }
      }
    }
  }

  return nil
}

/*
  Stop scheduler and all associated tasks.
*/
func (s *Scheduler) Shutdown() {
  s.Done <- true

  /*
    Stop all running tasks.
  */
  for {
    select {
    case t := <-s.Tasks:
      s.Logger.Printf("Stopping task.")
      t.Done <- true
    default:
      if s.TotalTasks == 0 {
        break
      }
    }
  }

  close(s.Tasks)
  close(s.Queue)
}

/*
  Increase the number of tasks available to do work by n. 
*/
func (s *Scheduler) Scale(n int) error {
  current := s.TotalTasks

  s.Logger.Printf("Scaling available tasks from %d to %d", current, current+n)

  for id := current; id < current+n; id++ {
    task := s.NewTask()

    s.Logger.Printf("Starting task.")
    go task.Run()

    s.Tasks <- task
  }

  return nil
}

/*
  Register a new worker.
*/
func (s *Scheduler) RegisterWorker(name string, worker Worker) {
  s.Logger.Printf("Registered worker '%s'", name)
  s.Registrar[name] = worker
}

/*
  Create a new job.
*/
func (s *Scheduler) NewJob(name string, ctx interface{}) *Job {
  s.Logger.Printf("Created job for '%s' worker.", name)
  return &Job{
    Name: name,
    Context: ctx,
  }
}

/*
  Schedule a job to be run
*/
func (s *Scheduler) SubmitJob(job *Job) {
  s.Logger.Printf("Submitting job for distribution to a '%s' worker.", job.Name)
  s.Queue <- job
}

/*
  Create a new task.
*/
func (s *Scheduler) NewTask() *Task {
  s.TotalTasks += 1

  return &Task{
    Jobs: make(chan *Job, 10),
    Scheduler: s,
    Logger: log.New(os.Stdout, "", LogFlags),
  }
}

/*
  Start a worker to listen for available work to be done from
  a job queue.
*/
func (t *Task) Run() {
  t.Logger.Printf("Started.")

  registrar := t.Scheduler.Registrar

  t.Logger.Printf("Waiting for jobs to become available.")

	for job := range t.Jobs {
    select {
    case <-t.Done:
      t.Logger.Printf("Stopping.")
      t.Scheduler.TotalTasks -= 1
      break
    default:
    }

    t.Logger.Printf("Retrieved a job for a '%s' worker. Distributing.", job.Name)

    worker := registrar[job.Name]
    output := worker(job.Context)

    if output == nil {
      t.Logger.Printf("Output from worker was empty.")
      continue
    }

    t.Logger.Printf("Received output from worker.")

    if output.Error != nil {
      t.Logger.Printf("Worker '%s' generated an error during processing.", job.Name)
      t.Logger.Fatal(output.Error)
    }

    if len(output.Jobs) != 0 {
      t.Logger.Printf("Worker returned %d jobs. Submitting them for distribution.", len(output.Jobs))

      for _, job := range output.Jobs {
        t.Jobs <- job
      }
    }

    t.Logger.Printf("Adding Self Back To Task Queue.")

    t.Scheduler.Tasks <- t
    t.Scheduler.ActiveTasks -= 1

    t.Logger.Printf("Completed Work.")
  }

  t.Logger.Printf("Done.")
}
