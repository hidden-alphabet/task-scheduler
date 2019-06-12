package scheduler

import (
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
}

/*
  Create a new scheduler. 
*/
func New() (*Scheduler, error) {
  log.Printf("[Scheduler] Created Scheduler.")

  scheduler := &Scheduler{
    Registrar: make(map[string]Worker),
    Queue: make(chan *Job, 10),
    Tasks: make(chan *Task, 1000),
    TotalTasks: 0,
    ActiveTasks: 0,
  }

  go scheduler.Run()

  err := scheduler.Scale(1)
  if err != nil {
    return nil, err
  }

	return scheduler, nil
}

/*
  Start delegating jobs out to workers.
*/
func (s *Scheduler) Run() error {
  log.Printf("[Scheduler] Started.")

  for {
    select {
    case <-s.Done:
      log.Printf("[Scheduler] Stopping.")
      break
    case t := <-s.Tasks:
      log.Printf("[Scheduler] Task became available for work.")

      log.Printf("[Scheduler] Waiting for job to send to task.")
      job := <-s.Queue
      log.Printf("[Scheduler] Retrieved job.")

      t.Jobs <- job

      s.ActiveTasks += 1
      s.WaitingJobs -= 1
    case <-time.After(3 * time.Second):
      log.Printf("[Scheduler] No tasks have become available in the last %d seconds.", 3)
      log.Printf("[Scheduler] Scaling up the number of available tasks by %d", 1)
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

  for {
    if s.TotalTasks == 0 {
      break
    }

    select {
    case t := <-s.Tasks:
      log.Printf("[Scheduler] Stopping task.")
      t.Done <- true
    default:
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

  log.Printf("[Scheduler] Scaling available tasks from %d to %d", current, current+n)

  for id := current; id < current+n; id++ {
    task := &Task{
      Jobs: make(chan *Job, 10),
      Scheduler: s,
    }

    log.Printf("[Scheduler] Starting task.")
    go task.Run()

    s.Tasks <- task
    s.TotalTasks += 1
  }

  return nil
}

/*
  Register a new worker.
*/
func (s *Scheduler) RegisterWorker(name string, worker Worker) {
  log.Printf("[Scheduler] Registering '%s' Worker.", name)
  s.Registrar[name] = worker
}

/*
  Create a new job.
*/
func (s *Scheduler) NewJob(name string, ctx interface{}) *Job {
  log.Printf("[Scheduler] Created job for '%s' worker.", name)
  return &Job{
    Name: name,
    Context: ctx,
  }
}

/*
  Schedule a job to be run
*/
func (s *Scheduler) SubmitJob(job *Job) {
  log.Printf("[Scheduler] Submitting job for distribution to a '%s' worker.", job.Name)
  s.Queue <- job
}

/*
  Start a worker to listen for available work to be done from
  a job queue.
*/
func (t *Task) Run() {
  log.Printf("[Task] Starting.")

  registrar := t.Scheduler.Registrar

  log.Printf("[Task] Waiting for jobs to become available.")

	for job := range t.Jobs {
    select {
    case <-t.Done:
      log.Printf("[Task] Stopping.")
      t.Scheduler.TotalTasks -= 1
      break
    default:
    }

    log.Printf("[Task] Retrieved a job for a '%s' worker. Distributing.", job.Name)

    worker := registrar[job.Name]
    output := worker(job.Context)

    if output == nil {
      log.Printf("[Task] Output from worker was empty.")
      continue
    }

    log.Printf("[Task] Received output from worker.")

    if output.Error != nil {
      log.Printf("[Task] Worker '%s' generated an error during processing.", job.Name)
      log.Fatal(output.Error)
    }

    if len(output.Jobs) != 0 {
      log.Printf("[Task] Worker returned %d jobs. Submitting them for distribution.", len(output.Jobs))

      for _, job := range output.Jobs {
        t.Jobs <- job
      }
    }

    log.Printf("[Task] Completed Work.")

    t.Scheduler.Tasks <- t
    t.Scheduler.ActiveTasks -= 1
	}

  log.Printf("[Task] Stopping.")
}
