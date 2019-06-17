package scheduler

import (
  "os"
  "fmt"
  "log"
)

/*
  A Job is an arbitrary object for providing context to each 
  worker during execution.
*/
type Job struct {
  Name string
  Context interface{}

  Logger *log.Logger
}

/*
  Create a new job.
*/
func NewJob(name string, ctx interface{}) *Job {
  job := &Job{
    Name: name,
    Context: ctx,
    Logger: log.New(os.Stdout, fmt.Sprintf("[%s] ", name), LogFlags),
  }

  return job
}
