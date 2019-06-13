package main

// NOTE: While the task is good for driving API design, this
// implementation is - by design - sequential, and can't make
// use of the concurrency management offered by scheduler. Instead,
// to better demonstrate the use case, we should have a second job
// which gets the name of the first commenter for every post.

import (
  "log"
  "sync"
  "errors"
  "net/http"
  "github.com/PuerkitoBio/goquery"
 // "github.com/hidden-alphabet/scheduler"
  ".."
)

const RibbonFarmCrawlJobName = "RibbonFarmCrawlJobName"

type RibbonFarmCrawlJob struct {
  URL string
  Titles *[]string
  Mutex *sync.Mutex
}

/*
  Crawls through Ribbon Farm's pagination to get the title of every ribbonfarm post
*/
func main() {
  s := scheduler.New()
  err := s.Start()
  if err != nil {
    log.Fatal(err)
  }

  log.Printf("[Crawler] Registering RibbonFarmCrawlerJob Worker.")

  s.RegisterWorker(RibbonFarmCrawlJobName, func(ctx interface{}) *scheduler.WorkerOutput {
    output := &scheduler.WorkerOutput{}

    log.Printf("[Crawler] Started RibbonFarmCrawlJob Worker.")

    job, ok := ctx.(RibbonFarmCrawlJob)
    if !ok {
      output.Error = errors.New("Unable to cast context to CrawlJob.")
      return output
    }

    log.Printf("[Crawler] Downloading HMTL from: %s", job.URL)

    res, err := http.Get(job.URL)
    if err != nil {
      output.Error = err
      return output
    }

    log.Printf("[Crawler] Parsing HTML.")

    document, err := goquery.NewDocumentFromReader(res.Body)
    if err != nil {
      output.Error = err
      return output
    }

    log.Printf("[Crawler] Parsing RibbonFarm Titles.")

    document.Find("div.post").Each(func(i int, s *goquery.Selection) {
      job.Mutex.Lock()
      *job.Titles = append(*job.Titles, s.Text())
      job.Mutex.Unlock()
    })

    log.Printf("[Crawler] Parsing RibbonFarm Links.")

    if link, exists := document.Find("div.navigation").Find("li.pagination-next").Find("a").Attr("href"); exists {

      log.Printf("[Crawler] Creating RibbonFarmCrawlJob To Parse: %s.", link)

      crawlJob := s.NewJob(RibbonFarmCrawlJobName, RibbonFarmCrawlJob{
        URL: link,
        Titles: job.Titles,
        Mutex: job.Mutex,
      })

      output.Jobs = append(output.Jobs, crawlJob)
    } else {
      log.Printf("[Crawler] No Links Found.")

      return output
    }

    log.Printf("[Crawler] Done.")

    return output
  })

  titles := []string{}

  log.Printf("[Crawler] Starting.")

  s.SubmitJob(s.NewJob(RibbonFarmCrawlJobName, RibbonFarmCrawlJob{
    URL: "https://ribbonfarm.com",
    Titles: &titles,
    Mutex: &sync.Mutex{},
  }))

  <-s.Done
}
