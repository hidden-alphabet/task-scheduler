package main

import (
	"errors"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"io"
	"log"
	"net/http"
	"os"

	".."
)

type RibbonFarmCrawlJob struct {
	URL    string
	Output *os.File
}

const (
	RibbonFarmCrawlJobName = "RibbonFarmCrawlJob"
)

/*
  Crawls through Ribbon Farm's pagination to get the title of every ribbonfarm post
*/
func main() {
	s := scheduler.NewScheduler(true)

	log.Printf("[Crawler] Registering RibbonFarmCrawlerJob Worker.")

	s.Register(RibbonFarmCrawlJobName, func(ctx interface{}) *scheduler.WorkerOutput {
		output := &scheduler.WorkerOutput{}

		log.Printf("[Crawler] Started RibbonFarmCrawlJob Worker.")

		job, ok := ctx.(RibbonFarmCrawlJob)
		if !ok {
			output.Error = errors.New(fmt.Sprintf("Unable to cast context to '%s'.", RibbonFarmCrawlJobName))
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

		document.Find("h2.entry-title").Find("a.entry-title-link").Each(func(i int, s *goquery.Selection) {
			_, err := io.WriteString(job.Output, s.Text())
			if err != nil {
				log.Fatal(err)
			}

			_, err = io.WriteString(job.Output, "\n")
			if err != nil {
				log.Fatal(err)
			}
		})

		log.Printf("[Crawler] Parsing RibbonFarm Links.")

		if link, exists := document.Find("div.navigation").Find("li.pagination-next").Find("a").Attr("href"); exists {
			log.Printf("[Crawler] Creating RibbonFarmCrawlJob To Parse: %s.", link)

			crawlJob := scheduler.NewJob(RibbonFarmCrawlJobName, RibbonFarmCrawlJob{Output: job.Output, URL: link})
			output.Jobs = append(output.Jobs, crawlJob)

			log.Printf("[Crawler] Done.")
		} else {
			log.Printf("[Crawler] No Links Found.")
		}

		return output
	})

	filename := "ribbonfarm.txt"
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	job := scheduler.NewJob(RibbonFarmCrawlJobName, RibbonFarmCrawlJob{Output: file, URL: "https://ribbonfarm.com"})

	go s.Start()
	log.Printf("[Crawler] Starting.")

	s.Jobs.Push(job)
	log.Printf("[Crawler] Added seed job.")

	<-s.ShouldStop
}
