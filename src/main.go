package main

import (
    "net/http"
    "io/ioutil"
	"time"
	"encoding/json"
	"sync"
	"log"
)

type StandardNews struct {
	Timestamp uint   `json:"timestamp"`
	Source    string `json:"source`
	Title     string `json:"title"`
	Body      string `json:"body"`
	URL       string `json:"url"`
	Types     string `json:"types"`
}

type TopNews struct {
	Code      int `json:"code"`
	Msg       string `json:"msg`
	NewsList  []struct {
		CTime       string `json:"ctime"`
		Title       string `json:"title"`
		Description string `json:"description"`
		PicURL      string `json:"picUrl"`
		URL         string `json:"url"`
		Source      string `json:"source"`
	}`json:"newsList"`
}

type GeneralNews struct {
	Code     int    `json:"code"`
	Msg      string `json:"msg"`
	NewsList []struct {
		Ctime       string `json:"ctime"`
		Title       string `json:"title"`
		Description string `json:"description"`
		URL         string `json:"url"`
		Source      string `json:"source"`
	} `json:"newslist"`
}

type NewsSource struct {
	URL   string
	Parse func([]byte) ([]StandardNews, error) 
}

var sourceList = map[string]NewsSource {
	"TopNews":     NewsSource {
						URL: "http://api.tianapi.com/topnews/index?key=7e268e56ea8e9188d5d94545cc8626ee", 
						Parse: topNewsToStandard},	
	"GeneralNews": NewsSource {
						URL: "http://api.tianapi.com/generalnews/index?key=7e268e56ea8e9188d5d94545cc8626ee", 
						Parse: generalNewsToStandard},											
}

func generalNewsToStandard(generalNews []byte) ([]StandardNews, error) {
	var g GeneralNews
	err := json.Unmarshal(generalNews, &g)
	if err != nil {
		return nil, err
	}
	standardNews := []StandardNews{}
	for i := range g.NewsList {
		var item StandardNews
		item.Source = "generalnews"
		item.Timestamp = uint(time.Now().Unix())
		item.Title = g.NewsList[i].Title
		item.Body = g.NewsList[i].Description
		item.URL = g.NewsList[i].URL
		standardNews = append(standardNews, item)
	}
	return standardNews, nil
}

func topNewsToStandard(topNews []byte) ([]StandardNews, error) {
	var t TopNews
	err := json.Unmarshal(topNews, &t)
	if err != nil {
		return nil, err
	}
	standardNews := []StandardNews{}
	for i := range t.NewsList {
		var item StandardNews
		item.Source = "topnews"
		item.Timestamp = uint(time.Now().Unix())
		item.Title = t.NewsList[i].Title
		item.Body = t.NewsList[i].Description
		item.URL = t.NewsList[i].URL
		standardNews = append(standardNews, item)
	}
	return standardNews, nil
}


func main() {
	
	ch := make(chan StandardNews, 1024)
	CallAPI(ch)
}

func CallAPI(ch chan StandardNews) {
	wg := sync.WaitGroup{}
	for sourceName, sourceInfo := range sourceList {
		wg.Add(1)
		go func(sourceName string, sourceInfo NewsSource) {
			results, _ := fetch(sourceInfo)
			for i := range results {
				// To Do: Push standardNews to Kafka
				log.Printf("%v", results[i])
			}
			wg.Done()
		}(sourceName, sourceInfo)
	}
	wg.Wait()
}

func fetch(sourceInfo NewsSource) ([]StandardNews, error) {
	res, err := http.Get(sourceInfo.URL)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)
	return sourceInfo.Parse(body)
}