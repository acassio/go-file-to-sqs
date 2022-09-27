package main

import (
	"time"
	"log"
	"github.com/acassio/go-file-to-sqs/publisher"
)

func main()  {

	start := time.Now()
	defer func ()  {
		elapsed := time.Since(start)
		log.Printf("Time Elapsed: %v s\n",elapsed.Seconds())
	}()
	svc := publisher.NewPublisher()
	err := svc.Run()
	if err!=nil{
		panic(err)
	}
}




