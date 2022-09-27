package main

import (
	"time"
	"log"
	"github.comm/acassio/go-tools/filetosqs/service"
)

func main()  {

	start := time.Now()
	defer func ()  {
		elapsed := time.Since(start)
		log.Printf("Time Elapsed: %v s\n",elapsed.Seconds())
	}()
	svc := service.NewPublisher()
	err := svc.Run()
	if err!=nil{
		panic(err)
	}
}




