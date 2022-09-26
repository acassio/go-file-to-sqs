package main

import (
	"github.com/acassio/go-sqs-publisher/publisher"
)

func main()  {

	svc := publisher.NewPublisher()
	err := svc.Run()
	if err!=nil{
		panic(err)
	}
}




