package main

import (
	"github.com/acassio/go-sqs-publisher/publisher"
	"fmt"
)

func main()  {

	svc := publisher.NewPublisher()
	err := svc.Run()
	if err!=nil{
		panic(err)
	}
	fmt.Println("messages sent!")

}




