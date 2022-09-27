package main

import (
	"time"
	"log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/acassio/go-tools/sqstofile/service"
	"os"
)

func main()  {

	start := time.Now()
	defer func ()  {
		elapsed := time.Since(start)
		log.Printf("Time Elapsed: %v s\n",elapsed.Seconds())
	}()

	sess,err := session.NewSession(&aws.Config{Region:aws.String(os.Getenv("AWS_REGION"))})
	if err!=nil{
		panic(err)
	}
	consumer :=  service.NewConsumer(sess)
	consumer.Run()	
	
}
