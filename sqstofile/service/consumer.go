package service

import (
	"sync/atomic"
	"encoding/json"
	"sync"
	"strings"
	"strconv"
	"time"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"flag"
	"os"
	"log"
)

//Consumer provides the operation method for consuming messages from the specified queue
// and save them into a file
type Consumer struct {
	session *session.Session
	client  *sqs.SQS
	queueUrl string
	filename string
	mpf,numberWorkers int
	totalMessages count32
}

type Message struct{
	Id,MessageBody *string
}

type count32 int32

func (c *count32) inc(num int32) int32 {
    return atomic.AddInt32((*int32)(c), num)
}

//NewConsumer returns a new Consumer created with the given parameters
func NewConsumer(session *session.Session) *Consumer {

	filename := flag.String("file", "", "file that will be created to store the messages.")
	queue := flag.String("queue", "", "URL of the queue from which messages will be read.")	
	mpf := flag.Int("mpf", 1000, "Approximate number of messages in each file.")
	numberWorkers := flag.Int("max-concurrency", 10, "Max number of concurrent processing")
	
	validate()

	return &Consumer{session: session, client: sqs.New(session),queueUrl:*queue,filename:*filename,mpf:*mpf,numberWorkers:*numberWorkers}
}


func validate(){

	required := []string{"file", "queue"}
	flag.Parse()

	seen := make(map[string]bool)
    flag.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	for _, req := range required {
        if !seen[req] {
            log.Printf("Missing required -%s argument\n", req)
            os.Exit(2) 
        }
    }

	if os.Getenv("AWS_REGION")==""{
		log.Println("AWS_REGION environment variable is not configured")
        os.Exit(2)
	}

}

//Run starts queue consumption and writes messages to the specified file.
func (s *Consumer) Run()  {

	baseDate := time.Now().Format("2006-01-02T15:04:05")
	
	sufix := 0
	count := count32(0)
	var filename string = baseDate+"_"+s.filename
	
	wg := sync.WaitGroup{}	
	wg.Add(s.numberWorkers)

	for i := 0; i < s.numberWorkers; i++ {
		
		go func(){
			defer wg.Done()
			for{
				result,err := s.receiveMessages()

				if err !=nil{
					log.Fatal(err)
				}

				if len(result.Messages)==0{
					break
				}
				
				err = writeFile(filename,result.Messages)
				
				if err !=nil{
					log.Fatal(err)
				}

				err = s.deleteMessages(result.Messages)

				if err !=nil{
					log.Fatal(err)
				}

				count.inc(int32(len(result.Messages)))
				
				if count >= count32(s.mpf){
					sufix++
					count = 0
					filename = baseDate+"_"+s.filename+"_"+strconv.Itoa(sufix)
				}				
				s.totalMessages.inc(int32(len(result.Messages)))
				log.Printf("messages received: %d\n",len(result.Messages))
			}
		}()
	}
	wg.Wait()	

	log.Printf("************\nTotal messages received: %d\n",s.totalMessages)
	
}



func writeFile(path string,messages []*sqs.Message)error{

	f, err := os.OpenFile(path,os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err !=nil{
		return err
	}
	defer f.Close()

	var result string
	for _,v := range messages{
		bytes,err := json.Marshal(Message{Id:v.MessageId,MessageBody:v.Body})
		if err !=nil{
			return err
		}
		result += strings.ReplaceAll(string(bytes),"\n","")+"\n"
	}   
	_, err = f.WriteString(result)

	if err != nil {
		return err
	}
	return nil
}

func (s *Consumer) receiveMessages()(*sqs.ReceiveMessageOutput,error){
	return s.client.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:aws.String(s.queueUrl),
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(5),
	})
}	

func (s *Consumer) deleteMessages(messages []*sqs.Message)error{


	request := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(s.queueUrl),
		Entries: []*sqs.DeleteMessageBatchRequestEntry{},
	}
	
	for _,v := range messages{
		request.Entries = append(request.Entries,&sqs.DeleteMessageBatchRequestEntry{
			Id:v.MessageId,
			ReceiptHandle: v.ReceiptHandle,
		})
	}

	_,err := s.client.DeleteMessageBatch(request)

	if err != nil {
		return err
	}
	return nil
}