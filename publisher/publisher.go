package publisher

import (
	"sync"
	"bufio"
	"os"
	"log"
	"flag"
	"fmt"
	"strconv"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

)

// Publisher read a file and send messages to a specified queue.
type Publisher struct {
	client  *sqs.SQS
	queueUrl string
	filename string
	reprocess []string
	sync.Mutex
}

// NewPublisher returns a new publisher to process files containing sqs messages
func NewPublisher() *Publisher {

	filename := flag.String("file", "", "filename to process")
	queue := flag.String("queue", "", "Queue that will receive the messages")	
	
	validate()
	
	session,err := session.NewSession(&aws.Config{Region:aws.String(os.Getenv("AWS_REGION"))})
	
	if err!=nil{
		panic(err)
	}
	return &Publisher{client: sqs.New(session),queueUrl:*queue,filename:*filename}
}

// validate args to make sure all required parameters are present.
func validate(){

	required := []string{"file", "queue"}
	flag.Parse()

	seen := make(map[string]bool)
    flag.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	for _, req := range required {
        if !seen[req] {
            fmt.Fprintf(os.Stderr, "missing required -%s argument\n", req)
            os.Exit(2) 
        }
    }

	if os.Getenv("AWS_REGION")==""{
		fmt.Println("AWS_REGION environment variable is not configured")
        os.Exit(2)
	}

}

// Run Process file and send message to the specified queue
func (s *Publisher) Run()error{

	log.Printf("Sending messages from file %s to queue %s",s.filename,s.queueUrl)

	file, err := os.Open(s.filename)
	if err!=nil{
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	chunk := []string{}
	wg := sync.WaitGroup{}
	for {	
		if scanner.Scan(){
			message := scanner.Text()	
			if len(message)==0{
				break
			}
			chunk = append(chunk,message)
			if len(chunk)==10{
				wg.Add(1)
				go s.SendMessages(chunk,&wg)			
				chunk = []string{}
			}
			continue
		}
		if len(chunk)>0{
			wg.Add(1)
			go s.SendMessages(chunk,&wg)		
		}
		break
	}
	wg.Wait()

	if len(s.reprocess)>0{
		log.Printf("%d message(s) will be reprocessed: %+v\n",len(s.reprocess),s.reprocess)
		for _,v := range chunkMessages(10,s.reprocess){
			wg.Add(1)
			go s.SendMessages(v,&wg)
		}
	}
	wg.Wait()

	log.Println("Processing finished!")

	return nil
}

func chunkMessages(chunkSize int,messages []string)[][]string{
	
	var divided [][]string
	for i := 0; i < len(messages); i += chunkSize {
		end := i + chunkSize

		if end > len(messages) {
			end = len(messages)
		}
		divided = append(divided, messages[i:end])
	}
	return divided
}


//SendMessages Delivers up to ten messages to the specified queue
func (s *Publisher) SendMessages(messages []string,wg *sync.WaitGroup)(*sqs.SendMessageBatchOutput,error){

	defer wg.Done()
	entries := []*sqs.SendMessageBatchRequestEntry{}
	for k,v:=range messages{
		entries = append(entries,&sqs.SendMessageBatchRequestEntry{
			MessageBody: aws.String(v),
			Id: aws.String(strconv.Itoa(k)),
		})
	}
	output,err := s.client.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueUrl:aws.String(s.queueUrl),
		Entries : entries,
	})

	
	if err!=nil{
		s.reprocess = append(s.reprocess,messages...)
		return nil,err
	}

	return output,nil
}	
