package service

import (
	"encoding/json"
	"sync/atomic"
	"sync"
	"bufio"
	"os"
	"log"
	"strconv"
	"flag"
	"fmt"
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
	rawContent bool
	totalMessages, totalSuccess,totalFailure count32
	sync.Mutex
}

type count32 int32

func (c *count32) inc(num int32) int32 {
    return atomic.AddInt32((*int32)(c), num)
}

func (c *count32) get() int32 {
    return atomic.LoadInt32((*int32)(c))
}

// NewPublisher returns a new publisher to process files containing sqs messages
func NewPublisher() *Publisher {

	filename := flag.String("file", "", "filename to process")
	queue := flag.String("queue", "", "Queue that will receive the messages")	
	rawContent := flag.Bool("raw-content", false, "Messages should be interpreted as raw")	
	
	validate()
	
	session,err := session.NewSession(&aws.Config{Region:aws.String(os.Getenv("AWS_REGION"))})
	
	if err!=nil{
		panic(err)
	}
	return &Publisher{client: sqs.New(session),queueUrl:*queue,filename:*filename,rawContent:*rawContent}
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

	//Scans the file, sending messages in batches of 10
	chunk := []string{}
	wg := sync.WaitGroup{}
	for {	
		if scanner.Scan(){
			message := scanner.Text()	
			if len(message)==0{
				break
			}
			s.totalMessages++
			chunk = append(chunk,message)
			if len(chunk)==10{
				wg.Add(1)
				go s.SendMessages(chunk,&wg,false)			
				chunk = []string{}
			}
			continue
		}
		if len(chunk)>0{
			wg.Add(1)
			go s.SendMessages(chunk,&wg,false)		
		}
		break
	}

	wg.Wait()

	//Verify if there are any messages to reprocess.
	if len(s.reprocess)>0{
		dataReprocess := s.reprocess
		s.reprocess = []string{}
		log.Printf("%d message(s) will be reprocessed:\n",len(dataReprocess))
		for _,v := range chunkMessages(10,dataReprocess){
			wg.Add(1)
			go s.SendMessages(v,&wg,true)
		}
	}

	wg.Wait()

	//Print Results
	if len(s.reprocess)>0{
		log.Printf("The following %d message(s) could not be sent: %+v\n",len(s.reprocess),s.reprocess)
	}

	log.Printf("TOTAL MESSAGES: %d\nTOTAL SUCCESS: %d\nTOTAL FAILURE: %d\n",s.totalMessages,s.totalSuccess,s.totalFailure)

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
func (s *Publisher) SendMessages(messages []string,wg *sync.WaitGroup,reprocess bool)(*sqs.SendMessageBatchOutput,error){

	defer wg.Done()
	entries := s.getEntries(messages)
	
	output,err := s.client.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueUrl:aws.String(s.queueUrl),
		Entries : entries,
	})
	
	if err!=nil{
		log.Println(err)
		s.Lock()
		s.reprocess = append(s.reprocess,messages...)
		s.Unlock()
		if reprocess{
			s.totalFailure.inc(int32(len(messages)))
		}
		return nil,err
	}

	s.totalSuccess.inc(int32(len(messages)))

	return output,nil
}	


func (s *Publisher) getEntries(messages []string)[]*sqs.SendMessageBatchRequestEntry{
	
	entries := []*sqs.SendMessageBatchRequestEntry{}
	
	if !s.rawContent{
		for _,v:=range messages{
			entry := sqs.SendMessageBatchRequestEntry{}

			err := json.Unmarshal([]byte(v),&entry)
			if err!=nil{
				fmt.Println(err)
				panic(err)
			}
			entries = append(entries,&entry)
		}
		return entries
	}
	
	for k,v:=range messages{
		entries = append(entries,&sqs.SendMessageBatchRequestEntry{
			MessageBody: aws.String(v),
			Id: aws.String(strconv.Itoa(k)),
		})
	}
	return entries
}