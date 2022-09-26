## Go SQS Publisher 

It`s a simple command line to automate the process of sending batch messages from a file to an SQS queue.
Each message within the file must be separated by line breaks.


### Requirements:
AWS credentials properly configured and with permission to send messages to the specified queue.

### Installation:
```go build -o $GOPATH/bin/go-sqs-publisher github.com/acassio/go-sqs-publisher/cmd```

### Usage:
```go-sqs-publisher -file my-text-file.txt -queue my-sqs-queue-url```