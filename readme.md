## Go File to SQS

It`s a simple command line to automate the process of sending batch messages from a file to an SQS queue.
Each message within the file must be separated by line breaks.


### Requirements:
AWS credentials properly configured and with permission to send messages to the specified queue.

### Installation:
```go build -o $GOPATH/bin/go-filetosqs github.com/acassio/go-file-to-sqs/cmd```

### Usage:
```go-filetosqs -file my-text-file.txt -queue my-sqs-queue-url```

|parameter|type|required|default|description
|---------|----|--------|-------------|----------
|file|string|yes||file that contains the messages. |
|queue|string|yes||URL of the queue to which messages will be sent.  |
|raw-content|boolean|no|false|Describe whether messages within the file should be read as raw content as opposed to format: {"Id":"...","MessageBody":"..."}.|
