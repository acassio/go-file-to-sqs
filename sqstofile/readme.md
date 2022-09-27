## Go SQS to File

It`s a Simple command line to consume messages from an SQS queue and save them to a file.


### Requirements:
AWS credentials properly configured and with permission to receive messages to the specified queue.

### Installation:
```go install github.com/acassio/go-tools/sqstofile@latest```

### Usage:
```sqstofile -file my-text-file.txt -queue my-sqs-queue-url```

|parameter|type|required|default|description
|---------|----|--------|-------------|----------
|file|string|yes||file where the messages will be saved in. |
|queue|string|yes||URL of the queue from which messages will be received. |
|max-concurrency|integer|no|10|Number of workers used to process the messages.|
