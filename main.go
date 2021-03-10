package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/guregu/dynamo"
	"github.com/yjst2012/struct2csv"
)

var (
	awsRegion       string = ""
	awsBucket       string = ""
	dynamoTableName string = ""
	awsSecretID     string = ""
	awsSecretKey    string = ""
	awsRole         string = ""
	batchSize       int    = 0
	filename        string = "/tmp/dynamo.csv"
)

// Data is the basic structure
type Data struct {
	UUID     string `json:"UUID"`
	Customer string `json:"Customer"`
}

func main() {
	lambda.Start(handleRequest)
}

func handleRequest(ctx context.Context) error {
	getEnv()

	// create local file
	file, err := os.Create(filename)
	logErrorWithMsg("failed to open local file "+filename, err)
	defer file.Close()

	// fetch all records from dynamodb
	records, err := fetchDyanmoRecords()
	logErrorWithMsg("failed to fetch dyanmodb", err)

	// set up csv writter
	writer := csv.NewWriter(file)
	writer.Comma = ','
	encoder := struct2csv.New()
	encoder.SetSeparators("\"", "\"")
	data, err := encoder.Marshal(records, true)
	logErrorWithMsg("failed to encode", err)

	// write to local file
	writer.WriteAll(data)
	logErrorWithMsg("write error", writer.Error())
	writer.Flush()
	log.Printf("total records written %d to local file", len(records))

	// upload to s3 bucket
	destPath := genDestFileName()
	upload2S3(filename, destPath)

	fmt.Printf("\nSuccessfully uploaded DynamoDB file to s3://%s/%s\n", awsBucket, destPath)
	return nil
}

func getEnv() {
	awsRegion = os.Getenv("VT_REGION")
	awsBucket = os.Getenv("AWS_BUCKET")
	dynamoTableName = os.Getenv("AWS_TABLE")
	batchSize, _ = strconv.Atoi(os.Getenv("BATCH_SIZE"))
	awsSecretID = os.Getenv("AWS_ACCESS")
	awsSecretKey = os.Getenv("AWS_SECRET")
	awsRole = os.Getenv("AWS_ROLE")
}

func initDynamoTableConnection() *dynamo.Table {
	// aws session for dynamodb connection
	log.Println("Initialising AWS session for DynamoDB ...")
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(awsRegion),
	})
	logErrorWithMsg("Error starting aws DynamoDB session: %v", err)
	// read dynamodb
	db := dynamo.New(sess, &aws.Config{})
	if db == nil {
		log.Panic("error creating dynamo db client!")
	}
	if dynamoTableName == "" {
		log.Panic("empty DynamoDB table name!")
	}
	table := db.Table(dynamoTableName)
	log.Println("dynamodb connection initialised", dynamoTableName)
	return &table
}

func fetchDyanmoRecords() ([]Data, error) {
	table := initDynamoTableConnection()
	records := []Data{}
	if err := table.Scan().All(&records); err != nil {
		return records, err
	}
	return records, nil
}

func upload2S3(src, dest string) {
	// set up different aws session for new s3 bucket
	log.Println("Initialising AWS session for new S3 bucket ...")
	sessBI, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsSecretID, awsSecretKey, ""),
	})
	logErrorWithMsg("Error starting aws new session", err)
	creds := stscreds.NewCredentials(sessBI, awsRole, func(arp *stscreds.AssumeRoleProvider) {
		arp.Duration = 60 * time.Minute
		arp.ExpiryWindow = 30 * time.Second
	})
	svc := s3.New(sessBI, aws.NewConfig().WithCredentials(creds))

	reader, err := os.Open(filename)
	logErrorWithMsg("Unable to open local file", err)
	defer reader.Close()

	log.Printf("using identity (%s) for s3 uploading for (%s)", awsRole, dest)
	uploader := s3manager.NewUploaderWithClient(svc)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(awsBucket),
		Key:    aws.String(dest),
		ACL:    aws.String("private"),
		Body:   reader,
		// ContentType:          aws.String("text/csv"),
		// ContentDisposition:   aws.String("attachment"),
		// ServerSideEncryption: aws.String("AES256"),
	})
	logErrorWithMsg("failed to upload to s3://"+awsBucket+"/"+dest, err)
	return
}

func genDestFileName() string {
	now := time.Now()
	year, month, day := now.Date()
	date := now.Format("20060102150405")
	dest := fmt.Sprintf("dynamo/%04d/%02d/%02d/TT_%s.csv", year, month, day, date)
	return dest
}

func logErrorWithMsg(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
