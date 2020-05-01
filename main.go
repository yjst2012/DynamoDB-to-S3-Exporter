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
)

func getEnv() {
	awsRegion = os.Getenv("VTV_REGION")
	awsBucket = os.Getenv("AWS_BUCKET")
	dynamoTableName = os.Getenv("AWS_TABLE")
	batchSize, _ = strconv.Atoi(os.Getenv("BATCH_SIZE"))
	awsSecretID = os.Getenv("AWS_ACCESS")
	awsSecretKey = os.Getenv("AWS_SECRET")
	awsRole = os.Getenv("AWS_ROLE")
}

type Data struct {
	UUID     string `json:"UUID"`
	Customer string `json:"Customer"`
}

func main() {
	lambda.Start(handleRequest)
	//handleRequest(context.TODO())
}

func handleRequest(ctx context.Context) error {
	getEnv()

	// aws session for dynamodb connection
	log.Println("Initialising AWS session for DynamoDB ...")
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(awsRegion),
	})
	checkErrorWithMsg("Error starting aws DynamoDB session: %v", err)

	// create local file
	filename := "/tmp/dynamo.csv"
	file, err := os.Create(filename)
	checkErrorWithMsg("failed to open local file "+filename, err)
	writer := csv.NewWriter(file)
	writer.Comma = ','
	enc := struct2csv.New()
	enc.SetSeparators("\"", "\"")

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

	// retrieve records in batch
	all := 0
	withHeader := true
	customers := make([]Data, batchSize)
	itr := table.Scan().SearchLimit(int64(batchSize)).Iter()
	for {
		i := 0
		quit := false
		for ; i < batchSize; i++ {
			more := itr.Next(&customers[i])
			if itr.Err() != nil {
				log.Println("unexpected error", itr.Err())
			}
			if !more {
				quit = true
				break
			}
			//log.Println(customers[i].Customer)
		}
		all += i
		valid := customers[:i]
		data, err := enc.Marshal(valid, withHeader)
		checkErrorWithMsg("enc marshal failed", err)
		withHeader = false
		writer.WriteAll(data)
		if quit {
			break
		}
		itr = table.Scan().StartFrom(itr.LastEvaluatedKey()).SearchLimit(int64(batchSize)).Iter()
	}
	log.Println("total records", all)
	writer.Flush()
	checkErrorWithMsg("write error", writer.Error())
	file.Close()

	// aws session for BI s3 bucket
	log.Println("Initialising AWS session for BI S3 bucket ...")
	sessBI, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsSecretID, awsSecretKey, ""),
	})
	checkErrorWithMsg("Error starting aws BI session", err)

	creds := stscreds.NewCredentials(sessBI, awsRole, func(arp *stscreds.AssumeRoleProvider) {
		arp.Duration = 60 * time.Minute
		arp.ExpiryWindow = 30 * time.Second
	})
	svc := s3.New(sessBI, aws.NewConfig().WithCredentials(creds))

	now := time.Now()
	year, month, day := now.Date()
	date := now.Format("20060102150405")
	reader, err := os.Open(filename)
	checkErrorWithMsg("Unable to open file", err)

	defer reader.Close()
	dest := fmt.Sprintf("dynamo/%04d/%02d/%02d/TT_%s.csv", year, month, day, date)
	log.Printf("using identity (%s) for uploading of (%s)", awsRole, dest)
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
	checkErrorWithMsg("failed to upload to s3://"+awsBucket+"/"+dest, err)
	fmt.Printf("\nSuccessfully uploaded DynamoDB file to s3://%s/%s\n", awsBucket, dest)
	return nil
}

func checkErrorWithMsg(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
