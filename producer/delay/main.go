package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

const (
	Topic     = "xxxxxx"
	Endpoint  = "xxxxxx"
	AccessKey = "xxxxxx"
	SecretKey = "xxxxxx"
)

func main() {
	// log to console
	os.Setenv("mq.consoleAppender.enabled", "true")
	rmq_client.ResetLogger()
	// In most case, you don't need to create many producers, singleton pattern is more recommended.
	producer, err := rmq_client.NewProducer(&rmq_client.Config{
		Endpoint: Endpoint,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
	},
		rmq_client.WithTopics(Topic),
	)
	if err != nil {
		log.Fatal(err)
	}
	// start producer
	err = producer.Start()
	if err != nil {
		log.Fatal(err)
	}
	// graceful stop producer
	defer producer.GracefulStop()
	for i := 0; i < 10; i++ {
		// new a message
		msg := &rmq_client.Message{
			Topic: Topic,
			Body:  []byte("this is a message : " + strconv.Itoa(i)),
		}
		// set keys and tag
		msg.SetKeys("a", "b")
		msg.SetTag("ab")
		// set delay timestamp
		msg.SetDelayTimestamp(time.Now().Add(time.Second * 10))
		// send message in sync
		resp, err := producer.Send(context.TODO(), msg)
		if err != nil {
			log.Fatal(err)
		}
		for i := 0; i < len(resp); i++ {
			fmt.Printf("%#v\n", resp[i])
		}
		// wait a moment
		time.Sleep(time.Second * 1)
	}
}
