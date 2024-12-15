package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "55",
		"auto.offset.reset": "earliest",
	}

	// kafka消费者
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("")
	}
	defer consumer.Close()

	// 订阅topic
	topic := "lxy"
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("")
	}

	// 手动设置 offset
	partition := int32(0)
	// 指定的offset
	offset := kafka.Offset(10)
	err = consumer.Assign([]kafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: partition,
			Offset:    offset,
		},
	})
	if err != nil {
		log.Fatalf("")
	}

	// 消费消息
	for {
		_, err := consumer.ReadMessage(-1)
		if err != nil {

		}
	}
}
