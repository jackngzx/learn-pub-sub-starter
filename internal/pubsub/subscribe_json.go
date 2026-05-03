package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {

	c, err := conn.Channel()
	if err != nil {
		return err
	}

	_, _, err = DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryCh, err := c.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range deliveryCh {
			var t T
			if err := json.Unmarshal(msg.Body, &t); err != nil {
				log.Fatalf("error unmarshalling channel body JSON: %v", err)
			}
			a := handler(t)
			switch a {
			case Ack:
				msg.Ack(false)
				fmt.Println("ack")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("nackrequeue")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("nackdiscard")
			}
		}
	}()

	return nil
}
