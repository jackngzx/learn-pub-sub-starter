package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

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
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			err := json.Unmarshal(data, &target)
			return target, err
		},
	)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {

	c, err := conn.Channel()
	if err != nil {
		return err
	}

	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	err = c.Qos(20, 0, true)
	if err != nil {
		return err
	}

	msgs, err := c.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		defer ch.Close()
		for msg := range msgs {
			t, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("error unmarshalling message: %v\n", err)
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

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	return subscribe[T](conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		buf := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(buf)
		var target T
		err := decoder.Decode(&target)
		return target, err
	},
	)
}
