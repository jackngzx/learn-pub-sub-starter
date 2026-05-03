package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("error creating a new channel on connection")
		return nil, amqp.Queue{}, err
	}
	var d, autoDelete, exclusive bool

	switch queueType {
	case Durable:
		d = true
		autoDelete = false
		exclusive = false
	default:
		d = false
		autoDelete = true
		exclusive = true
	}

	q, _ := ch.QueueDeclare(queueName, d, autoDelete, exclusive, false, amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})

	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		fmt.Println(err)
		return nil, amqp.Queue{}, err
	}
	return ch, q, nil
}
