package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	c := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(c)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	fmt.Println("Connection is successful")

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
		return
	}

	qName := "game_logs"
	routingKey := routing.GameLogSlug + ".*"

	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, qName, routingKey, pubsub.SimpleQueueType(pubsub.Durable))
	if err != nil {
		fmt.Println(err)
		return
	}

	err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	gamelogic.PrintServerHelp()

Loop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "pause":
			fmt.Println("Sending a pause message")
			if err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			}); err != nil {
				fmt.Println(err)
				return
			}
		case "resume":
			fmt.Println("Sending a resume message")
			if err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			}); err != nil {
				fmt.Println(err)
				return
			}
		case "quit":
			fmt.Println("exiting program")
			break Loop
		default:
			fmt.Println("command unknown, please try again")
		}
	}
}
