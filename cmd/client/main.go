package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

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
	usrName, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println(err)
		return
	}

	gS := gamelogic.NewGameState(usrName)

	// pause
	pauseq := routing.PauseKey + "." + usrName
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, pauseq, routing.PauseKey, pubsub.Transient, handlerPause(gS))
	if err != nil {
		fmt.Println(err)
		return
	}

	// move
	movekey := routing.ArmyMovesPrefix + ".*"
	moveq := routing.ArmyMovesPrefix + "." + usrName
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, moveq, movekey, pubsub.Transient, handlerMove(gS, ch))
	if err != nil {
		fmt.Println(err)
		return
	}

	// war
	warkey := routing.WarRecognitionsPrefix + "." + usrName
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", warkey, pubsub.Durable, handlerWarConsume(gS, ch))
	if err != nil {
		fmt.Println(err)
		return
	}

Loop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":
			err := gS.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := gS.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("Move is successful!")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, movekey, move)
			if err != nil {
				fmt.Println(err)
				return
			}
		case "status":
			gS.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) == 1 {
				fmt.Println("second word needed")
				continue
			}
			i, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Println(err)
				continue
			}
			for range i {
				msg := gamelogic.GetMaliciousLog()
				err = pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+gS.Player.Username, msg)
				if err != nil {
					fmt.Println(err)
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			os.Exit(1)
			break Loop
		default:
			fmt.Println("command not found")
			continue
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Program is shutting down, closing connection")
}
