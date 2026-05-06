package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(mv gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		move := gs.HandleMove(mv)
		switch move {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			key := routing.WarRecognitionsPrefix + "." + gs.GetUsername()
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, key, gamelogic.RecognitionOfWar{
				Attacker: mv.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				fmt.Println(err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		}
		fmt.Println("error: move outcome not found")
		return pubsub.NackDiscard
	}
}

func publishGameLog(gs *gamelogic.GameState, ch *amqp.Channel, msg string) error {
	usr := gs.GetUsername()
	routingKey := routing.GameLogSlug + "." + usr
	return pubsub.PublishGob(ch, routing.ExchangePerilTopic, routingKey, routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    usr,
	})
}
func handlerWarConsume(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(war gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(war)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			message := fmt.Sprintf("%s won a war against %s\n", winner, loser)
			err := publishGameLog(gs, ch, message)
			if err != nil {
				fmt.Println(err)
				return pubsub.NackDiscard
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			message := fmt.Sprintf("%s won a war against %s\n", winner, loser)
			err := publishGameLog(gs, ch, message)
			if err != nil {
				fmt.Println(err)
				return pubsub.NackDiscard
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			message := fmt.Sprintf("A war between %s and %s resulted in a draw\n", winner, loser)
			err := publishGameLog(gs, ch, message)
			if err != nil {
				fmt.Println(err)
				return pubsub.NackDiscard
			}
			return pubsub.Ack
		default:
			fmt.Println(outcome)
			return pubsub.NackDiscard
		}
	}
}
