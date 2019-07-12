package goamqp

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Publisher struct {
	channel    *amqp.Channel
	exchange   string
	routingKey string
}

func (pub *Publisher) PublishMessage(message string) error {
	publishingMessage := amqp.Publishing{
		ContentType: "application/json",
		Body:        []byte(message),
	}

	err := pub.channel.Publish(
		pub.exchange,      // exchange
		pub.routingKey,    // routing key
		false,             // mandatory
		false,             // immediate
		publishingMessage, // message
	)

	if err != nil {
		err = errors.Wrap(err, "failed to publish message")
	}

	return err
}

func (pub *Publisher) Close() error {
	return pub.channel.Close()
}