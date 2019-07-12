package goamqp

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"log"
)

type Consumer struct {
	channel *amqp.Channel
	queue   string
}

func (con *Consumer) Consume(callback func(message string) bool) error {
	messages, err := con.channel.Consume(
		con.queue, // queue
		"",        // consumer (empty auto generates a consumer key)
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return errors.Wrap(err, "Failed to register a consumer")
	}

	go func() {
		for message := range messages {
			stringMessage := string(message.Body)
			result := callback(stringMessage)

			if result {
				err := con.channel.Ack(message.DeliveryTag, false)
				if err != nil {
					// TODO: What happens when message is successfully processed but can't be acked.
					log.Println("Unable to ACK message")
				}
			} else {
				err := con.channel.Nack(message.DeliveryTag, false, true)
				if err != nil {
					// TODO: What happens when message is successfully processed but can't be acked.
					log.Println("Unable to NACK message")
				}
			}
		}
	}()

	return nil
}

func (con *Consumer) Close(queue string) error {
	return con.channel.Close()
}
