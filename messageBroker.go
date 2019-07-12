package goamqp

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type MessageBroker struct {
	connection *amqp.Connection
}

func (mq *MessageBroker) Close() {
	_ = mq.connection.Close()
}

func (mq *MessageBroker) CreatePublisher(exchange string, routingKey string, queue string) (Publisher, error) {
	channel, err := mq.createChannel()
	if err != nil {
		return Publisher{}, err
	}

	err = mq.createExchange(exchange, channel)
	if err != nil {
		return Publisher{}, err
	}

	err = mq.createQueue(queue, channel)
	if err != nil {
		return Publisher{}, err
	}

	err = mq.bindQueue(queue, routingKey, exchange, channel)
	if err != nil {
		return Publisher{}, err
	}

	publisher := Publisher{
		channel:    channel,
		exchange:   exchange,
		routingKey: routingKey,
	}

	return publisher, nil
}

func (mq *MessageBroker) createChannel() (*amqp.Channel, error) {
	channel, err := mq.connection.Channel()
	if err != nil {
		err = errors.Wrap(err, "failed to create channel")
	}
	return channel, err
}

func (mq *MessageBroker) createExchange(exchange string, channel *amqp.Channel) error {
	err := channel.ExchangeDeclare(
		exchange, // exchange
		"direct", // kind
		true,     // durable
		false,    // autoDelete
		false,    // internal
		false,    //no wait
		nil,      // args
	)
	if err != nil {
		err = errors.Wrap(err, "failed to create exchange")
	}

	return err
}

func (mq *MessageBroker) createQueue(name string, channel *amqp.Channel) error {
	_, err := channel.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		err = errors.Wrap(err, "error declaring queue")
	}
	return err
}

func (mq *MessageBroker) bindQueue(queue string, routingKey string, exchange string, channel *amqp.Channel) error {
	return channel.QueueBind(
		queue,
		routingKey,
		exchange,
		false,
		nil,
	)
}

func (mq *MessageBroker) CreateConsumer(queue string) (Consumer, error) {
	channel, err := mq.createChannel()
	if err != nil {
		return Consumer{}, err
	}

	consumer := Consumer{
		channel: channel,
		queue:   queue,
	}

	return consumer, nil
}
