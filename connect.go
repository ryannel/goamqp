package goamqp

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"time"
)

func Start(host string, port int, username string, password string) (MessageBroker, error) {
	connectionString := createConnectionString(host, port, username, password)

	connection, err := connectWithRetry(connectionString, 3, 8, 20)
	if err != nil {
		return MessageBroker{}, err
	}

	return MessageBroker{
		connection: connection,
	}, nil
}

func connect(connectionString string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		err = errors.Wrap(err, "failed to connect to RabbitMq")
	}
	return conn, err
}

func createConnectionString(host string, port int, username string, password string) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d", username, password, host, port)
}

func connectWithRetry(connectionString string, minBackOff int, maxBackOff int, numRetries int) (*amqp.Connection, error) {
	for i := 1; i <= numRetries; i++ {
		connection, err := connect(connectionString)
		if err == nil {
			return connection, nil
		}
		sleepTime := time.Duration(rand.Intn(maxBackOff-minBackOff) + minBackOff)
		log.Print("error connecting to rabbit, retrying in: " + fmt.Sprintf("%d", sleepTime) + " seconds")
		time.Sleep(sleepTime * time.Second)
	}
	return nil, errors.New("rabbit connection retries exceeded")
}
