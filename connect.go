package goamqp

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"time"
)

func Connect(host string, port int, username string, password string, vhost string) (MessageBroker, error) {
	connectionString := createConnectionString(host, port, username, password, vhost)
	connection, err := connect(connectionString)

	messageBroker := MessageBroker{
		connection: connection,
	}

	return messageBroker, err
}

func ConnectWithRetry(host string, port int, username string, password string, vhost string, minBackOff int, maxBackOff int, numRetries int) (*amqp.Connection, error) {
	connectionString := createConnectionString(host, port, username, password, vhost)
	return connectWithRetry(connectionString, minBackOff, maxBackOff, numRetries)
}

func connect(connectionString string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		err = errors.Wrap(err, "failed to connect to RabbitMq")
	}
	return conn, err
}

func createConnectionString(host string, port int, username string, password string, vhost string) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", username, password, host, port, vhost)
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
