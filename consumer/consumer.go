package main

import (
	"context"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

func handleMessage(msg amqp.Delivery) {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.Background())

	collection := client.Database("testdb").Collection("messages")
	_, err = collection.InsertOne(context.Background(), bson.M{"body": string(msg.Body), "routing_key": msg.RoutingKey})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Received a message: %s", msg.Body)
}

func consumeMessages(routingKey string) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	err = ch.QueueBind(
		q.Name,       // queue name
		routingKey,   // routing key
		"topic_logs", // exchange
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatal(err)
	}

	for msg := range msgs {
		handleMessage(msg)
	}
}

func main() {
	go consumeMessages("report.*")
	go consumeMessages("*.updated")
	go consumeMessages("report.#")
	select {}
}
