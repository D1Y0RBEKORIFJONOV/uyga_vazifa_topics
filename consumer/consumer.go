package main

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"net/http"
)

func publishMessage(body string, routingKey string) {
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

	err = ch.Publish(
		"topic_logs", // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}

func createMessageHandler(w http.ResponseWriter, r *http.Request) {
	var msg struct {
		Body       string `json:"body"`
		RoutingKey string `json:"routing_key"`
	}
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	publishMessage(msg.Body, msg.RoutingKey)
	w.WriteHeader(http.StatusCreated)
}

func main() {
	http.HandleFunc("/messages", createMessageHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
