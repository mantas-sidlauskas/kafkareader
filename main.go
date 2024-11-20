package main

import (
	"context"
	"flag"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
	"github.com/uber/cadence/.gen/go/indexer"
	"github.com/uber/cadence/common/codec"
	"os"
)

func main() {
	topic := flag.String("topic", "", "topic to read")
	broker := flag.String("broker", "", "broker to connect to")
	flag.Parse()

	if *topic == "" || *broker == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	fmt.Println("Reading ", *topic, " from ", *broker)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{*broker},
		Topic:    *topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	c := codec.NewThriftRWEncoder()

	for {
		var val indexer.Message
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("Error: ", err)
		}
		c.Decode(m.Value, &val)

		fmt.Println(val.String())
	}
}
