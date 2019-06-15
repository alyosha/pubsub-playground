package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
)

const (
	exitOK = iota
	exitError
)

type config struct {
	ProjectID string `envconfig:"PROJECT_ID" required:"true"`
	TopicID   string `envconfig:"TOPIC_ID" required:"true"`
	PubCount  int    `envconfig:"PUB_COUNT" required:"true"`
}

type sampleMsg struct {
	EventID string
}

func main() {
	os.Exit(_main())
}

func _main() int {
	var env config
	if err := envconfig.Process("", &env); err != nil {
		fmt.Fprintf(os.Stderr, "failed to process envconfig: %s\n", err)
		return exitError
	}

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to initalize zap logger: %s", err)
	}
	defer logger.Sync()

	client, err := pubsub.NewClient(context.Background(), env.ProjectID)
	if err != nil {
		logger.Error("failed to create new pubsub client", zap.Error(err))
		return exitError
	}

	topic := client.Topic(env.TopicID)

	var wg sync.WaitGroup
	for i := 0; i < env.PubCount; i++ {
		wg.Add(1)
		go func() {
			evtID := uuid.New().String()
			msg := sampleMsg{
				EventID: evtID,
			}

			data, err := json.Marshal(msg)
			if err != nil {
				logger.Error("failed to marshal the message",
					zap.String("event_id", evtID),
					zap.Error(err))
				wg.Done()
				return
			}

			rs := topic.Publish(context.Background(), &pubsub.Message{Data: data})
			if _, err := rs.Get(context.Background()); err != nil {
				logger.Error("failed to publish the message",
					zap.String("body", string(data)),
					zap.Error(err))
			}

			wg.Done()
			return
		}()
	}

	wg.Wait()

	return exitOK
}
