package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
)

const (
	exitOK = iota
	exitError
)

type config struct {
	ProjectID              string        `envconfig:"PROJECT_ID" required:"true"`
	SubscriptionID         string        `envconfig:"SUBSCRIPTION_ID" required:"true"`
	MaxExtension           time.Duration `envconfig:"MAX_EXTENSION" default:"60s"`
	MaxOutstandingMessages int           `envconfig:"MAX_OUTSTANDING_MESSAGES" default:"-1"`
	MaxOutstandingBytes    int           `envconfig:"MAX_OUTSTANDING_BYTES" default:"-1"`
	NumGoroutines          int           `envconfig:"NUM_GOROUTINES" default:"-1"`
}

func main() {
	os.Exit(_main())
}

func _main() int {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to initalize zap logger: %s", err)
	}
	defer logger.Sync()

	logger.Info("starting up")

	var env config
	if err := envconfig.Process("", &env); err != nil {
		logger.Error("failed to process environment variables", zap.Error(err))
		return exitError
	}

	pubsubClient, err := pubsub.NewClient(context.Background(), env.ProjectID)
	if err != nil {
		logger.Error("failed to initialize pubsub client", zap.Error(err))
		return exitError
	}

	subscription := pubsubClient.Subscription(env.SubscriptionID)
	ok, err := subscription.Exists(context.Background())
	if err != nil {
		logger.Error("failed to check existence of subscription")
		return exitError
	}

	if !ok {
		logger.Error("subscription doesn't exist")
		return exitError
	}

	subscription.ReceiveSettings.MaxExtension = env.MaxExtension
	subscription.ReceiveSettings.MaxOutstandingMessages = env.MaxOutstandingMessages
	subscription.ReceiveSettings.MaxOutstandingBytes = env.MaxOutstandingBytes
	subscription.ReceiveSettings.NumGoroutines = env.NumGoroutines

	logger.Info(
		"will use the following subscription config",
		zap.Any("MaxExtension", env.MaxExtension),
		zap.Int("MaxOutstandingMessages", env.MaxOutstandingMessages),
		zap.Int("MaxOutstandingBytes", subscription.ReceiveSettings.MaxOutstandingBytes),
		zap.Int("NumGoroutines", subscription.ReceiveSettings.NumGoroutines),
	)

	w := worker{
		logger:       logger,
		subscription: subscription,
	}

	doneCh := make(chan struct{}, 1)
	go func() {
		if err := w.run(context.Background()); err != nil {
			logger.Error("error returned from worker", zap.Error(err))
		}
		doneCh <- struct{}{}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, os.Kill)

	select {
	case <-doneCh:
	case <-sigCh:
	}

	return exitOK
}
