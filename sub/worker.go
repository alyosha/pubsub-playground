package main

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

type sampleMsg struct {
	EventID string
}

type worker struct {
	logger       *zap.Logger
	subscription *pubsub.Subscription
}

func (w *worker) run(ctx context.Context) error {
	err := w.subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		newMsg := sampleMsg{}
		if err := json.Unmarshal(msg.Data, &newMsg); err != nil {
			w.logger.Error("failed to unmarshal message body", zap.Error(err))
			return
		}

		w.logger.Info("received new event", zap.String("event_id", newMsg.EventID), zap.Any("time", time.Now()))

		msg.Ack()
	})
	return err
}
