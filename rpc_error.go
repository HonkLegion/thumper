package thumper

import (
	"context"
	"errors"

	"github.com/HonkLegion/thumper/amqp"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// Error kinds understood by the PHP client (Thumper\Exception\ThumperException).
const (
	errKindAMQP       = "amqp"       // the broker refused the operation, code is the AMQP reply code
	errKindNack       = "nack"       // the broker nacked a published message
	errKindTimeout    = "timeout"    // the operation did not finish in time
	errKindConnection = "connection" // no usable connection to the broker
)

// Result is the reply of the RabbitMQ RPC methods. RoadRunner's RPC passes a Go error
// to the worker as text only, so RabbitMQ errors are returned in the reply.
// An empty Kind means success.
type Result struct {
	Kind   string `msgpack:"alias:kind" json:"kind"`
	Code   int    `msgpack:"alias:code" json:"code"`
	Reason string `msgpack:"alias:reason" json:"reason"`
}

// set stores a RabbitMQ error in the reply and returns nil.
// Any other error is returned unchanged.
func (r *Result) set(err error) error {
	if err == nil {
		return nil
	}

	var amqpErr *amqp091.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		*r = Result{Kind: errKindTimeout, Reason: err.Error()}
	case errors.Is(err, amqp.ErrNacked):
		*r = Result{Kind: errKindNack, Reason: err.Error()}
	// amqp091.ErrClosed is an *amqp091.Error too, so it has to be matched before the broker errors
	case errors.Is(err, amqp.ErrClosed), errors.Is(err, amqp091.ErrClosed):
		*r = Result{Kind: errKindConnection, Reason: err.Error()}
	case errors.As(err, &amqpErr):
		*r = Result{Kind: errKindAMQP, Code: amqpErr.Code, Reason: amqpErr.Reason}
	default:
		return err
	}
	return nil
}
