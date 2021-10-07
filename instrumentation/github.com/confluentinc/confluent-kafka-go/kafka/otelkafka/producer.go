// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package kafka provides functions to trace the confluentinc/confluent-kafka-go package (https://github.com/confluentinc/confluent-kafka-go).

package otelkafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	oteltrace "go.opentelemetry.io/otel/trace"
)

// Producer wraps a kafka.Producer.
type Producer struct {
	ctx                context.Context
	doneCtx            context.Context
	doneCtxCancel      context.CancelFunc
	confluentProducer  *kafka.Producer
	otelProduceChannel chan *kafka.Message

	tracer      oteltrace.Tracer
	propagators propagation.TextMapPropagator
}

// WrapProducer wraps a kafka.Producer so requests are traced.
func WrapProducer(ctx context.Context, confluentProducer *kafka.Producer) *Producer {
	doneCtx, doneCtxCancel := context.WithCancel(context.Background())
	otelProducer := &Producer{
		ctx:                ctx,
		doneCtx:            doneCtx,
		doneCtxCancel:      doneCtxCancel,
		confluentProducer:  confluentProducer,
		otelProduceChannel: make(chan *kafka.Message),

		tracer:      otel.Tracer("github.com/svennjegac/opentelemetry-go-contrib/instrumentation/github.com/confluentinc/confluent-kafka-go/kafka/otelkafka"),
		propagators: otel.GetTextMapPropagator(),
	}
	otelProducer.traceProduceChannel()
	return otelProducer
}

// ProduceChannel returns a channel which can receive kafka Messages and will
// send them to the underlying producer channel.
func (p *Producer) ProduceChannel() chan *kafka.Message {
	return p.otelProduceChannel
}

func (p *Producer) traceProduceChannel() {
	go func() {
		defer p.doneCtxCancel()

		for msg := range p.otelProduceChannel {
			span := p.startSpan(msg)
			select {
			case p.confluentProducer.ProduceChannel() <- msg:
				span.AddEvent(
					"confluent-produce-channel-enqueue",
					trace.WithAttributes(
						attribute.String("kafka-msg-key", string(msg.Key)),
					),
				)
				span.End()

			case <-p.ctx.Done():
				span.AddEvent(
					"confluent-produce-channel-skip",
					trace.WithAttributes(
						attribute.String("kafka-msg-key", string(msg.Key)),
					),
				)
				span.RecordError(
					errors.New("confluent produce channel skip"),
					trace.WithAttributes(
						attribute.String("kafka-msg-key", string(msg.Key)),
					),
				)
				span.End()
				return
			}
		}
	}()
}

// Close closes internal produce channel.
// Use case:
// - User is writing to otel producer ProduceChannel()
// - User stops writing to otel producer ProduceChannel()
// - Now Close() can be safely called
func (p *Producer) Close() {
	close(p.otelProduceChannel)
}

// WaitTeardown ensures that otel producer stopped writing to underlying confluent kafka produce channel.
func (p *Producer) WaitTeardown() {
	<-p.doneCtx.Done()
}

func (p *Producer) startSpan(msg *kafka.Message) oteltrace.Span {
	// If there's a span context in the message, use that as the parent context.
	carrier := NewMessageCarrier(msg)
	ctx := p.propagators.Extract(context.Background(), carrier)
	ctx, span := p.tracer.Start(ctx, fmt.Sprintf("%s-produce", *msg.TopicPartition.Topic))

	// Inject the span context so consumers can pick it up
	carrier = NewMessageCarrier(msg)
	p.propagators.Inject(ctx, carrier)
	return span
}

// Produce calls the underlying Producer.Produce and traces the request.
func (p *Producer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	span := p.startSpan(msg)

	var otelDeliveryChan chan kafka.Event
	if deliveryChan != nil {
		otelDeliveryChan = make(chan kafka.Event, 1)
	}

	err := p.confluentProducer.Produce(msg, otelDeliveryChan)
	// message is not enqueued in librdkafka, method should return immediately
	if err != nil {
		span.RecordError(
			errors.Wrap(err, "confluent produce cannot enqueue message"),
			trace.WithAttributes(
				attribute.String("kafka-msg-key", string(msg.Key)),
			),
		)
		span.End()
		return err
	}

	// message is successfully enqueued in librdkafka, ack will be available on the events channel
	// method should return immediately
	if otelDeliveryChan == nil {
		span.AddEvent(
			"confluent-produce-enqueue",
			trace.WithAttributes(
				attribute.String("kafka-msg-key", string(msg.Key)),
			),
		)
		span.End()
		return nil
	}

	// message is successfully enqueued in librdkafka, ack will be available on the otel delivery chan
	// start a goroutine which will pass ack to the actual delivery channel
	// both otel and actual delivery channels have buffer of 1, so this goroutine will for sure be garbage collected
	// (if user is not trying to read from actual delivery channel, this goroutine is still able to write
	// to delivery channel and exit)
	go func() {
		event := <-otelDeliveryChan
		span.AddEvent(
			"confluent-produce-delivery-acknowledgement",
			trace.WithAttributes(
				attribute.String("kafka-msg-key", string(msg.Key)),
			),
		)

		if ack, ok := event.(*kafka.Message); ok {
			if ack.TopicPartition.Error != nil {
				span.RecordError(
					ack.TopicPartition.Error,
					trace.WithAttributes(
						attribute.String("kafka-msg-key", string(ack.Key)),
					),
				)
			}
		}

		span.End()
		deliveryChan <- event
	}()

	return nil
}
