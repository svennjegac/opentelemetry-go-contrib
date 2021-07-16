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

package otelkafka

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "go.opentelemetry.io/contrib/instrumentation/github.com/confluentinc/confluent-kafka-go/kafka/otelkafka"

	kafkaPartitionField  = "messaging.kafka.partition"
	kafkaMessageKeyField = "messaging.kafka.message_key"
)

type config struct {
	Tracer         oteltrace.Tracer
	TracerProvider oteltrace.TracerProvider
	Propagators    propagation.TextMapPropagator
	ctx            context.Context
}

func newConfig(opts ...Option) *config {
	cfg := &config{
		Tracer:         otel.Tracer("sven.njegac/basic"),
		TracerProvider: otel.GetTracerProvider(),
		Propagators:    otel.GetTextMapPropagator(),
		ctx:            context.Background(),
	}

	return cfg
}

// Option specifies instrumentation configuration options.
type Option func(*config)

// WithPropagators specifies propagators to use for extracting If none are specified, global
// ones will be used.
func WithPropagators(propagators propagation.TextMapPropagator) Option {
	return func(cfg *config) {
		cfg.Propagators = propagators
	}
}

// WithTracerProvider specifies a tracer provider to use for creating a tracer.
// If none is specified, the global provider is used.
func WithTracerProvider(provider oteltrace.TracerProvider) Option {
	return func(cfg *config) {
		cfg.TracerProvider = provider
	}
}

// WithContext sets the config context to ctx.
func WithContext(ctx context.Context) Option {
	return func(cfg *config) {
		cfg.ctx = ctx
	}
}

// WithTracer sets the config tracer
func WithTracer(tracer oteltrace.Tracer) Option {
	return func(cfg *config) {
		cfg.Tracer = tracer
	}
}
