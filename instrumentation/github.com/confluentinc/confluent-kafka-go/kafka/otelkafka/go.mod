module github.com/svennjegac/opentelemetry-go-contrib/instrumentation/github.com/confluentinc/confluent-kafka-go/kafka/otelkafka

go 1.13

replace go.opentelemetry.io/contrib => ../../../../../..

require (
	github.com/confluentinc/confluent-kafka-go v1.5.2
	github.com/stretchr/testify v1.6.1
	go.opentelemetry.io/contrib v0.13.0
	go.opentelemetry.io/otel v0.13.0
)
