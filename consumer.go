/*
Copyright © 2022 Red Hat, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"strings"
	"time"

	tlsutils "github.com/RedHatInsights/insights-operator-utils/tls"
	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

const (
	// key for topic name used in structured log messages
	topicKey = "topic"

	// key for broker group name used in structured log messages
	groupKey = "group"

	// key for message offset used in structured log messages
	offsetKey = "offset"

	// key for message partition used in structured log messages
	partitionKey = "partition"
)

// Consumer represents any consumer of insights-rules messages
type Consumer interface {
	Serve()
	Close() error
	ProcessMessage(msg *sarama.ConsumerMessage) error
}

// KafkaConsumer in an implementation of Consumer interface
// Example:
//
// kafkaConsumer, err := consumer.New(brokerCfg, storage)
// if err != nil {
//     panic(err)
// }
//
// kafkaConsumer.Serve()
//
// err := kafkaConsumer.Stop()
// if err != nil {
//     panic(err)
// }
type KafkaConsumer struct {
	Configuration                        BrokerConfiguration
	ConsumerGroup                        sarama.ConsumerGroup
	numberOfSuccessfullyConsumedMessages uint64
	numberOfErrorsConsumingMessages      uint64
	Verbose                              bool
	Ready                                chan bool
	Cancel                               context.CancelFunc
}

// DefaultSaramaConfig is a config which will be used by default
// here you can use specific version of a protocol for example
// useful for testing
var DefaultSaramaConfig *sarama.Config

// NewConsumer constructs new implementation of Consumer interface
func NewConsumer(brokerCfg BrokerConfiguration, verbose bool) (*KafkaConsumer, error) {
	return NewWithSaramaConfig(brokerCfg, DefaultSaramaConfig, verbose)
}

// NewWithSaramaConfig constructs new implementation of Consumer interface with custom sarama config
func NewWithSaramaConfig(
	brokerCfg BrokerConfiguration,
	saramaConfig *sarama.Config,
	verbose bool,
) (*KafkaConsumer, error) {
	var err error
	if saramaConfig == nil {
		saramaConfig, err = saramaConfigFromBrokerConfig(brokerCfg)
		if err != nil {
			log.Error().Err(err).Msg("unable to create sarama configuration from current broker configuration")
			return nil, err
		}
	}

	log.Info().
		Str("addr", brokerCfg.Address).
		Str("group", brokerCfg.Group).
		Msg("Configuration")

	consumerGroup, err := sarama.NewConsumerGroup([]string{brokerCfg.Address}, brokerCfg.Group, saramaConfig)
	if err != nil {
		return nil, err
	}

	consumer := &KafkaConsumer{
		Configuration:                        brokerCfg,
		ConsumerGroup:                        consumerGroup,
		Verbose:                              verbose,
		numberOfSuccessfullyConsumedMessages: 0,
		numberOfErrorsConsumingMessages:      0,
		Ready:                                make(chan bool),
	}

	return consumer, nil
}

// Serve starts listening for messages and processing them. It blocks current thread.
func (consumer *KafkaConsumer) Serve() {
	ctx, cancel := context.WithCancel(context.Background())
	consumer.Cancel = cancel

	go func() {
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := consumer.ConsumerGroup.Consume(ctx, []string{consumer.Configuration.Topic}, consumer); err != nil {
				log.Fatal().Err(err).Msg("Unable to recreate Kafka session")
			}

			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				log.Info().Err(ctx.Err()).Msg("Stopping consumer")
				return
			}

			log.Info().Msg("Created new kafka session")

			consumer.Ready = make(chan bool)
		}
	}()

	// Await till the consumer has been set up
	log.Info().Msg("Waiting for consumer to become ready")
	<-consumer.Ready
	log.Info().Msg("Finished waiting for consumer to become ready")

	// Actual processing is done in goroutine created by sarama (see ConsumeClaim below)
	log.Info().Msg("Started serving consumer")
	<-ctx.Done()
	log.Info().Msg("Context cancelled, exiting")

	cancel()
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	log.Info().Msg("New session has been setup")
	// Mark the consumer as ready
	close(consumer.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Info().Msg("New session has been finished")
	return nil
}

// ConsumeClaim starts a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Info().
		Int64(offsetKey, claim.InitialOffset()).
		Msg("Starting messages loop")

	for message := range claim.Messages() {
		// not needed ATM, to be loged in consumer.HandleMessage
		// log.Info().Int64(offsetKey, message.Offset).Msg("Message retrieved")

		consumer.HandleMessage(message)

		session.MarkMessage(message, "")
	}

	return nil
}

// Close method closes all resources used by consumer
func (consumer *KafkaConsumer) Close() error {
	if consumer.Cancel != nil {
		consumer.Cancel()
	}

	if consumer.ConsumerGroup != nil {
		if err := consumer.ConsumerGroup.Close(); err != nil {
			log.Error().
				Err(err).
				Msg("Unable to close consumer group")
		}
	}

	return nil
}

// GetNumberOfSuccessfullyConsumedMessages returns number of consumed messages
// since creating KafkaConsumer obj
func (consumer *KafkaConsumer) GetNumberOfSuccessfullyConsumedMessages() uint64 {
	return consumer.numberOfSuccessfullyConsumedMessages
}

// GetNumberOfErrorsConsumingMessages returns number of errors during consuming messages
// since creating KafkaConsumer obj
func (consumer *KafkaConsumer) GetNumberOfErrorsConsumingMessages() uint64 {
	return consumer.numberOfErrorsConsumingMessages
}

// HandleMessage handles the message and does all logging, metrics, etc
func (consumer *KafkaConsumer) HandleMessage(msg *sarama.ConsumerMessage) {
	if msg == nil {
		log.Error().Msg("nil message")
		return
	}

	log.Info().
		Int64(offsetKey, msg.Offset).
		Int32(partitionKey, msg.Partition).
		Str(topicKey, msg.Topic).
		Time("message_timestamp", msg.Timestamp).
		Msg("Started processing message")

	startTime := time.Now()
	err := consumer.ProcessMessage(msg)
	timeAfterProcessingMessage := time.Now()
	messageProcessingDuration := timeAfterProcessingMessage.Sub(startTime).Seconds()

	// Something went wrong while processing the message.
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error processing message consumed from Kafka")
		consumer.numberOfErrorsConsumingMessages++
	} else {
		// The message was processed successfully.
		consumer.numberOfSuccessfullyConsumedMessages++
	}

	log.Info().
		Str(topicKey, consumer.Configuration.Topic).
		Str(groupKey, consumer.Configuration.Group).
		Int64(offsetKey, msg.Offset).
		Int32(partitionKey, msg.Partition).
		Str(topicKey, msg.Topic).
		Uint64("consumed messages", consumer.numberOfSuccessfullyConsumedMessages).
		Uint64("errors", consumer.numberOfErrorsConsumingMessages).
		Msgf("Processing of message took '%v' seconds", messageProcessingDuration)
}

// ProcessMessage processes an incoming message
func (consumer *KafkaConsumer) ProcessMessage(msg *sarama.ConsumerMessage) error {
	value := msg.Value

	log.Info().Int("length", len(value)).Msg("Message length")

	if consumer.Verbose {
		log.Info().Str("content", string(value)).Msg("Message value")
	}

	return nil
}

func saramaConfigFromBrokerConfig(cfg BrokerConfiguration) (*sarama.Config, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V0_10_2_0

	/* TODO: we need to do it in production code
	if brokerCfg.Timeout > 0 {
		saramaConfig.Net.DialTimeout = brokerCfg.Timeout
		saramaConfig.Net.ReadTimeout = brokerCfg.Timeout
		saramaConfig.Net.WriteTimeout = brokerCfg.Timeout
	}
	*/
	if strings.Contains(cfg.SecurityProtocol, "SSL") {
		saramaConfig.Net.TLS.Enable = true
	}
	if cfg.CertPath != "" {
		tlsConfig, err := tlsutils.NewTLSConfig(cfg.CertPath)
		if err != nil {
			log.Error().Msgf("Unable to load TLS config for %s cert", cfg.CertPath)
			return nil, err
		}
		saramaConfig.Net.TLS.Config = tlsConfig
	}
	if strings.HasPrefix(cfg.SecurityProtocol, "SASL_") {
		log.Info().Msg("Configuring SASL authentication")
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cfg.SaslUsername
		saramaConfig.Net.SASL.Password = cfg.SaslPassword
		saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(cfg.SaslMechanism)
	}
	return saramaConfig, nil
}
