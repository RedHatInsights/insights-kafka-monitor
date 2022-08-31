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
	"flag"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Shopify/sarama"
)

// Messages
const (
	versionMessage                 = "Insights Kafka monitor version 1.0"
	authorsMessage                 = "Pavel Tisnovsky, Red Hat Inc."
	connectionToBrokerMessage      = "Connection to broker"
	notConnectedToBrokerMessage    = "Not connected to broker"
	brokerConnectionSuccessMessage = "Broker connection OK"
	brokerAddressMessage           = "Broker address"
	brokerConfigurationMessage     = "Broker configuration"
	securityProtocol               = "Security protocol"
	certPath                       = "SSL cert path"
	saslMechanism                  = "SASL Mechanism"
	saslUsername                   = "SASL username"
	topic                          = "Topic"
	group                          = "Group"
	enabled                        = "Enabled"
	verbose                        = "Verbose"
)

// Configuration-related constants
const (
	configFileEnvVariableName = "INSIGHTS_KAFKA_MONITOR_CONFIG_FILE"
	defaultConfigFileName     = "config"
)

// Exit codes
const (
	// ExitStatusOK means that the tool finished with success
	ExitStatusOK = iota
	// ExitStatusError is a general error code
	ExitStatusError
	// ExitStatusConsumerError is returned in case of any consumer-related error
	ExitStatusConsumerError
	// ExitStatusKafkaError is returned in case of any Kafka-related error
	ExitStatusKafkaError
)

// showVersion function displays version information.
func showVersion() {
	fmt.Println(versionMessage)
}

// showAuthors function displays information about authors.
func showAuthors() {
	fmt.Println(authorsMessage)
}

// showConfiguration function displays actual configuration.
func showConfiguration(config ConfigStruct) {
	brokerConfig := GetBrokerConfiguration(&config)
	log.Info().
		Str(brokerAddressMessage, brokerConfig.Address).
		Str(securityProtocol, brokerConfig.SecurityProtocol).
		Str(certPath, brokerConfig.CertPath).
		Str(saslMechanism, brokerConfig.SaslMechanism).
		Str(saslUsername, brokerConfig.SaslUsername).
		Str(topic, brokerConfig.Topic).
		Str(group, brokerConfig.Group).
		Bool(enabled, brokerConfig.Enabled).
		Msg(brokerConfigurationMessage)

	loggingConfig := GetLoggingConfiguration(&config)
	log.Info().
		Str("Level", loggingConfig.LogLevel).
		Bool("Pretty colored debug logging", loggingConfig.Debug).
		Msg("Logging configuration")

	outputConfig := GetOutputConfiguration(&config)
	log.Info().
		Bool(verbose, outputConfig.Verbose).
		Msg("Output configuration")
}

// tryToConnectToKafka function just tries connection to Kafka broker
func tryToConnectToKafka(config ConfigStruct) (int, error) {
	log.Info().Msg("Checking connection to Kafka")

	// prepare broker configuration
	brokerConfiguration := GetBrokerConfiguration(&config)

	log.Info().Str(brokerAddressMessage, brokerConfiguration.Address).Msg(brokerAddressMessage)

	// create new broker instance (w/o any checks)
	broker := sarama.NewBroker(brokerConfiguration.Address)

	// check broker connection
	err := broker.Open(nil)
	if err != nil {
		log.Error().Err(err).Msg(connectionToBrokerMessage)
		return ExitStatusKafkaError, err
	}

	// check if connection remain
	connected, err := broker.Connected()
	if err != nil {
		log.Error().Err(err).Msg(connectionToBrokerMessage)
		return ExitStatusKafkaError, err
	}
	if !connected {
		log.Error().Err(err).Msg(notConnectedToBrokerMessage)
		return ExitStatusConsumerError, err
	}

	log.Info().Msg(brokerConnectionSuccessMessage)

	// everything seems to be ok
	return ExitStatusOK, nil
}

// startService function tries to start the Kafka monitor service.
func startService(config ConfigStruct) (int, error) {
	// prepare broker
	brokerConfiguration := GetBrokerConfiguration(&config)

	verboseMode := GetOutputConfiguration(&config).Verbose

	// log the config
	log.Info().
		Str(brokerAddressMessage, brokerConfiguration.Address).
		Str(topic, brokerConfiguration.Topic).
		Str(group, brokerConfiguration.Group).
		Bool(enabled, brokerConfiguration.Enabled).
		Bool(verbose, verboseMode).
		Msg(brokerConfigurationMessage)

	// if broker is disabled, simply don't start it
	if brokerConfiguration.Enabled {
		log.Info().Msg("Broker is enabled, about to start it")
		err := startConsumer(brokerConfiguration, verboseMode)
		if err != nil {
			log.Error().Err(err)
			return ExitStatusConsumerError, err
		}
	} else {
		log.Info().Msg("Broker is disabled, not starting it")
	}

	return ExitStatusOK, nil
}

// startConsumer function starts the Kafka consumer.
func startConsumer(config BrokerConfiguration, verbose bool) error {
	consumer, err := NewConsumer(config, verbose)
	if err != nil {
		log.Error().Err(err).Msg("Construct broker failed")
		return err
	}
	consumer.Serve()
	return nil
}

// doSelectedOperation function perform operation selected on command line.
// When no operation is specified, the Insights Kafka monitor service is
// started instead.
func doSelectedOperation(configuration ConfigStruct, cliFlags CliFlags) (int, error) {
	switch {
	case cliFlags.ShowVersion:
		showVersion()
		return ExitStatusOK, nil
	case cliFlags.ShowAuthors:
		showAuthors()
		return ExitStatusOK, nil
	case cliFlags.ShowConfiguration:
		showConfiguration(configuration)
		return ExitStatusOK, nil
	case cliFlags.CheckConnectionToKafka:
		return tryToConnectToKafka(configuration)
	default:
		exitCode, err := startService(configuration)
		return exitCode, err
	}
	// this can not happen: return ExitStatusOK, nil
	// return ExitStatusOK, nil
}

// main function is entry point to the Kafka monitor service.
func main() {
	var cliFlags CliFlags

	// define and parse all command line options
	flag.BoolVar(&cliFlags.ShowVersion, "version", false, "show version")
	flag.BoolVar(&cliFlags.ShowAuthors, "authors", false, "show authors")
	flag.BoolVar(&cliFlags.ShowConfiguration, "show-configuration", false, "show configuration")
	flag.BoolVar(&cliFlags.CheckConnectionToKafka, "check-kafka", false, "check connection to Kafka")
	flag.Parse()

	// config has exactly the same structure as *.toml file
	config, err := LoadConfiguration(configFileEnvVariableName, defaultConfigFileName)
	if err != nil {
		log.Err(err).Msg("Load configuration")
		os.Exit(ExitStatusError)
	}

	if config.Logging.Debug {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	log.Debug().Msg("Started")

	// perform selected operation
	exitStatus, err := doSelectedOperation(config, cliFlags)
	if err != nil {
		log.Err(err).Msg("Do selected operation")
		os.Exit(exitStatus)
		return
	}

	log.Debug().Msg("Finished")
}
