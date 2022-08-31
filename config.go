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

// This source file contains definition of data type named ConfigStruct that
// represents configuration of Kafka monitor service. This source file also
// contains function named LoadConfiguration that can be used to load
// configuration from provided configuration file and/or from environment
// variables. Additionally several specific functions named
// GetStorageConfiguration and GetLoggingConfiguration are to be used to return
// specific configuration options.

// Default name of configuration file is config.toml
// It can be changed via environment variable KAFKA_MONITOR_CONFIG_FILE

// An example of configuration file that can be used in devel environment:
//
// [broker]
// address = "kafka:29092"
// topic = "ccx.ocp.results"
// group = "aggregator"
// enabled = true
//
// [logging]
// debug = true
// log_level = ""
//
// Environment variables that can be used to override configuration file settings:
// TBD

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"

	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// Common constants used for logging and error reporting
const (
	filenameAttribute               = "filename"
	parsingConfigurationFileMessage = "parsing configuration file"
)

// ConfigStruct is a structure holding the whole notification service
// configuration
type ConfigStruct struct {
	Broker  BrokerConfiguration  `mapstructure:"broker"  toml:"broker"`
	Logging LoggingConfiguration `mapstructure:"logging" toml:"logging"`
	Output  OutputConfiguration  `mapstructure:"output"  toml:"output"`
}

// LoggingConfiguration represents configuration for logging in general
type LoggingConfiguration struct {
	// Debug enables pretty colored logging
	Debug bool `mapstructure:"debug" toml:"debug"`

	// LogLevel sets logging level to show. Possible values are:
	// "debug"
	// "info"
	// "warn", "warning"
	// "error"
	// "fatal"
	//
	// logging level won't be changed if value is not one of listed above
	LogLevel string `mapstructure:"log_level" toml:"log_level"`
}

// BrokerConfiguration represents configuration for the broker
type BrokerConfiguration struct {
	// Address represents Kafka address
	Address string `mapstructure:"address" toml:"address"`
	// SecurityProtocol represents the security protocol used by the broker
	SecurityProtocol string `mapstructure:"security_protocol" toml:"security_protocol"`
	// CertPath is the path to a file containing the certificate to be used with the broker
	CertPath string `mapstructure:"cert_path" toml:"cert_path"`
	// SaslMechanism is the SASL mechanism used for authentication
	SaslMechanism string `mapstructure:"sasl_mechanism" toml:"sasl_mechanism"`
	// SaslUsername is the username used in case of PLAIN mechanism
	SaslUsername string `mapstructure:"sasl_username" toml:"sasl_username"`
	// SaslPassword is the password used in case of PLAIN mechanism
	SaslPassword string `mapstructure:"sasl_password" toml:"sasl_password"`
	// Topic is name of Kafka topic
	Topic string `mapstructure:"topic" toml:"topic"`
	// Group is name of Kafka group
	Group string `mapstructure:"group" toml:"group"`
	// Enabled is set to true if Kafka consumer is to be enabled
	Enabled bool `mapstructure:"enabled" toml:"enabled"`
}

// OutputConfiguration configures which log messages to use
type OutputConfiguration struct {
	Verbose bool `mapstructure:"verbose" toml:"verbose"`
}

// LoadConfiguration loads configuration from defaultConfigFile, file set in
// configFileEnvVariableName or from env
func LoadConfiguration(configFileEnvVariableName, defaultConfigFile string) (ConfigStruct, error) {
	var config ConfigStruct

	// env. variable holding name of configuration file
	configFile, specified := os.LookupEnv(configFileEnvVariableName)
	if specified {
		log.Info().Str(filenameAttribute, configFile).Msg(parsingConfigurationFileMessage)
		// we need to separate the directory name and filename without
		// extension
		directory, basename := filepath.Split(configFile)
		file := strings.TrimSuffix(basename, filepath.Ext(basename))
		// parse the configuration
		viper.SetConfigName(file)
		viper.AddConfigPath(directory)
	} else {
		log.Info().Str(filenameAttribute, defaultConfigFile).Msg(parsingConfigurationFileMessage)
		// parse the configuration
		viper.SetConfigName(defaultConfigFile)
		viper.AddConfigPath(".")
	}

	// try to read the whole configuration
	err := viper.ReadInConfig()
	if _, isNotFoundError := err.(viper.ConfigFileNotFoundError); !specified && isNotFoundError {
		// If config file is not present (which might be correct in
		// some environment) we need to read configuration from
		// environment variables. The problem is that Viper is not
		// smart enough to understand the structure of config by
		// itself, so we need to read fake config file
		fakeTomlConfigWriter := new(bytes.Buffer)

		err := toml.NewEncoder(fakeTomlConfigWriter).Encode(config)
		if err != nil {
			return config, err
		}

		fakeTomlConfig := fakeTomlConfigWriter.String()

		viper.SetConfigType("toml")

		err = viper.ReadConfig(strings.NewReader(fakeTomlConfig))
		if err != nil {
			return config, err
		}
	} else if err != nil {
		// error is processed on caller side
		return config, fmt.Errorf("fatal error config file: %s", err)
	}

	// override config from env if there's variable in env

	const envPrefix = "INSIGHTS_KAFKA_MONITOR_"

	viper.AutomaticEnv()
	viper.SetEnvPrefix(envPrefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "__"))

	err = viper.Unmarshal(&config)
	if err != nil {
		return config, err
	}

	// everything's should be ok
	return config, nil
}

// GetLoggingConfiguration returns logging configuration
func GetLoggingConfiguration(config *ConfigStruct) LoggingConfiguration {
	return config.Logging
}

// GetBrokerConfiguration returns broker configuration
func GetBrokerConfiguration(config *ConfigStruct) BrokerConfiguration {
	return config.Broker
}

// GetOutputConfiguration returns output configuration
func GetOutputConfiguration(config *ConfigStruct) OutputConfiguration {
	return config.Output
}
