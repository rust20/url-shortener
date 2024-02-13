package config

import (
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type ServerConfig struct {
	Name     string
	Addr     string
	RestAddr string
}

type Config struct {
	// CurrentNode
	Nodes []ServerConfig

	// Self ServerConfig

	HeartbeatInterval    time.Duration
	ElectionTimeoutBase  time.Duration
	ElectionTimeoutRange int

	DBFileName string

	RaftServePort string
	RestServePort string
}

func GetConfig(configName string) Config {

	viper.SetConfigName(configName) // name of config file (without extension)
	viper.SetConfigType("yaml")     // REQUIRED if the config file does not have the extension in the name

	// viper.AddConfigPath("/etc/appname/")  // path to look for the config file in
	// viper.AddConfigPath("$HOME/.appname") // call multiple times to add many search paths
	viper.SetDefault("RaftServePort", ":1337")
	viper.SetDefault("RestServePort", ":8080")

	viper.AddConfigPath(".")    // optionally look for config in the working directory
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.Panicf("fatal error config file: %v", err)
	}

	cfg := &Config{}
	viper.Unmarshal(cfg)
	if err != nil {
		log.Panicf("unable to decode into struct, %v", err)
	}

	// log.Info(cfg)

	return *cfg
}
