package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

type ServerConfig struct {
	Lab3DotSqlFilePath string `json:"lab3_dotsql_filepath"`
	Lab4DotSqlFilePath string `json:"lab4_dotsql_filepath"`

	DatabaseUsername           string `json:"database_username"`
	DatabasePassword           string `json:"database_password"`
	DatabaseConnectionProtocol string `json:"database_connection_protocol"`
	DatabaseHost               string `json:"database_host"`
	DatabasePort               uint   `json:"database_port"`

	ServerConnectionProtocol string `json:"server_connection_protocol"`
	ServerHost               string `json:"server_host"`
	ServerPort               uint   `json:"server_port"`
}

var SrvConf = &ServerConfig{}

func init() {
	configPath := flag.String("config", "", "config path")

	flag.Parse()

	if configPath == nil || *configPath == "" {
		log.Panicf("configPath can't be empty!")
	}

	SrvConf.load(*configPath)
	SrvConf.verify()
}

func (sc *ServerConfig) load(configPath string) {

	jsonFile, err := os.Open(configPath)
	if err != nil {
		log.Panicf("err while loading config: %v", err)
	}

	b, err := io.ReadAll(jsonFile)
	if err != nil {
		log.Panicf("err while loading config: %v", err)
	}

	err = json.Unmarshal(b, &sc)
	if err != nil {
		log.Panicf("err while loading config: %v", err)
	}
}

func (sc *ServerConfig) verify() {
	// sql queries
	if sc.Lab3DotSqlFilePath == "" {
		log.Panicf("Lab3DotSqlFilePath can't be empty!")
	}
	if sc.Lab4DotSqlFilePath == "" {
		log.Panicf("Lab4DotSqlFilePath can't be empty!")
	}

	// db auth
	if sc.DatabaseUsername == "" {
		log.Panicf("DatabaseUsername can't be empty!")
	}
	if sc.DatabasePassword == "" {
		log.Panicf("DatabasePassword can't be empty!")
	}

	// db conn
	if sc.DatabaseConnectionProtocol == "" {
		log.Printf("DatabaseConnectionProtocol is empty, setting to default: tcp.")
		sc.DatabaseConnectionProtocol = "tcp"
	}
	if sc.DatabaseHost == "" {
		log.Printf("DatabaseHost is empty, setting to default: localhost.")
		sc.DatabaseHost = "localhost"
	}
	if sc.DatabasePort == 0 {
		log.Printf("DatabasePort == 0, setting to default: 3306.")
		sc.DatabasePort = 3306
	}

	// srv conn
	if sc.ServerConnectionProtocol == "" {
		log.Printf("ServerConnectionProtocol is empty, setting to default: tcp.")
		sc.ServerConnectionProtocol = "tcp"
	}
	if sc.ServerHost == "" {
		log.Printf("ServerHost is empty, setting to default: localhost.")
		sc.ServerHost = "localhost"
	}
	if sc.ServerPort == 0 {
		log.Printf("ServerPort == 0, setting to default: 5555.")
		sc.ServerPort = 5555
	}
}

// username:password@protocol(host:port)/  <-- empty db name required!
func (sc *ServerConfig) DataSourceName() string {
	return fmt.Sprintf(
		"%s:%s@%s(%s:%d)/",
		SrvConf.DatabaseUsername,
		SrvConf.DatabasePassword,
		SrvConf.DatabaseConnectionProtocol,
		SrvConf.DatabaseHost,
		SrvConf.DatabasePort,
	)
}
