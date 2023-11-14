package mev

import (
	"encoding/json"
	"os"
)

type MEVFlag struct {
	Enable     bool
	ConfigPath string
}

func MEVFlagDefault() *MEVFlag {
	return &MEVFlag{
		Enable:     false,
		ConfigPath: "",
	}
}

type MEVConfig struct {
	Server struct {
		TcpPort          int
		DomainSocketPath string
		GrpcMaxRecvMsgSz int
	}
}

func loadMEVconfig(path string) (*MEVConfig, error) {
	j, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c MEVConfig
	if err := json.Unmarshal(j, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
