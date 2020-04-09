package adapter

import (
	"encoding/json"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type SourceConfig struct {
	Sources map[string]SourceInfo `json:"sources"`
}

type SourceInfo struct {
	Host   string                 `json:"host"`
	Port   int                    `json:"port"`
	Params map[string]interface{} `json:"params"`
}

type SourceManager struct {
	sources map[string]*Client
}

func CreateSourceManager() *SourceManager {
	return &SourceManager{
		sources: make(map[string]*Client),
	}
}

func (sm *SourceManager) Initialize() error {

	config, err := sm.LoadSourceConfig(viper.GetString("source.config"))
	if err != nil {
		return err
	}

	// Initializing database connections
	for name, info := range config.Sources {

		log.WithFields(log.Fields{
			"name": name,
		}).Info("Initializing source")

		// Initialize source connector
		_, err := sm.InitSource(name, &info)
		if err != nil {
			log.Error(err)
			continue
		}
	}

	return nil
}

func (sm *SourceManager) LoadSourceConfig(filename string) (*SourceConfig, error) {

	// Open configuration file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	// Read
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var config SourceConfig
	json.Unmarshal(byteValue, &config)

	return &config, nil
}

func (sm *SourceManager) InitSource(name string, info *SourceInfo) (*Client, error) {

	if db, ok := sm.sources[name]; ok {
		return db, nil
	}

	c := CreateClient()
	err := c.Connect(info.Host, info.Port, info.Params)
	if err != nil {
		return nil, err

	}

	// subscribe to channel
	err = c.Subscribe()
	if err != nil {
		return nil, err
	}

	sm.sources[name] = c

	return c, nil
}

func (sm *SourceManager) GetClient(name string) *Client {

	if s, ok := sm.sources[name]; ok {
		return s
	}

	return nil
}
