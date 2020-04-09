package adapter

import (
	app "gravity-adapter-nats-streaming/app/interface"

	log "github.com/sirupsen/logrus"
)

type Service struct {
	app app.AppImpl
	sm  *SourceManager
}

func CreateService(a app.AppImpl) *Service {

	sm := CreateSourceManager()
	if sm == nil {
		return nil
	}

	err := sm.Initialize()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Preparing service
	service := &Service{
		app: a,
		sm:  sm,
	}

	return service
}
