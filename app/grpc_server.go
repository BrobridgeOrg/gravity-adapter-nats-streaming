package app

import (
	"net"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	app "gravity-adapter-nats-streaming/app/interface"
	pb "gravity-adapter-nats-streaming/pb"
	adapter "gravity-adapter-nats-streaming/services/adapter"
)

func (a *App) InitGRPCServer(host string) error {

	// Start to listen on port
	lis, err := net.Listen("tcp", host)
	if err != nil {
		log.Fatal(err)
		return err
	}

	log.WithFields(log.Fields{
		"host": host,
	}).Info("Starting gRPC server on " + host)

	// Create gRPC server
	s := grpc.NewServer()

	// Register data source adapter service
	adapterService := adapter.CreateService(app.AppImpl(a))
	pb.RegisterAdapterServer(s, adapterService)
	reflection.Register(s)

	log.WithFields(log.Fields{
		"service": "Adapter",
	}).Info("Registered service")

	// Starting server
	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}
