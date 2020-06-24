package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	pb "gravity-adapter-nats-streaming/pb"

	"github.com/flyaways/pool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
	"github.com/sony/sonyflake"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type Packet struct {
	EventName string      `json:"event"`
	Payload   interface{} `json:"payload"`
}

type Client struct {
	ID        string
	Info      *SourceInfo
	grpcPool  *pool.GRPCPool
	Connector stan.Conn
}

func CreateClient() *Client {

	address := viper.GetString("dsa.host")

	options := &pool.Options{
		InitTargets:  []string{address},
		InitCap:      5,
		MaxCap:       30,
		DialTimeout:  time.Second * 5,
		IdleTimeout:  time.Second * 60,
		ReadTimeout:  time.Second * 5,
		WriteTimeout: time.Second * 5,
	}

	// Initialize connection pool
	p, err := pool.NewGRPCPool(options, grpc.WithInsecure())

	if err != nil {
		log.Printf("%#v\n", err)
		return nil
	}

	if p == nil {
		log.Printf("p= %#v\n", p)
		return nil
	}

	// Generate client ID
	clientID := viper.GetString("event_store.client_name")
	if len(clientID) == 0 {

		// Genereate a unique ID for instance
		flake := sonyflake.NewSonyflake(sonyflake.Settings{})
		id, err := flake.NextID()
		if err != nil {
			return nil
		}

		clientID = strconv.FormatUint(id, 16)
	}

	return &Client{
		ID:       clientID,
		grpcPool: p,
	}
}

func (client *Client) onReconnected(natsConn *nats.Conn) {

	for {
		log.Warn("re-connect to event server")

		// Initializing NATS Streaming
		err := client.initSTAN(natsConn)
		if err != nil {
			log.Error("Failed to connect to event server")
			time.Sleep(time.Duration(1) * time.Second)
			continue
		}

		break
	}
}

func (client *Client) initSTAN(natsConn *nats.Conn) error {

	clusterID, ok := client.Info.Params["cluster_id"]
	if !ok {
		return nil
	}

	// Connect to NATS Streaming
	sc, err := stan.Connect(
		clusterID.(string),
		client.ID,
		stan.NatsConn(natsConn),
	)
	if err != nil {
		return err
	}

	client.Connector = sc

	return nil
}

func (client *Client) Connect(host string, port int, params map[string]interface{}) error {

	client.Info = &SourceInfo{
		Host:   host,
		Port:   port,
		Params: make(map[string]interface{}),
	}

	for key, value := range params {
		client.Info.Params[key] = value
	}

	// required cluster ID
	clusterID, ok := params["cluster_id"]
	if !ok {
		return nil
	}

	// required channel
	channel, ok := client.Info.Params["channel"]
	if !ok {
		return errors.New("channel is required")
	}

	log.WithFields(log.Fields{
		"host":       host,
		"clientName": client.ID,
		"clusterID":  clusterID,
		"channel":    channel,
	}).Info("Connecting to source")

	// Create NATS connection
	uri := fmt.Sprintf("%s:%d", host, port)
	nc, err := nats.Connect(uri,
		nats.PingInterval(10*time.Second),
		nats.MaxPingsOutstanding(3),
		nats.MaxReconnects(-1),
		nats.ReconnectHandler(client.onReconnected),
	)
	if err != nil {
		return err
	}

	// Initializing NATS Streaming
	err = client.initSTAN(nc)
	if err != nil {
		return err
	}

	return nil
}

func (client *Client) Subscribe() error {

	channel, ok := client.Info.Params["channel"]
	if !ok {
		return errors.New("channel is required")
	}

	durableName, ok := client.Info.Params["durable_name"]
	if !ok {
		if _, err := client.Connector.Subscribe(channel.(string), client.HandleMessage, stan.SetManualAckMode()); err != nil {
			return err
		}

	} else {

		if _, err := client.Connector.Subscribe(channel.(string), client.HandleMessage, stan.DurableName(durableName.(string)), stan.SetManualAckMode()); err != nil {
			return err
		}
	}

	return nil
}

func (client *Client) HandleMessage(m *stan.Msg) {

	var packet Packet

	// Parse JSON
	err := json.Unmarshal(m.Data, &packet)
	if err != nil {
		m.Ack()
		return
	}

	log.WithFields(log.Fields{
		"event": packet.EventName,
		"seq":   m.Sequence,
	}).Info("Received event")

	// Convert payload to JSON string
	payload, err := json.Marshal(packet.Payload)
	if err != nil {
		m.Ack()
		return
	}

	request := &pb.PublishRequest{
		EventName: packet.EventName,
		Payload:   string(payload),
	}

	// Getting connection from pool
	conn, err := client.grpcPool.Get()
	if err != nil {
		log.Error("Failed to get connection: ", err)
		return
	}
	defer client.grpcPool.Put(conn)

	// Preparing context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Publish
	resp, err := pb.NewDataSourceAdapterClient(conn).Publish(ctx, request)
	if err != nil {
		log.Error("did not connect: ", err)
		return
	}

	if resp.Success == false {
		log.Error("Failed to push message to data source adapter")
		return
	}

	m.Ack()
}
