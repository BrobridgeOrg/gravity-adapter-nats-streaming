package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	pb "gravity-adapter-nats-streaming/pb"

	"github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
	"github.com/sony/sonyflake"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type Packet struct {
	EventName string      `json:"eventName"`
	Payload   interface{} `json:"payload"`
}

type Client struct {
	Info      *SourceInfo
	Connector stan.Conn
}

func CreateClient() *Client {
	return &Client{}
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

	clusterID, ok := params["cluster_id"]
	if !ok {
		return nil
	}

	channel, ok := client.Info.Params["channel"]
	if !ok {
		return errors.New("channel is required")
	}

	// Genereate a unique ID for instance
	flake := sonyflake.NewSonyflake(sonyflake.Settings{})
	id, err := flake.NextID()
	if err != nil {
		return nil
	}

	idStr := strconv.FormatUint(id, 16)

	log.WithFields(log.Fields{
		"host":       host,
		"clientName": idStr,
		"clusterID":  clusterID,
		"channel":    channel,
	}).Info("Connecting to source")

	uri := fmt.Sprintf("%s:%d", host, port)

	// Connect to queue server
	sc, err := stan.Connect(clusterID.(string), idStr, stan.NatsURL(uri))
	if err != nil {
		return err
	}

	client.Connector = sc

	return nil
}

func (client *Client) Subscribe() error {

	channel, ok := client.Info.Params["channel"]
	if !ok {
		return errors.New("channel is required")
	}

	durableName, ok := client.Info.Params["durable_name"]
	if !ok {
		if _, err := client.Connector.Subscribe(channel.(string), client.HandleMessage); err != nil {
			return err
		}

	} else {

		if _, err := client.Connector.Subscribe(channel.(string), client.HandleMessage, stan.DurableName(durableName.(string))); err != nil {
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
		return
	}

	log.WithFields(log.Fields{
		"event": packet.EventName,
	}).Info("Received event")

	// Convert payload to JSON string
	payload, err := json.Marshal(packet.Payload)
	if err != nil {
		return
	}

	request := &pb.PublishRequest{
		EventName: packet.EventName,
		Payload:   string(payload),
	}

	// Set up a connection to data soource adapter.
	address := viper.GetString("dsa.host")
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Error("did not connect: ", err)
		return
	}
	defer conn.Close()

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
	}
}
