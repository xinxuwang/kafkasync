package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/xinxuwang/kafkasync"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"
)

var KS *kafkasync.KafkaSync

type EchoRouter struct {
}

type Ping struct {
	RequestId string `json:"requestId"`
	Context   string `json:"context"`
}

type Pong struct {
	RequestId string `json:"requestId"`
	Context   string `json:"context"`
}

func (br *EchoRouter) GetProducerId(msg *sarama.ProducerMessage) (clientId string, err error) {
	pingReq := Ping{}
	value, ok := msg.Value.(sarama.ByteEncoder)
	if !ok {
		return "", fmt.Errorf("Convert error:%v\n", ok)
	}
	err = json.Unmarshal(value, &pingReq)
	if err != nil {
		return "", fmt.Errorf("Unmarshal error:%v\n", err)
	}
	return pingReq.RequestId, nil
}

func (br *EchoRouter) FindReplayChans(ks *kafkasync.KafkaSync, msg *sarama.ConsumerMessage) (replayChans []chan *sarama.ConsumerMessage, err error) {
	pongResponse := Pong{}
	err = json.Unmarshal(msg.Value, &pongResponse)
	if err != nil {
		return nil, fmt.Errorf("Unmarshal error:%v\n", err)
	}
	client, ok := ks.ProducerMap[pongResponse.RequestId]
	if !ok {
		return nil, fmt.Errorf("RequestId[%v] not in ProducerMap\n", pongResponse.RequestId)
	}
	replayChans = append(replayChans, client.Reply)
	return replayChans, nil
}

func init() {
	router := EchoRouter{}
	clusterCfg := cluster.NewConfig()
	clusterCfg.Metadata.Retry.Max = 10000
	clusterCfg.Consumer.Return.Errors = true
	clusterCfg.Group.Return.Notifications = true
	clusterCfg.Consumer.Offsets.AutoCommit.Enable = false
	clusterCfg.Consumer.Offsets.CommitInterval = 1 * time.Second
	clusterCfg.Version = sarama.V2_4_0_0
	clusterCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	//
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Metadata.Retry.Max = 10000
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Version = sarama.V2_4_0_0
	cfg := &kafkasync.Config{
		ClusterCfg: clusterCfg,
		SaramaCfg:  kafkaConfig,
		//WaitReplayTimeout: 10 * time.Second,
		Router:          &router,
		ConsumerBrokers: []string{"127.0.0.1:9092"},
		ProducerBrokers: []string{"127.0.0.1:9092"},
		ConsumerTopics:  []string{"echo-response"},
		ConsumerGroupId: "test-test",
		StopChan:        make(chan struct{}),
	}
	ks, err := kafkasync.NewKafkaSync(cfg)
	if err != nil {
		os.Exit(0)
	}
	KS = ks
}

func main() {
	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals)

	go func() {
		select {
		case <-signals:
			fmt.Println("stop signals")
			close(KS.Config.StopChan)
		}
	}()
	num := 10
	wg := sync.WaitGroup{}
	wg.Add(num)
	for i := 0; i < num; i++ {
		go func(i int) {
			defer wg.Done()
			content := Ping{
				RequestId: fmt.Sprintf("%d", i),
				Context:   fmt.Sprintf("%d,ping", i),
			}
			data, err := json.Marshal(content)
			if err != nil {
				log.Println("Marshal:", err)
				return
			}
			msg := &sarama.ProducerMessage{
				Topic: "echo-request",
				Value: sarama.ByteEncoder(data),
			}
			ctx, _ := context.WithTimeout(context.Background(), time.Minute*5)
			reply, err := KS.SendMessage(msg, ctx)
			if err != nil {
				log.Println("Send error:", err, " msg:", string(data))
				return
			}
			log.Println("reply:", string(reply.Value))
			time.Sleep(KS.Config.ClusterCfg.Consumer.Offsets.CommitInterval + time.Second*1)
		}(i)
	}
	wg.Wait()
}
