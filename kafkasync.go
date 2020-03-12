package kafkasync

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"time"
)

type ProducerClient struct {
	Reply       chan *sarama.ConsumerMessage
	ProducerMsg *sarama.ProducerMessage
}

type Config struct {
	ClusterCfg      *cluster.Config
	SaramaCfg       *sarama.Config
	Router          Router
	ConsumerBrokers []string
	ProducerBrokers []string
	ConsumerTopics  []string
	ConsumerGroupId string
	StopChan        chan struct{}
}

type KafkaSync struct {
	Consumer           *cluster.Consumer
	Producer           sarama.SyncProducer
	ProducerMap        map[string]*ProducerClient
	ProducerRegister   chan *ProducerClient
	ProducerUnRegister chan *ProducerClient
	Config             *Config
}

type Router interface {
	GetProducerId(msg *sarama.ProducerMessage) (clientId string, err error)
	FindReplayChans(ks *KafkaSync, msg *sarama.ConsumerMessage) (replayChans []chan *sarama.ConsumerMessage, err error)
}

func NewKafkaSync(cfg *Config) (*KafkaSync, error) {
	if err := cfg.ClusterCfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka producer config invalidate. err: %v", err)
	}

	consumer, err := cluster.NewConsumer(cfg.ConsumerBrokers, cfg.ConsumerGroupId, cfg.ConsumerTopics, cfg.ClusterCfg)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer create fail. err: %v", err)
	}

	if err := cfg.SaramaCfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka producer config invalidate. err: %v", err)
	}
	producer, err := sarama.NewSyncProducer(cfg.ProducerBrokers, cfg.SaramaCfg)
	if err != nil {
		return nil, fmt.Errorf("kafka producer create fail. err: %v", err)
	}

	if cfg.Router == nil {
		return nil, fmt.Errorf("Router is nil\n")
	}
	kafkaSync := &KafkaSync{
		Consumer:           consumer,
		Producer:           producer,
		ProducerMap:        make(map[string]*ProducerClient, 0),
		ProducerRegister:   make(chan *ProducerClient, 0),
		ProducerUnRegister: make(chan *ProducerClient, 0),
		Config:             cfg,
	}
	go kafkaSync.runConsumer()
	return kafkaSync, nil
}

func (ks *KafkaSync) runConsumer() {
	for {
		select {
		case msg, ok := <-ks.Consumer.Messages():
			if ok {
				fmt.Printf("%s/%d/%d\t%s\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value, msg.Timestamp.String())
				//Route the replay msg to the sender
				replayChans, err := ks.Config.Router.FindReplayChans(ks, msg)
				if err != nil {
					fmt.Printf("Can't find the replayChan msg[%s],err[%v]\n", msg.Value, err)
					ks.Consumer.MarkOffset(msg, "")
					break
				}
				ks.Consumer.MarkOffset(msg, "")
				for _, v := range replayChans {
					v <- msg
				}
			}
		case notify, more := <-ks.Consumer.Notifications():
			if more {
				fmt.Printf("Kafka consumer rebalance: %v\n", notify)
			}
		case client := <-ks.ProducerRegister:
			clientId, err := ks.Config.Router.GetProducerId(client.ProducerMsg)
			if err != nil {
				break
			}
			ks.ProducerMap[clientId] = client
		case client := <-ks.ProducerUnRegister:
			clientId, err := ks.Config.Router.GetProducerId(client.ProducerMsg)
			if err != nil {
				break
			}
			delete(ks.ProducerMap, clientId)
		case <-ks.Config.StopChan:
			fmt.Println("Safe stopping wait for all msg committed")
			time.Sleep(ks.Config.ClusterCfg.Consumer.Offsets.CommitInterval + time.Second*1)
			err := ks.Consumer.Close()
			if err != nil {
				fmt.Println("Stop Consumer err:", err)
			}
			err = ks.Producer.Close()
			if err != nil {
				fmt.Println("Stop Producer err:", err)
			}
			//close reply chan and clean map
			for k, v := range ks.ProducerMap {
				close(v.Reply)
				delete(ks.ProducerMap, k)
			}
			return
		}
	}
}

func (ks *KafkaSync) SendMessage(msg *sarama.ProducerMessage, ctx context.Context) (reply *sarama.ConsumerMessage, err error) {
	replyChan := make(chan *sarama.ConsumerMessage, 1)

	client := &ProducerClient{
		Reply:       replyChan,
		ProducerMsg: msg,
	}
	ks.ProducerRegister <- client

	_, _, err = ks.Producer.SendMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("Kafka send message error %v. msg:%v\n", err, msg)
	}

	select {
	case rm, isOpen := <-replyChan:
		if !isOpen {
			err = fmt.Errorf("Chan close\n")
		} else {
			reply = rm
			ks.ProducerUnRegister <- client
		}
	case <-ctx.Done():
		err = ctx.Err()
		ks.ProducerUnRegister <- client
	}
	return
}
