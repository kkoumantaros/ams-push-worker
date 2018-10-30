package brokers

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type topicLock struct {
	sync.Mutex
}

// Kafka struct that implements the broker interface and provides functionality to interact with Kafka
type Kafka struct {
	sync.Mutex
	createTopicLock topicLock
	consumeLock     map[string]*topicLock
	Config          *sarama.Config
	Client          sarama.Client
	Consumer        sarama.Consumer
	Servers         []string
}

// NewKafkaBroker creates a new kafka broker object
func NewKafkaBroker(peers []string) *Kafka {
	brk := new(Kafka)
	brk.Servers = peers
	brk.Initialize()
	return brk
}

// Initialize sets up the Kafka object and attempts to connect to broker backend
func (b *Kafka) Initialize() {

	for {
		// Try to initialize broker backend
		log.Info("BROKER", "\t", "Attempting to connect to kafka backend: ", b.Servers)
		b.createTopicLock = topicLock{}
		b.consumeLock = make(map[string]*topicLock)
		b.Config = sarama.NewConfig()
		b.Config.Consumer.Fetch.Default = 1000000

		var err error

		b.Client, err = sarama.NewClient(b.Servers, nil)

		b.Consumer, err = sarama.NewConsumer(b.Servers, b.Config)

		// if err happened log it and retry in 3sec - else all is ok so return
		if err != nil {
			log.Error("BROKER", "\t", err.Error())
			time.Sleep(3 * time.Second)
		} else {
			log.Info("BROKER", "\t", "Kafka Backend Initialized! Kafka node list", b.Servers)
			return
		}
	}
}

// Consume function to consume a message from the broker
func (b *Kafka) Consume(ctx context.Context, topic string, offset int64, imm bool, max int64) ([]string, error) {

	b.lockForTopic(topic)

	defer b.unlockForTopic(topic)
	// Fetch offsets
	newOff, err := b.Client.GetOffset(topic, 0, sarama.OffsetNewest)

	if err != nil {
		log.Error(err.Error())
	}

	oldOff, err := b.Client.GetOffset(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Error(err.Error())
	}

	log.Infof("Consuming topic: %v, min_offset: %v, max_offset: %v, current_offset: %v", topic, oldOff, newOff, offset)

	// If tracked offset is equal or bigger than topic offset means no new messages
	if offset >= newOff {
		return []string{}, nil
	}

	// If tracked offset is left behind increment it to topic's min. offset
	if offset < oldOff {
		log.Infof("Tracked offset is off for topic: %v, broker_offset %v, tracked_offset: %v", topic, offset, oldOff)
		return []string{}, errors.New("offset is off")
	}

	partitionConsumer, err := b.Consumer.ConsumePartition(topic, 0, offset)

	if err != nil {
		log.Errorf("Unable to consume topic %v, %v, min_offset: %v, max_offset: %v, current_offset: %v", topic, err.Error(), newOff, oldOff, offset)
		return []string{}, err

	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Error(err)
		}
	}()

	messages := make([]string, 0)
	var consumed int64
	timeout := time.After(300 * time.Second)

	if imm {
		timeout = time.After(100 * time.Millisecond)
	}

ConsumerLoop:
	for {
		select {
		// If the http client cancels the http request break consume loop
		case <-ctx.Done():
			{
				break ConsumerLoop
			}
		case <-timeout:
			{
				break ConsumerLoop
			}
		case msg := <-partitionConsumer.Messages():

			messages = append(messages, string(msg.Value[:]))

			consumed++

			log.Infof("Consumed: %v, Max: %v, Latest Message: %v", consumed, max, string(msg.Value[:]))

			// if we pass over the available messages and still want more
			if consumed >= max {
				break ConsumerLoop
			}

			if offset+consumed > newOff-1 {
				// if returnImmediately is set don't wait for more
				if imm {
					break ConsumerLoop
				}

			}

		}
	}

	return messages, nil
}

func (b *Kafka) lockForTopic(topic string) {
	// Check if lock for topic exists
	_, present := b.consumeLock[topic]
	if present == false {
		// TopicLock is not in list so add it
		b.createTopicLock.Lock()
		_, nowPresent := b.consumeLock[topic]
		if nowPresent == false {
			b.consumeLock[topic] = &topicLock{}
			b.consumeLock[topic].Lock()
		}
		b.createTopicLock.Unlock()
	} else {
		b.consumeLock[topic].Lock()
	}
}

func (b *Kafka) unlockForTopic(topic string) {
	// Check if lock for topic exists
	_, present := b.consumeLock[topic]
	if present == false {
		return
	}

	b.consumeLock[topic].Unlock()

}

// CloseConnections closes open consumer and client
func (b *Kafka) CloseConnections() {

	// Close Consumer
	if err := b.Consumer.Close(); err != nil {
		log.Error(err)
	}
	// Close Client
	if err := b.Client.Close(); err != nil {
		log.Error(err)
	}

}
