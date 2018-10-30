package brokers

import (
	"context"
	"errors"
)

type MockBroker struct {
	Client      bool
	Consumer    bool
	MessageList map[string][]string
	Offset      int64
}

func NewMockBroker() *MockBroker {
	brk := new(MockBroker)
	brk.Initialize()
	return brk
}

func (mb *MockBroker) Initialize() {

	mb.Client = true
	mb.Consumer = true

	mb.Offset = int64(0)

	mb.MessageList = make(map[string][]string)

	mb.MessageList["test-topic-1"] = append(mb.MessageList["test-topic-1"], "msg0", "msg1", "msg2", "msg3", "msg4", "msg5")

}

func (mb *MockBroker) Consume(ctx context.Context, topic string, offset int64, imm bool, max int64) ([]string, error) {

	var msgs []string
	var err error

	topicMsgs, _ := mb.MessageList[topic]

	if offset >= int64(len(topicMsgs)) {
		return msgs, err
	}

	if offset < mb.Offset {
		return msgs, errors.New("offset is off")
	}

	// begin 'consuming'
	var consumed int64 = 0
	for _, msg := range topicMsgs[offset:] {

		if consumed > max {
			break
		}

		msgs = append(msgs, msg)
		consumed++
	}

	return msgs, nil

}

func (mb *MockBroker) CloseConnections() {
	mb.Client = false
	mb.Consumer = false
}
