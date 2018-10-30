package brokers

import (
	"context"
	"github.com/stretchr/testify/suite"
	"testing"
)

type BrokerTestSuite struct {
	suite.Suite
}

func (suite *BrokerTestSuite) TestInitialize() {

	mockBroker := NewMockBroker()
	msgList := make(map[string][]string)

	msgList["test-topic-1"] = append(msgList["test-topic-1"], "msg0", "msg1", "msg2", "msg3", "msg4", "msg5")

	suite.Equal(true, mockBroker.Client)
	suite.Equal(true, mockBroker.Consumer)
	suite.Equal(int64(0), mockBroker.Offset)
	suite.Equal(msgList, mockBroker.MessageList)
}

func (suite *BrokerTestSuite) TestCloseConnections() {

	mockBroker := NewMockBroker()
	mockBroker.CloseConnections()

	suite.Equal(false, mockBroker.Client)
	suite.Equal(false, mockBroker.Consumer)
}

func (suite *BrokerTestSuite) TestConsume() {

	mockBroker := NewMockBroker()

	// consume all messages available at a topic
	expMsgList1 := make([]string, 0)
	expMsgList1 = append(expMsgList1, "msg0", "msg1", "msg2", "msg3", "msg4", "msg5")
	msgList1, err1 := mockBroker.Consume(context.Background(), "test-topic-1", 0, false, 5)

	// try to consume from an offset larger than the bounds of the list
	msgList2, err2 := mockBroker.Consume(context.Background(), "test-topic-1", 7, false, 2)

	// consume 2 messages starting from offset 2
	expMsgList3 := make([]string, 0)
	expMsgList3 = append(expMsgList3, "msg2", "msg3", "msg4")
	msgList3, err3 := mockBroker.Consume(context.Background(), "test-topic-1", 2, false, 2)

	// left-behind offset
	mockBroker.Offset = 5
	_, err4 := mockBroker.Consume(context.Background(), "test-topic-1", 2, false, 2)

	suite.Equal(expMsgList1, msgList1)
	suite.Equal(0, len(msgList2))
	suite.Equal(expMsgList3, msgList3)

	suite.Nil(err1)
	suite.Nil(err2)
	suite.Nil(err3)
	suite.Equal("offset is off", err4.Error())
}

func TestBrokerTestSuite(t *testing.T) {
	suite.Run(t, new(BrokerTestSuite))
}
