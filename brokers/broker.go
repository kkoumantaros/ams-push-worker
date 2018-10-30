package brokers

import "context"

type Broker interface {
	Initialize()
	Consume(ctx context.Context, topic string, offset int64, imm bool, max int64) ([]string, error)
	CloseConnections()
}
