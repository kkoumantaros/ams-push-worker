package sendersx

type Sender interface {
	Send(msg string) error
}
