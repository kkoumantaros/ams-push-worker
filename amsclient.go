package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

type Attributes map[string]string

type PushMsg struct {
	Msg Message `json:"message"`
	Sub string  `json:"subscription"`
}

// RecMsg holds info for a received message
type RecMsg struct {
	AckID string  `json:"ackId,omitempty"`
	Msg   Message `json:"message"`
}

type AckMsgs struct {
	AckIDS []string `json:"ackIds"`
}

// RecList holds the array of the receivedMessages - subscription related
type RecList struct {
	RecMsgs []RecMsg `json:"receivedMessages"`
}

// Message struct used to hold message information
type Message struct {
	ID      string     `json:"messageId,omitempty"`
	Attr    Attributes `json:"attributes,omitempty"`  // used to hold attribute key/value store
	Data    string     `json:"data"`                  // base64 encoded data payload
	PubTime string     `json:"publishTime,omitempty"` // publish timedate of message
}

type AMSClient struct {
	Endpoint     string
	Client       *http.Client
	Project      string
	Token        string
	ContentType  string
}

type PullOptions struct {
	MaxMessages       string `json:"maxMessages"`
	ReturnImmediately string `json:"returnImmediately"`
}

// Subscription struct to hold information for a given topic
type Subscription struct {
	ProjectUUID string     `json:"-"`
	Name        string     `json:"-"`
	Topic       string     `json:"-"`
	FullName    string     `json:"name"`
	FullTopic   string     `json:"topic"`
	PushCfg     PushConfig `json:"pushConfig"`
	Ack         int        `json:"ackDeadlineSeconds,omitempty"`
	Offset      int64      `json:"-"`
	NextOffset  int64      `json:"-"`
	PendingAck  string     `json:"-"`
}

// PushConfig holds optional configuration for push operations
type PushConfig struct {
	Pend   string      `json:"pushEndpoint"`
	RetPol RetryPolicy `json:"retryPolicy"`
}

// RetryPolicy holds information on retry policies
type RetryPolicy struct {
	PolicyType string `json:"type,omitempty"`
	Period     int    `json:"period,omitempty"`
}

func (ams *AMSClient) loadSubscription(subNAME string) (Subscription, error) {

	var sub Subscription
	var err error

	url := fmt.Sprintf("https://%v/v1/projects/%v/subscriptions/%v?key=%v", ams.Endpoint, ams.Project, subNAME, ams.Token)

	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return sub, nil
	}

	req.Header.Set("Content-type", ams.ContentType)
	log.Infof("Trying to retrieve subscription: %v from endpoint: %v", subNAME, ams.Endpoint)

	t1 := time.Now()
	resp, err := ams.Client.Do(req)

	if err != nil {
		return sub, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		buf := bytes.Buffer{}
		buf.ReadFrom(resp.Body)
		err = errors.New(fmt.Sprintf("Endpoint: %v responded with status code: %v and response: %v", ams.Endpoint, resp.StatusCode, buf.String()))
		return sub, err
	}

	err = json.NewDecoder(resp.Body).Decode(&sub)
	if err != nil {
		return sub, err
	}

	log.Infof("Retrieved subscription: %+v from endpoint: %v in %v", sub, ams.Endpoint, time.Since(t1).String())

	return sub, err
}

func (ams *AMSClient) pullMsg(subName string) (RecList, error) {

	var reqList RecList
	var err error

	url := fmt.Sprintf("https://%v/v1/projects/%v/subscriptions/%v:pull?key=%v", ams.Endpoint, ams.Project, subName, ams.Token)

	pullOptions := PullOptions{MaxMessages: "1", ReturnImmediately: "true"}
	pullOptB, err := json.Marshal(pullOptions)

	if err != nil {
		return reqList, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(pullOptB))
	if err != nil {
		return reqList, err
	}

	req.Header.Set("Content-Type", ams.ContentType)
	log.Infof("Trying to pull messages using subscription: %v from endpoint: %v", subName, ams.Endpoint)

	t1 := time.Now()
	resp, err := ams.Client.Do(req)

	if err != nil {
		return reqList, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {

		buf := bytes.Buffer{}
		buf.ReadFrom(resp.Body)
		err = errors.New(fmt.Sprintf("Endpoint: %v responded with status code: %v and response: %v", ams.Endpoint, resp.StatusCode, buf.String()))
		return reqList, err
	}

	err = json.NewDecoder(resp.Body).Decode(&reqList)

	if err != nil {
		return reqList, err
	}

	log.Infof("Messages %+v from endpoint: %v consumed in: %v", reqList, ams.Endpoint, time.Since(t1).String())

	return reqList, err

}

func (ams *AMSClient) publish(msg PushMsg, endpoint string) error {

	var err error

	msgB, err := json.Marshal(msg)

	if err != nil {

	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(msgB))

	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", ams.ContentType)
	log.Infof("Trying to push message: %+v to: %v", msg, endpoint)

	t1 := time.Now()
	resp, err := ams.Client.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 && resp.StatusCode != 204 && resp.StatusCode != 102 {

		buf := bytes.Buffer{}
		buf.ReadFrom(resp.Body)
		err = errors.New(fmt.Sprintf("Endpoint: %v responded with status code: %v and response: %v", endpoint, resp.StatusCode, buf.String()))
		return err
	}

	log.Infof("Message: %+v to endpoint: %v delivered in: %v", msg, endpoint, time.Since(t1).String())

	return err

}

func (ams *AMSClient) ackMessage(subName string, ackId string) error {

	var err error

	ack := AckMsgs{AckIDS: []string{ackId}}

	ackB, err := json.Marshal(ack)

	if err != nil {
		return err
	}

	url := fmt.Sprintf("https://%v/v1/projects/%v/subscriptions/%v:acknowledge?key=%v", ams.Endpoint, ams.Project, subName , ams.Token)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(ackB))

	if err != nil {
		return err
	}

	t1 := time.Now()
	resp, err := ams.Client.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {

		buf := bytes.Buffer{}
		buf.ReadFrom(resp.Body)
		err = errors.New(fmt.Sprintf("Could not acknowledge message with ackID: %v. AMS responded with %v in %v", ackId, buf.String(), time.Since(t1).String()))

	}

	return err

}

func (ams *AMSClient) Push(subName string) error {

	var err error

	// load thr subscription from ams
	sub, err := ams.loadSubscription(subName)

	if err != nil {
		return err
	}

	// pull messages
	recM, err := ams.pullMsg(subName)

	if err != nil {
		return err
	}

	// check if there are new messages
	if len(recM.RecMsgs) == 0 {
		log.Info("No new messages")
		return err
	}

	for _,msg := range recM.RecMsgs {

		// publish the message to the endpoint
		pMsg := PushMsg{Msg: msg.Msg, Sub:subName}
		err = ams.publish(pMsg, sub.PushCfg.Pend)

		if err != nil {
			return err

		}

		err := ams.ackMessage(subName, msg.AckID)
		if err != nil {
			return err
		}

	}

	return err
}