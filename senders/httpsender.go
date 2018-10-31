package sendersx

import (
	"bytes"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

type HttpSender struct {
	Endpoint string
	Client   http.Client
}

func NewHttpSender(endpoint string) *HttpSender {
	hs := new(HttpSender)
	hs.Endpoint = endpoint
	hs.Client = http.Client{Timeout: 3 * time.Second}
	return hs
}

func (hs *HttpSender) Send(msg string) error {

	var err error
	req, err := http.NewRequest("POST", hs.Endpoint, bytes.NewBuffer([]byte(msg)))
	req.Header.Set("Content-Type", "application/json")
	log.Infof("Sending message: %v to endpoint: %v", msg, hs.Endpoint)

	t1 := time.Now()
	resp, err := hs.Client.Do(req)

	if err == nil {

		if resp.StatusCode != 200 && resp.StatusCode != 201 && resp.StatusCode != 204 && resp.StatusCode != 102 {

			buf := bytes.Buffer{}
			buf.ReadFrom(resp.Body)
			defer resp.Body.Close()
			err = errors.New(fmt.Sprintf("Endpoint: %v responded with status code: %v and response: %v", hs.Endpoint, resp.StatusCode, buf.String()))
			return err
		}

		log.Infof("Message: %v to endpoint: %v delivered in: %v", msg, hs.Endpoint, time.Since(t1).String())

	} else {
		return err
	}

	return err

}
