package main

import (
	"crypto/tls"
	"flag"
	"log"
	"net/http"
	"time"
)

func main() {

	var project = flag.String("project", "", "AMS project")
	var sub = flag.String("sub", "", "AMS subscription")
	var host = flag.String("host", "", "AMS host")
	var token = flag.String("token", "", "AMS token")
	var pollRate = flag.Int64("poll", 0, "Poll rate")
	var remoteEndpoint = flag.String("endpoint", "", "Remote endpoint url")
	var remoteEndpointAuthHeader = flag.String("auth", "", "Remote endpoint expected authorization header")

	flag.Parse()

	// build the client and execute the request
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: transCfg, Timeout: time.Duration(30 * time.Second)}
	ams := AMSClient{Endpoint: *host, Project: *project, Token: *token, Client: client}

	for {
		err := ams.Push(*sub, *remoteEndpoint, *remoteEndpointAuthHeader)

		if err != nil {
			log.Printf("ERROR: %v", err)
		}

		time.Sleep(time.Duration(*pollRate) * time.Second)
	}

}
