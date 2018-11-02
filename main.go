package main

import (
	log "github.com/sirupsen/logrus"
	lSyslog "github.com/sirupsen/logrus/hooks/syslog"
	"log/syslog"
	"flag"
	"net/http"
	"crypto/tls"
	"time"
)

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true, DisableColors: true})
	hook, err := lSyslog.NewSyslogHook("", "", syslog.LOG_INFO, "")
	if err == nil {
		log.AddHook(hook)
	}
}

func main() {

	var project = flag.String("project", "", "AMS project")
	var sub = flag.String("sub", "", "AMS subscription")
	var host = flag.String("host", "", "AMS host")
	var token = flag.String("token", "", "AMS token")
	var pollRate = flag.Int64("poll", 0, "Poll rate")

	flag.Parse()

	// build the client and execute the request
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: transCfg, Timeout: time.Duration(30 * time.Second)}
	ams := AMSClient{Endpoint: *host, Project: *project, Token: *token, Client: client}

	for {
		err := ams.Push(*sub)

		if err != nil {
			log.Error(err)
		}

		time.Sleep(time.Duration(*pollRate) * time.Second)
	}

}