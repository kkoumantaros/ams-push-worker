# go-push-worker

### How to run

#### Arguments

`--host:` AMS host and ams port

`--token:` AMS token

`--sub`: AMS subscription

`--project:` AMS project that the subscription belongs to

`--endpoint:` Remote endpoint that we expect to receive messages to

`--poll:` How often should the worker poll ams for new messages

`--auth:` Expected Authorization header value from the remote endpoint

#### Example

```
./go-push-worker
 --host 127.0.0.1:8080
 --token b328c3861f061f87cbd34cf34f36ba2ae20883a5
 --sub demo-sub
 --project ps-demo-project
 --endpoint  https://192.168.1.6:5000/receive_here
 --poll 2
 --auth tok3n
```