# BunnySheperd
A small, focused set of RabbitMQ helpers for Go, built on top of github.com/rabbitmq/amqp091-go. It provides:
- A resilient ConnectionManager with automatic reconnects and clean shutdown
- A confirm-aware Publisher with lazy channel reinit and backoff retries
- A QoS-configurable Consumer with handler timeouts and backoff
- A tiny model package with a generic JSONMessage envelope

These helpers aim to be pragmatic and production-friendly while keeping the API surface small and idiomatic.

## Installation
```bash
go get github.com/kubenetic/BunnySheperd
```

Import paths in examples use the module path from go.mod:
- github.com/kubenetic/BunnySheperd/pkg/rabbitmq
- github.com/kubenetic/BunnySheperd/pkg/model

## Requirements
- Go 1.25+
- A reachable RabbitMQ broker (tested with RabbitMQ 4.1+)

## Overview
Below are practical, copy-pasteable usage examples. They show how to:
- Create and manage a single connection with `ConnectionManager`
- Publish messages with confirmations and retries using `Publisher`
- Consume messages with backoff and QoS using `Consumer`
- Customize behavior via options (timeouts, prefetch, backoff)
- Use the generic `model.JSONMessage[T]` envelope for JSON payloads

All examples assume you have a live RabbitMQ broker and the `amqp091-go` dependency (already required by this module).

---

### ConnectionManager
`ConnectionManager` owns a single AMQP connection, monitors it, and auto-reconnects with exponential backoff. Create it 
once and share it across publishers/consumers.

```go
package main

import (
    "context"
    "log"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
    rmq "github.com/kubenetic/BunnySheperd/pkg/rabbitmq"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Use default AMQP config (nil) or provide a custom amqp.Config
    cm, err := rmq.NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
    if err != nil {
        log.Fatalf("failed to create connection manager: %v", err)
    }
    defer cm.Close() // signals the background watcher to stop

    // Example: borrow a channel (package-internal methods do this for you)
    // ch, err := cm.getChannel() // not exported; shown for context only
}
```

Notes:
- `NewConnectionManager` immediately connects and starts a background watcher.
- The manager reacts to `NotifyClose` and attempts reconnection with backoff.
- Call `Close()` to stop the watcher and cleanup.

---

### Publisher
`Publisher` opens a confirm-enabled channel, automatically reinitializes on channel close, and retries publishes with 
exponential backoff and jitter. It accepts a `model.Message` envelope (see `JSONMessage` below).

#### Basic publish with defaults
```go
package main

import (
    "context"
    "log"
    "time"

    rmq "github.com/kubenetic/BunnySheperd/pkg/rabbitmq"
    "github.com/kubenetic/BunnySheperd/pkg/model"
)

func main() {
    ctx := context.Background()
    
    cm, err := rmq.NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
    if err != nil { 
        log.Fatal(err) 
    }
    defer cm.Close()

    pub, err := rmq.NewPublisher(cm)
    if err != nil { 
        log.Fatal(err) 
    }
    defer pub.Close()

    // Build a JSON envelope for your payload
    payload := Job{
        JobID: "job-123", 
        Files: []string{"/tmp/a", "/tmp/b"},
    }
    msg := &model.JSONMessage[Job]{
        Payload: payload,
    }

    // Exchange and routingKey must exist/bindings configured
    publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()

    if err := pub.Publish(publishCtx, "scan.jobs.x", "jobs", true, msg); err != nil {
        log.Fatalf("publish failed: %v", err)
    }

    log.Println("published ok")
}
```

Key behaviors:
- Confirms: waits for ack/nack with `ConfirmTimeout`. Retries on nack/timeout.
- Channel close: detects `amqp.ErrClosed` and lazily reinitializes the channel before retrying.
- `mandatory=true` will cause unroutable messages to be returned; see next section.

#### Handling returned (unroutable) messages
```go
pub, err := rmq.NewPublisher(cm)
if err != nil { 
    log.Fatal(err) 
}

pub.OnReturn = func(ctx context.Context, ret amqp.Return) error {
    // This callback runs with a 10s timeout.
    log.Printf("unroutable: exchange=%s key=%s code=%d text=%s", 
        ret.Exchange, ret.RoutingKey, ret.ReplyCode, ret.ReplyText)
    return nil
}

msg := &model.JSONMessage[string]{Payload: "hello"}
if err := pub.Publish(context.Background(), "my.exchange", "missing.binding", true, msg); err != nil {
    log.Printf("publish returned error: %v", err)
}
```

#### Customizing publisher behavior
```go
pub, err := rmq.NewPublisher(
    cm,
    rmq.WithMaxRetries(3),                 // default 5
    rmq.WithInitialBackoff(300*time.Millisecond),
    rmq.WithConfirmTimeout(3*time.Second), // default 5s
)
if err != nil { log.Fatal(err) }
```

---

### Consumer
`Consumer` manages a consumer channel with QoS and exposes a `Subscribe` method that handles backoff on errors in your 
handler. You provide a `MessageHandler` that processes `amqp.Delivery`.

#### Basic subscribe
```go
package main

import (
    "context"
    "log"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
    rmq "github.com/kubenetic/BunnySheperd/pkg/rabbitmq"
)

func main() {
    ctx := context.Background()
    
    cm, err := rmq.NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
    if err != nil { 
        log.Fatal(err) 
    }
    defer cm.Close()

    consumer, err := rmq.NewConsumer(cm)
    if err != nil { 
        log.Fatal(err) 
    }
    defer consumer.Close()

    handler := func(hCtx context.Context, d amqp.Delivery) error {
        log.Printf("got msg id=%s corr=%s body=%s", d.MessageId, d.CorrelationId, string(d.Body))
        // Acknowledge on success
        if err := d.Ack(false); err != nil {
            return err
        }
        return nil
    }

    // Subscribe blocks until the context is done or consumption stops
    subCtx, cancel := context.WithCancel(ctx)
    defer cancel()

    if err := consumer.Subscribe(subCtx, "scan.jobs.q", "<consumer-tag>", handler); err != nil {
        log.Fatalf("subscribe failed: %v", err)
    }
}
```

Notes:
- QoS prefetch defaults to `1`. Use `WithPrefetchCount(n)` to change.
- The handler runs with a time-bounded context (default `30s`). You can override via `WithMessageHandlerTimeout`.
- On handler error, the consumer uses backoff (default initial `500ms`, max `10s`). Customize with `WithBackoff`.

#### Customizing consumer behavior
```go
consumer, err := rmq.NewConsumer(
    cm,
    rmq.WithPrefetchCount(5),
    rmq.WithMessageHandlerTimeout(45*time.Second),
    rmq.WithBackoff(300*time.Millisecond, 8*time.Second),
)
if err != nil { log.Fatal(err) }
```

---

### Message envelopes with model.JSONMessage
The publisher expects a `model.Message` with metadata and a payload as bytes. `model.JSONMessage[T]` is a convenient, 
generic envelope that marshals the payload to JSON.

```go
import (
    "github.com/kubenetic/BunnySheperd/pkg/model"
    amqp "github.com/rabbitmq/amqp091-go"
)

// Build a JSON message with typed payload
type Job struct{
    JobId string `json:"job_id"`
    Files []string `json:"files"`
}

m := &model.JSONMessage[Job]{
    Payload: Job{
        JobId: "job-123", 
        Files: []string{"/tmp/a"},
    },
    // Optional: customize message metadata
    Headers:     amqp.Table{"x-source": "scanner"},
    ContentType: "application/json", // default if empty
    // MessageId and CorrelationId are auto/derived if empty
}

// Accessors used by the publisher under the hood:
_ = m.GetMessageId()       // auto-generates UUID if empty
_ = m.GetCorrelationId()   // falls back to MessageId if empty
_ = m.GetHeaders()
_ = m.GetContentType()     // defaults to application/json
_, _ = m.GetPayload()      // JSON-encodes Payload
```

---

### Putting it all together: worker-like flow
This mirrors patterns in `pkg/service/scanner/workerpool.go`.

```go
ctx := context.Background()
cm, err := rmq.NewConnectionManager(ctx, "amqp://guest:guest@localhost:5672/", nil)
if err != nil { 
    log.Fatal(err) 
}
defer cm.Close()

consumer, err := rmq.NewConsumer(cm)
if err != nil { 
    log.Fatal(err) 
}
defer consumer.Close()

publisher, err := rmq.NewPublisher(cm)
if err != nil { 
    log.Fatal(err) 
}
defer publisher.Close()

handleJob := func(hCtx context.Context, d amqp.Delivery) error {
    // 1) Deserialize job
    var job struct {
        JobId string `json:"job_id"`
        Files []string `json:"files"`
    }
    if err := json.Unmarshal(d.Body, &job); err != nil {
        _ = d.Nack(false, false)
        return err
    }

    // 2) Do work ...

    // 3) Publish result
    res := &model.JSONMessage[any]{Payload: map[string]any{"job_id": job.JobId, "ok": true}}
    if err := publisher.Publish(hCtx, "scan.results.x", "", true, res); err != nil {
        // Republish/backoff will happen inside; if it still fails, decide your ack strategy
        _ = d.Nack(false, true) // requeue
        return err
    }

    // 4) Ack job
    return d.Ack(false)
}

if err := consumer.Subscribe(ctx, "scan.jobs.q", "<consumer-tag>", handleJob); err != nil {
    log.Fatal(err)
}
```

---

### Error types you may see
- `rabbitmq.ErrConnectionNotInitilized`: connection not yet established when creating a channel
- `rabbitmq.ErrConnectionClosed`: connection closed when creating a channel
- `rabbitmq.ErrNacked`: publish not confirmed (nack)
- `rabbitmq.ErrConfirmTimeout`: publish confirm wait timed out
- `rabbitmq.ErrChannelReinitBackoffExceed`: internal limit while trying to reinit a channel
- `rabbitmq.ErrRepublishBackoffExceed`: internal limit while retrying publish

---

### Tips and best practices
- Create one `ConnectionManager` per process and share it.
- Create separate `Publisher` and `Consumer` per logical component; they manage their own channels.
- Always set timeouts/deadlines on publish/subscribe contexts.
- Consider `mandatory=true` during migration/testing so you catch unroutable messages via `OnReturn`.
- Tune `prefetch` and handler timeout to your workload; too high prefetch can starve fair dispatch.
- Ensure exchanges, queues, and bindings are declared upfront (this package does not auto-declare them).
