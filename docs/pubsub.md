# Pubsub

Quado uses https://github.com/igorshapiro/hubjs for pub sub

To be able to publish message use `pubsub` dependency.

Add following config to service to enable pull model of pubsub add a service.yml file with following configuration to config folder

```yaml
name: 'service_name'
hub: #Optional
  host: 'http://hub:8080' #Optional
messages:
  host: 'amqp://localhost' #May be an array of string urls or url objects, the one with the shortest ping is chosen
  input: 'service_in' #Optional default value is `${name}_in`
  output: 'service_out' #Optional default value is `${name}_out`
  concurrency: 10 #Optional default value is 10
storage:
  host: 'mongodb://localhost:27017/hub'
  schedule: 'service_schedule' #Optional default value is `${name}_schedule`
  dead: 'service_dead_v2' #Optional default value is `${name}_dead_v2`
retrySchedule: [ 1000, 6000 , 60000 ] #Optional retries frequency   
```

# Implementation Notes
Quadro uses a connection manager to ensure the underlying AMQP connection is re-established automatically in the event of a connection failure. To do this, there is an internal queue within the connection manager which holds messages until they can be sent to the server. To prevent overflowing this queue (and to ensure backpressure is felt) you should always await `publish` calls which will not resolve until the message has been sent successfully and acknowledged by the server.

# API

## .publish(messageType, messageContent)
publishes a message to rabbitmq

## Receive Message

To receive messages add a handler class that implements handle method to the handler folder with the name `${messageType}.js`

Quadro will automatically picks the handler on startup and registers it. It will receive all the messages from the input queue and routes the message to appropriate handler based on messageType

Sample handler code:

```js
module.exports = function Handler(log) {
  this.handle = async function(ctx) {
    // Handle the message here
  }
}
```

Handlers are passed the message context

### Message context

#### .message

Contains the message content

#### .failure(msg, code)

Fails message handling, triggering either message rescheduling, or moving it
to the dead letter collection

#### .ignore(msg)

Ignores the message - acknowledges the message as if it was successfully handled.

#### .success()

Acknowledges the message handling, marking it handled

#### .retryAfterSec(seconds)

Reschedules the message to be delivered after `seconds` interval

### .willRetry()

Return true if pubsub will retry this message at later state. Returns false if
this is final retry.
