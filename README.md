# mqemitter-kafka

Kafka backed [MQEmitter](http://github.com/mcollina/mqemitter) using [KafkaJS](https://kafka.js.org/).

See [MQEmitter](http://github.com/mcollina/mqemitter) for the actual API.

_**NOTE**: preliminary version, WIP._

## Install

```bash
npm install mqemitter-kafka --save
```

or

```bash
yarn add mqemitter-kafka
```

## Example

```js
const MqemitterKafka = require("mqemitter-mongodb");

const mq = MqemitterKafka({
  kafka: {
    brokers: ["localhost:9092"]
  }
});

const msg  = {
  topic: "hello world",
  payload: "or any other fields"
};

mq.on("hello world", function (message, cb) {
  // call callback when you are done
  // do not pass any errors, the emitter cannot handle it.
  cb();
});

// topic is mandatory
mq.emit(msg, function () {
  // emitter will never return an error
});
```

## API

### MQEmitterKafka([opts])

Create a new instance of mqemitter-kafka.

Options:

* `topic`: the Kafka topic to use, defaults to `mqemitter`
* `localEmitCheck`: a function to check if must fallback to "local" [MQEmitter](http://github.com/mcollina/mqemitter), defaults to `() => false`
* `kafka`: options for [KafkaJS](https://kafka.js.org/) client (alternative to `client`)
* `client`: an instance of [KafkaJS](https://kafka.js.org/) client (alternative to `kafka`)
* `startDelay`: amount of milliseconds to wait after producer and consumer have completed startup before notfying started condition
* `closeDelay`: amount of milliseconds to wait before disconnecting producer and consumer when calling close method

If neither `kafka` or `client` option are supplied, connection to `localhost:9092` will be attempted.

**Supplying an external client it's recommended.**

### localEmitCheck(obj)

This is for use with [Aedes](https://github.com/moscajs/aedes): sometimes is useful to have some message distributed to other instances of Aedes connected through mqemitter-kafka, while having some others only local (think of MQTT topics handled by the broker directly, where client only publish and nobody subscribes).

The function argument is the message object, returning `true` will avoid producing the message on Kafka, falling back on a "local" [MQEmitter](http://github.com/mcollina/mqemitter), returning false will keep the normal behaviour.

### Events

The mqemitter-kafka instance has a `status` property, holding a Node.JS EventEmitter exposing the following events:

* `started`
* `error`

### Kafka error handling

Errors on the producer bubble up in the callback chain and are exposed through `status` `error` event.

Error handling on the consumer must be handled externally of the mqemitter-kafka instance. Please see [KafkaJS client options](https://kafka.js.org/docs/configuration#options), [KafkaJS consumer options](https://kafka.js.org/docs/consuming#options) and [KafkaJS retry mechanism](https://kafka.js.org/docs/retry-detailed).

## Acknowledgements

Implementation inspired after [mqemitter-mongodb](https://github.com/mcollina/mqemitter-mongodb).

## License

MIT
