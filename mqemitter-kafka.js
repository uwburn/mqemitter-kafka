"use strict";

const { Partitioners, Kafka } = require("kafkajs");
const inherits = require("inherits");
const MQEmitter = require("mqemitter");
const EE = require("events").EventEmitter;
const uuidv4 = require("uuid").v4;

const oldEmit = MQEmitter.prototype.emit;

function noop() {}

function falsy() {
  return false;
}

function MQEmitterKafka(opts) {
  if (!(this instanceof MQEmitterKafka)) {
    return new MQEmitterKafka(opts);
  }

  const id = `mqemiter_${uuidv4()}`;

  opts = opts || {};
  opts.topic = opts.topic || "mqemitter";
  opts.localEmitCheck = opts.localEmitCheck || falsy;
  opts.closeDelay = opts.closeDelay || 100;

  this.status = new EE();
  this.status.setMaxListeners(0);
  this.status.on("error", () => {});

  this._opts = opts;

  if (opts.client) {
    this._kafka = opts.client;
  }
  else {
    const defaultOpts = { clientId: id, brokers: ["localhost:9092"] };
    const kafkaOpts = this._opts.kafka ? Object.assign(defaultOpts, this._opts.kafka) : defaultOpts;

    this._kafka = new Kafka(kafkaOpts);
  }

  (async () => {
    this._started = false;

    this._producer = this._kafka.producer({ createPartitioner: Partitioners.JavaCompatiblePartitioner });
    await this._producer.connect();

    this._consumer = this._kafka.consumer({
      groupId: id
    });

    await this._consumer.connect();

    await this._consumer.subscribe({ topic: this._opts.topic });

    await this._consumer.run({
      eachMessage: async (payload) => {
        if (this._closed) {
          return;
        }

        const msg = JSON.parse(payload.message.value);

        const obj = msg.obj;
        if (msg.payloadType == "BUFFER") {
          obj.payload = Buffer.from(obj.payload, "base64");
        }

        oldEmit.call(this, obj);
      }
    });

    this._started = true;

    this.status.emit("started");
  })();

  this._queue = [];

  MQEmitter.call(this, opts);
}

inherits(MQEmitterKafka, MQEmitter);

function invokeCallbacks(queue, err) {
  for (const d of queue) {
    if (d.cb) {
      d.cb(err);
    }
  }
}

function buildKafkaMessage(obj) {
  let payload = obj.payload;
  let payloadType = "JSON";

  let cpy;
  if (Buffer.isBuffer(payload)) {
    payload = payload.toString("base64");
    payloadType = "BUFFER";

    cpy = Object.assign({}, obj);
    cpy.payload = payload;
  }
  else {
    cpy = obj;
  }

  return {
    key: obj.topic,
    value: JSON.stringify({
      payloadType,
      obj: cpy
    })
  };
}

MQEmitterKafka.prototype._processQueue = async function() {
  if (this._producing || this._queue.length <= 0) {
    return;
  }

  this._producing = true;

  const length = this._queue.length;
  const kafkaMessages = [];
  const localMessages = [];

  for (let i = 0; i < length; ++i) {
    const e = this._queue[i];

    if (this._opts.localEmitCheck(e.obj)) {
      localMessages.push(e);
    }
    else {
      e.kafkaMessage = buildKafkaMessage(e.obj);
      kafkaMessages.push(e);
    }
  }

  if (kafkaMessages.length > 0) {
    try {
      await this._producer.send({
        topic: this._opts.topic,
        messages: kafkaMessages.map(e => e.kafkaMessage)
      });

      invokeCallbacks(kafkaMessages, null);
    }
    catch (err) {
      this.status.emit("error", err);

      invokeCallbacks(kafkaMessages, err);
    }
  }

  for (const e of localMessages) {
    oldEmit.call(this, e.obj, e.cb);
  }

  this._queue.splice(0, length);

  this._producing = false;
  this._processQueue();
};

MQEmitterKafka.prototype.emit = function(obj, cb) {
  if (!this.closed && !this._started) {
    this.status.once("started", this.emit.bind(this, obj, cb));
    return this;
  }

  if (this.closed) {
    if (cb) {
      cb(new Error("MQEmitterKafka is closed"));
    }
  }
  else {
    this._queue.push({ obj, cb });
    this._processQueue();
  }

  return this;
};

MQEmitterKafka.prototype.close = function(cb) {
  cb = cb || noop;

  if (this.closed) {
    return cb();
  }

  if (!this._started) {
    this.status.once("started", this.close.bind(this, cb));
    return;
  }

  setTimeout(async () => {
    await this._consumer.disconnect();
    await this._producer.disconnect();

    this.closed = true;

    cb();
  }, this._opts.closeDelay);

  return this;
};

module.exports = MQEmitterKafka;
