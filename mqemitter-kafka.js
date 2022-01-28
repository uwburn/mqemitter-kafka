"use strict";

const Kafka = require("kafkajs").Kafka;
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

  this.status = new EE();
  this.status.setMaxListeners(0);
  this.status.on("error", () => {});

  this._opts = opts;

  let that = this;

  if (opts.client) {
    that._kafka = opts.client;
  }
  else {
    let defaultOpts = { clientId: id, brokers: ["localhost:9092"] };
    let kafkaOpts = that._opts.kafka ? Object.assign(defaultOpts, that._opts.kafka) : defaultOpts;

    that._kafka = new Kafka(kafkaOpts);
  }
  setImmediate(waitStartup);

  this._started = false;

  async function waitStartup() {
    that._producer = that._kafka.producer();
    await that._producer.connect();

    that._consumer = that._kafka.consumer({ groupId: id });
    await that._consumer.connect();

    start();
  }

  this._waiting = new Map();
  this._queue = [];
  this._executingBulk = false;

  async function start() {
    await that._consumer.subscribe({ topic: that._opts.topic });

    await that._consumer.run({
      eachMessage: async (payload) => {
        let msg = JSON.parse(payload.message.value);

        let obj = msg.obj;
        if (msg.payloadType == "BUFFER") {
          obj.payload = Buffer.from(obj.payload, "base64");
        }

        oldEmit.call(that, obj);
      }
    });

    that._started = true;

    that.status.emit("started");
  }

  MQEmitter.call(this, opts);
}

inherits(MQEmitterKafka, MQEmitter);

MQEmitterKafka.prototype._write = async function(obj, cb) {
  try {
    if (this._opts.localEmitCheck(obj)) {
      oldEmit.call(this, obj, cb);
    }
    else {
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

      await this._producer.send({
        topic: this._opts.topic,
        messages: [
          {
            key: obj.topic,
            value: JSON.stringify({
              payloadType,
              obj: cpy
            })
          }
        ]
      });

      if (cb) {
        cb();
      }
    }
  }
  catch (err) {
    this.status.emit("error", err);

    if (cb) {
      cb(err);
    }
  }
};

MQEmitterKafka.prototype.emit = function(obj, cb) {
  if (!this.closed && !this._started) {
    // actively poll if Kafka is ready
    this.status.once("started", this.emit.bind(this, obj, cb));
    return this;
  }
  else if (this.closed) {
    let err = new Error("MQEmitterKafka is closed");
    if (cb) {
      cb(err);
    }
  }
  else {
    this._write(obj, cb);
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
  }, this._opts.delayClose);

  return this;
};

module.exports = MQEmitterKafka;
