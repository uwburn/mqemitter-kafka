'use strict'

var inherits = require('inherits')
var MQEmitter = require('mqemitter')
var through = require('through2')
var pump = require('pump')
var nextTick = process.nextTick
var EE = require('events').EventEmitter
var Kafka = require("kafka-node");
var msgpack = require("msgpack-lite")

function MQEmitterKafka(opts) {
  if (!(this instanceof MQEmitterKafka)) {
    return new MQEmitterKafka(opts)
  }

  opts = opts || {}

  this._opts = opts

  var that = this

  this._producerClient = new Kafka.Client("localhost:2181", "mqemitter-producer")
  this._producer = new Kafka.Producer(this._producerClient)
  this._producer.once("ready", waitStartup)

  this._consumerClient = new Kafka.Client("localhost:2181", "mqemitter-consumer")
  this._consumer = new Kafka.ConsumerStream(this._consumerClient, [{ topic: "mqemitter" }], { encoding: "buffer" })

  this._started = false
  this.status = new EE()

  function waitStartup() {
    that._producer.createTopics(["mqemitter"], false, function (err) {
      if (err)
        return that.status.emit('error', err)

      start();
    });
  }

  var oldEmit = MQEmitter.prototype.emit

  this._waiting = {}

  function start() {
    pump(that._consumer, through.obj(process), function () {
      if (that.closed) {
        return
      }

      if (that._started) {
        that.status.emit('error', new Error('Error receiving data from Kafka'))
      }
    })

    that.status.emit('stream')

    function process(obj, enc, cb) {
      if (that.closed) {
        return cb()
      }

      var packet = msgpack.decode(obj.value)

      that._started = true
      that._lastId = obj._id
      oldEmit.call(that, packet, cb)

      var id = obj.offset
      if (that._waiting[id]) {
        nextTick(that._waiting[id])
        delete that._waiting[id]
      }
    }
  }
  MQEmitter.call(this, opts)
}

inherits(MQEmitterKafka, MQEmitter)

MQEmitterKafka.prototype.emit = function (packet, cb) {
  var that = this
  var err

  if (this.closed) {
    err = new Error('MQEmitterKafka is closed')
    if (cb) {
      cb(err)
    } else {
      throw err
    }
  } else {
    this._producer.send([{
      topic: "mqemitter",
      messages: [msgpack.encode(packet)],
      key: packet.topic
    }], function (err, data) {
      if (cb) {
        if (err)
          return cb(err)

        var id = data.mqemitter[0];
        if (id > that._lastId) {
          that._waiting[id] = cb
        } else {
          cb()
        }
      }
    })
  }
  return this
}

MQEmitterKafka.prototype.close = function (cb) {
  cb = cb || noop

  if (this.closed) {
    return cb()
  }

  this.closed = true

  var that = this
  MQEmitter.prototype.close.call(this, function () {
    async.series([
      function (callback) {
        that._consumer.removeTopics(["mqemitter"], callback)
      },
      that._consumer.close,
      that._producer.close,
      that._consumerClient.close,
      that._producerClient.close
    ], cb)
  })

  return this
}

function noop() { }

module.exports = MQEmitterKafka