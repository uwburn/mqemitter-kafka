'use strict'

var inherits = require('inherits')
var MQEmitter = require('mqemitter')
var through = require('through2')
var pump = require('pump')
var EE = require('events').EventEmitter
var Kafka = require('node-rdkafka')
var msgpack = require('msgpack-lite')
var LRU = require('lru-cache')
var hyperid = require('hyperid')()

function MQEmitterKafka (opts) {
  if (!(this instanceof MQEmitterKafka)) {
    return new MQEmitterKafka(opts)
  }

  opts = opts || {}

  this._opts = opts

  var that = this

  this._cache = LRU({
    max: 10000,
    maxAge: 60 * 1000 // one minute
  })

  this._started = false
  this.status = new EE()

  var producerReady = false
  var consumerReady = false

  this._producer = new Kafka.Producer({
    'metadata.broker.list': 'localhost:9092',
    'client.id': 'mqemitter-producer',
    'debug': 'all'
  })
  this._producer.on('ready', function () {
    producerReady = true
    waitStartup()
  })

  this._consumerStream = Kafka.createReadStream({
    'metadata.broker.list': 'localhost:9092',
    'client.id': 'mqemitter-consumer',
    'group.id': 'mqemitter',
    'socket.keepalive.enable': true,
    'debug': 'all'
  }, {}, {
    topics: 'mqemitter',
    waitInterval: 0,
    objectMode: false
  })

  this._consumer = this._consumerStream.consumer
  this._consumer.on('ready', function () {
    consumerReady = true
    waitStartup()
  })

  this._producer.connect()
  this._consumer.connect()

  function waitStartup () {
    if (!producerReady || !consumerReady) {
      return
    }

    start()
  }

  var oldEmit = MQEmitter.prototype.emit

  function start () {
    pump(that._consumerStream, through.obj(process), function () {
      if (that.closed) {
        return
      }

      if (that._started) {
        that.status.emit('error', new Error('Error receiving data from Kafka'))
      }
    })

    that.status.emit('stream')

    function process (data, enc, cb) {
      if (that.closed) {
        return cb()
      }

      var packet = msgpack.decode(data)

      if (!that._cache.get(packet.id)) {
        oldEmit.call(that, packet.msg, cb)
      }
      that._cache.set(packet.id, true)
    }
  }
  MQEmitter.call(this, opts)
}

inherits(MQEmitterKafka, MQEmitter)

MQEmitterKafka.prototype.emit = function (msg, cb) {
  if (this.closed) {
    var err = new Error('MQEmitterKafka is closed')
    if (cb) {
      cb(err)
    } else {
      throw err
    }
  } else {
    var packet = {
      id: hyperid(),
      msg: msg
    }

    try {
      this._producer.produce('mqemitter', -1, msgpack.encode(packet), packet.id)
      if (cb) {
        cb()
      }
    } catch (err) {
      if (cb) {
        cb(err)
      } else {
        throw err
      }
    }
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
    var consumerDisconnected = false
    var producerDisconnected = false

    that._producer.once('disconnected', function () {
      producerDisconnected = true
      waitClose()
    })
    that._producer.disconnect()

    that._consumer.disconnect()
    that._consumer.once('disconnected', function () {
      consumerDisconnected = true
      waitClose()
    })

    function waitClose () {
      if (!consumerDisconnected || !producerDisconnected) {
        return
      }

      cb()
    }
  })

  return this
}

function noop () { }

module.exports = MQEmitterKafka
