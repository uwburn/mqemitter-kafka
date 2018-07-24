'use strict'

var debug = require('debug')('mqemitter-kafka')
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

  debug('Opening write stream')
  this._producerStream = Kafka.createWriteStream({
    'metadata.broker.list': 'localhost:9092',
    'client.id': 'mqemitter-producer'
  }, {}, {
    topic: 'mqemitter'
  })

  debug('Opening read stream')
  this._consumerStream = Kafka.createReadStream({
    'metadata.broker.list': 'localhost:9092',
    'client.id': 'mqemitter-consumer',
    'group.id': 'mqemitter',
    'socket.keepalive.enable': true
  }, {}, {
    topics: 'mqemitter',
    waitInterval: 0,
    objectMode: false
  })

  start()

  var oldEmit = MQEmitter.prototype.emit

  function start () {
    that._started = true

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

      // Deduplication
      if (!that._cache.get(packet.id)) {
        debug('Relaying local emit of packet with id ' + packet.id)
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
      debug('Emitting packet with id ' + packet.id)
      this._producerStream.write(msgpack.encode(packet))
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
    that._producerStream.destroy()
    that._consumerStream.destroy()

    cb()
  })

  return this
}

function noop () { }

module.exports = MQEmitterKafka
