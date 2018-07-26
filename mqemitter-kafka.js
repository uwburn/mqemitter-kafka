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
var Qlobber = require('qlobber').Qlobber

var qlobberOpts = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#'
}

function MQEmitterKafka (opts) {
  if (!(this instanceof MQEmitterKafka)) {
    return new MQEmitterKafka(opts)
  }

  opts = opts || {}

  if (!opts.id) {
    throw new Error('Emitter id is mandatory')
  }

  this._opts = opts

  var that = this

  this._emitterMatcher = new Qlobber(qlobberOpts)

  var emitters = {}

  this._cache = LRU({
    max: 10000,
    maxAge: 60 * 1000 // one minute
  })

  this._ready = false
  this.status = new EE()

  var presenceReady = false
  var controlReady = false

  debug('Opening presence write stream')
  this._producerBrokerStream = Kafka.createWriteStream({
    'metadata.broker.list': 'localhost:9092',
    'client.id': 'mqemitter-presence-producer_' + this._opts.id
  }, {}, {
    topic: 'mqemitter-presence'
  })
  debug('Opening presence read stream')
  this._consumerBrokerStream = Kafka.createReadStream({
    'metadata.broker.list': 'localhost:9092',
    'client.id': 'mqemitter-presence-consumer_' + this._opts.id,
    'group.id': 'mqemitter-presence-consumer_' + this._opts.id,
    'socket.keepalive.enable': true
  }, {
    'auto.offset.reset': 'earliest'
  }, {
    topics: 'mqemitter-presence',
    waitInterval: 0,
    objectMode: false
  })
  this._consumerBrokerStream.consumer.seek({ topic: 'mqemitter-presence', partition: 0, offset: 0 }, 0, noop)

  debug('Opening control write stream')
  this._produceControlStream = Kafka.createWriteStream({
    'metadata.broker.list': 'localhost:9092',
    'client.id': 'mqemitter-control-producer_' + this._opts.id
  }, {}, {
    topic: 'mqemitter-control'
  })
  debug('Opening control read stream')
  this._consumerControlStream = Kafka.createReadStream({
    'metadata.broker.list': 'localhost:9092',
    'client.id': 'mqemitter-control-consumer_' + this._opts.id,
    'group.id': 'mqemitter-control-consumer_' + this._opts.id,
    'socket.keepalive.enable': true
  }, {
    'auto.offset.reset': 'earliest'
  }, {
    topics: 'mqemitter-control',
    waitInterval: 0,
    objectMode: false
  })
  this._consumerControlStream.consumer.seek({ topic: 'mqemitter-control', partition: 0, offset: 0 }, 0, noop)

  debug('Opening message read stream')
  this._consumerMessageStream = Kafka.createReadStream({
    'metadata.broker.list': 'localhost:9092',
    'client.id': 'mqemitter-message-consumer_' + this._opts.id,
    'group.id': 'mqemitter-message-consumer_' + this._opts.id,
    'socket.keepalive.enable': true
  }, {
    'auto.offset.reset': 'earliest'
  }, {
    topics: 'mqemitter-message_' + this._opts.id,
    waitInterval: 0,
    objectMode: false
  })

  this._emitKafka = function (msg) {
    var packet = {
      id: hyperid(),
      msg: msg
    }

    debug('Emitting packet for topic ' + msg.topic)

    var emitterIds = that._emitterMatcher.match(msg.topic)
    var emitterId
    for (emitterId of emitterIds) {
      debug('Emitting packet with id ' + packet.id + ' to ' + emitterId)
      emitters[emitterId].producerMessageStream.write(msgpack.encode(packet))
    }
  }

  this._emit = MQEmitter.prototype.emit
  this._on = MQEmitter.prototype.on
  this._removeListener = MQEmitter.prototype.removeListener

  function heartbeat () {
    this._producerBrokerStream.write(msgpack.encode({
      action: 'heartbeat',
      source: this._opts.id,
      time: new Date().getTime()
    }))
  }

  function ensureEmitter (id) {
    if (!emitters[id]) {
      emitters[id] = {
        producerMessageStream: Kafka.createWriteStream({
          'metadata.broker.list': 'localhost:9092',
          'client.id': 'mqemitter-control-producer_' + opts.id + '_' + id
        }, {}, {
          topic: 'mqemitter-message_' + id
        }),
        time: new Date().getTime()
      }
    }

    return emitters[id]
  }

  function ready () {
    if (!presenceReady) {
      return
    }

    if (!controlReady) {
      return
    }

    debug('Ready')
    this._ready = true
    this.status.emit('ready')
  }

  this._producerBrokerStream.write(msgpack.encode({ action: 'init' }))
  this._produceControlStream.write(msgpack.encode({ action: 'init' }))
  heartbeat.call(that)
  that._heartbeatInterval = setInterval(heartbeat.bind(that), 10000)

  pump(that._consumerBrokerStream, through.obj(processPresence), function () {
    if (that.closed) {
      return
    }

    that.status.emit('error', new Error('Error receiving data from Kafka'))
  })

  function processPresence (data, enc, cb) {
    if (that.closed) {
      return cb()
    }

    if (!that._ready) {
      presenceReady = true
      ready.call(that)
    }

    var packet = msgpack.decode(data)

    switch (packet.action) {
      case 'heartbeat':
        debug('Presence heartbeat message received from ' + packet.source)
        break
      case 'init':
        debug('Presence init message received')
        break
    }

    cb()
  }

  pump(that._consumerControlStream, through.obj(processControl), function () {
    if (that.closed) {
      return
    }

    that.status.emit('error', new Error('Error receiving data from Kafka'))
  })

  function processControl (data, enc, cb) {
    if (that.closed) {
      return cb()
    }

    if (!that._ready) {
      controlReady = true
      ready.call(that)
    }

    var packet = msgpack.decode(data)

    switch (packet.action) {
      case 'on':
        debug('Adding emitter ' + packet.source + ' for topic ' + packet.topic)
        ensureEmitter(packet.source)
        that._emitterMatcher.add(packet.topic, packet.source)
        break
      case 'removeListener':
        debug('Removing emitter ' + packet.source + ' for topic ' + packet.topic)
        that._emitterMatcher.remove(packet.topic, packet.source)
        break
      case 'init':
        debug('Control init message received')
        break
      default:
        debug('Unknown action')
    }

    cb()
  }

  pump(that._consumerMessageStream, through.obj(processMessage), function () {
    if (that.closed) {
      return
    }

    that.status.emit('error', new Error('Error receiving data from Kafka'))
  })

  that.status.emit('stream')

  function processMessage (data, enc, cb) {
    if (that.closed) {
      return cb()
    }

    var packet = msgpack.decode(data)

    // Deduplication
    if (!that._cache.get(packet.id)) {
      debug('Relaying local emit of packet with id ' + packet.id)
      that._emit(packet.msg, cb)
    }
    that._cache.set(packet.id, true)
  }

  MQEmitter.call(that, opts)
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
    try {
      this._emitKafka(msg)
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

MQEmitterKafka.prototype.on = function (topic, cb, done) {
  if (!this._ready) {
    this.status.once('ready', this.on.bind(this, topic, cb, done))
    return
  }

  debug('On ' + topic)
  this._produceControlStream.write(msgpack.encode({
    action: 'on',
    topic: topic,
    source: this._opts.id
  }))

  if (done) {
    setImmediate(done)
  }

  // cb()
  this._on(topic, cb)

  return this
}

MQEmitterKafka.prototype.removeListener = function (topic, cb, done) {
  if (!this._ready) {
    this.status.once('ready', this.removeListener.bind(this, topic, cb, done))
    return
  }

  this._produceControlStream.write(msgpack.encode({
    action: 'removeListener',
    topic: topic,
    source: this._opts.id
  }))

  if (done) {
    setImmediate(done)
  }

  // cb()
  this._removeListener(topic, cb)

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
