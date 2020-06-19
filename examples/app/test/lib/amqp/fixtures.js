// https://github.com/benbria/node-amqp-connection-manager/blob/master/test/fixtures.js

const { EventEmitter } = require('events')
const sinon = require('sinon')

class FakeAmqp {
  constructor() { this.reset() }

  kill() {
    const err = new Error('Died in a fire')
    this.connection.emit('error', err)
    this.connection.emit('close', err)
  }

  simulateRemoteClose() {
    this.connection.emit('close', new Error('Connection closed'))
  }

  simulateRemoteBlock() {
    this.connection.emit('blocked', new Error('Connection blocked'))
  }

  simulateRemoteUnblock() {
    this.connection.emit('unblocked')
  }

  reset() {
    this.connection = null
    this.url = null
    this.failConnections = false
    this.deadServers = []
    this.connect = sinon.spy(url => {
      if (this.failConnections) {
        return Promise.reject(new Error('No'))
      }

      let allowConnection = true
      this.deadServers.forEach(deadUrl => {
        if (url.startsWith(deadUrl)) {
          allowConnection = false
        }
      })
      if (!allowConnection) {
        return Promise.reject(new Error(`Dead server ${url}`))
      }

      this.connection = new FakeConnection(url)
      return Promise.resolve(this.connection)
    })
  }
}

class FakeConfirmChannel extends EventEmitter {
  constructor() {
    super()
    this.publish = sinon.spy((exchange, routingKey, content, options, callback) => {
      console.log('publish')
      this.emit('publish', { exchange, routingKey, content, options })
      callback(null)
      return true
    })

    this.sendToQueue = sinon.spy((queue, content, options, callback) => {
      console.log('sendToQueue')
      this.emit('sendToQueue', { queue, content, options })
      callback(null)
      return true
    })

    this.ack = sinon.spy(function(message, allUpTo) {})
    this.ackAll = sinon.spy(function() {})
    this.nack = sinon.spy(function(message, allUpTo, requeue) {})
    this.nackAll = sinon.spy(function(requeue) {})
    this.assertQueue = sinon.spy(function(queue, options) {})
    this.bindQueue = sinon.spy(function(queue, source, pattern, args) {})
    this.assertExchange = sinon.spy(function(exchange, type, options) {})

    this.close = sinon.spy(() => this.emit('close'))
  }
}

class FakeConnection extends EventEmitter {
  constructor(url) {
    super()
    this.url = url
    this._closed = false
  }

  createConfirmChannel() {
    return Promise.resolve(new FakeConfirmChannel())
  }

  close() {
    this._closed = true
    return Promise.resolve()
  }
}

class FakeAmqpConnectionManager extends EventEmitter {
  constructor() {
    super()
    this.connected = false
    this._channels = []
  }

  isConnected() {
    return this.connected
  }

  simulateConnect() {
    const url = 'amqp://localhost'
    this._currentConnection = new FakeConnection(url)
    this.connected = true
    this.emit('connect', {
      connection: this._currentConnection,
      url
    })
  }

  simulateDisconnect() {
    this._currentConnection = null
    this.connected = false
    this.emit('disconnect', {
      err: new Error(('Boom!'))
    })
  }
}

module.exports = {
  FakeAmqp,
  FakeConfirmChannel,
  FakeConnection,
  FakeAmqpConnectionManager
}