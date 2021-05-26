const { EventEmitter } = require('events')

const PASSTHROUGH_FUNCTIONS = ['ack', 'ackAll', 'nack', 'nackAll', 'purgeQueue', 'checkQueue', 'assertQueue', 'bindQueue', 'assertExchange']

/**
 * Calls to `publish()` or `sendToQueue()` work just like in amqplib, but messages are queued
 * internally to ensure delivery even after a connection drop. If the underlying connection drops,
 * ChannelWrapper will wait for a new connection and continue.
 *
 * @event connect Emitted every time this channel connects or reconnects
 * @event error (err, {name}) Emitted if there is an error setting up the channel or if the
 *  underlying channel throws an error.
 * @event close Emitted when this channel closes via a call to `close()`
 */
module.exports = class AmqpChannelWrapper extends EventEmitter {
  /**
   * Create a new channel wrapper instance. Should usually be called from a connection manager.
   *
   * @param {EventEmitter} connectionManager Connection this channel exists on to receive events
   * @param {(amqp.Channel, AmqpChannelWrapper) => Promise<void>} configurator
   * @param {string} options.name Name of the channel for debugging and tracking
   */
  constructor(connectionManager, configurator = null, options = {}) {
    super()
    this._onConnect = this._onConnect.bind(this)
    this._onDisconnect = this._onDisconnect.bind(this)
    this._onSendConfirmation = this._onSendConfirmation.bind(this)
    this._onConnectionClose = this._onConnectionClose.bind(this)

    /** Name for debugging/tracking */
    this.name = options.name

    /** Whether this channel has been closed. Once closed, it should not be re-opened. */
    this._closed = false
    /** Connection manager it is using for the AMQP connection */
    this._manager = connectionManager
    /** Current amqp channel */
    this._channel = null
    /** Promise for setting up the channel */
    this._setupPromise = null
    /** Worker "thread" which asynchronously manages the pending message queue */
    this._workerPromise = null
    /** Id of the current worker (used to make sure there is only one worker at a time). */
    this._workerId = 0

    /** List of configuration functions to use to setup the channel */
    this._configurators = [ ]
    if (configurator) this._configurators.push(configurator)

    /** List of pending messages in the form { type: string, args: any[], resolve: function, reject: function } */
    this._pending = []
    /** Messages that have been sent but not yet acked by the server */
    this._unconfirmed = []

    this._manager._channels.push(this) // register itself to make sure it is always added
    this._manager.on('connect', this._onConnect)
    this._manager.on('disconnect', this._onDisconnect)
    this._manager.on('close', this._onConnectionClose)

    // if already connected, don't wait for next connection event
    if (this._manager.connected) {
      this._onConnect({ connection: this._manager._currentConnection })
    }

    // define "passthrough" functions as waiting for a channel connection, then calling them on the
    // channel with the same args as passed. `Sync` versions just return false if the channel is not
    // available at the time of calling.
    for (const fnName of PASSTHROUGH_FUNCTIONS) {
      this[fnName] = async (...args) => (await this.waitForConnection())[fnName](...args)
      this[fnName + 'Sync'] = (...args) => this._connection && this._connection[fnName](...args)
    }
  }

  /**
   * Add new configuration to the channel. All configurators will be run in order starting with the
   * first one passed to the constructor (if one was passed).
   *
   * If there is a connection, the configurator will be run immediately, and this promise will
   * complete when it is done running.
   *
   * `this` will not be re-bound. To access this AmqpChannelWrapper instance, receive the second
   * parameter to the lambda function.
   *
   * @param {(amqp.Channel, AmqpChannelWrapper) => Promise<void>} configurator
   */
  async addSetup(configurator) {
    const channel = await this.waitForConnection()
    this._configurators.push(configurator)
    try {
      await configurator(channel, this)
    } catch (err) {
      this.emit('error', { name: this.name })
      await this.close()
      throw err
    }
  }

  /**
   * Generate a promise which resolves when this next gets connected with the current channel.
   * This does not need to be called and awaited as part of the workflow.
   *
   * @returns {Promise<amqp.Channel>}
   */
  async waitForConnection() {
    if (this._closed) throw new Error('Connection manager closed')
    if (this._channel) return this._channel
    await new Promise((resolve, reject) => {
      const closeHandler = () => reject(new Error('Connection closed'))
      this.once('close', closeHandler)
      this.once('connect', function() {
        this.removeListener('close', closeHandler)
        resolve(arguments)
      })
    })
    return this._channel
  }

  /**
   * Publish a message.
   * @see ampq.Channel.publish
   * @return {Promise<void>} Promise which resolves when this message is published.
   */
  async publish(...args) {
    if (args.length === 3) { args.push({}) } // confirm channel requires options object
    if (args.length !== 4) { throw new Error('Invalid arguments!') }
    await this._enqueue('publish', args)
  }

  /**
   * Send a message to a queue.
   * @see amqp.Channel.sendToQueue
   * @returns {Promise<void>}
   */
  async sendToQueue(...args) {
    if (args.length === 2) { args.push({}) } // confirm channel requires options object
    if (args.length !== 3) { throw new Error('Invalid arguments!') }
    await this._enqueue('sendToQueue', args)
  }

  _enqueue(type, args) {
    if (this._closed) throw new Error('Channel closed!')

    // basically we are generating a promise which we can resolve once the queued message is sent
    // or reject if we have to drop it/can't send it for some reason.
    return new Promise((resolve, reject) => {
      this._pending.push({ type, args, resolve, reject })
      // make sure the worker is active
      this._startWorker()
    })
  }

  /**
   * Destroy this channel. Any unset messages will have their associated promises rejected.
   * @returns {Promise<void>}
   */
  async close() {
    if (this._closed) return
    this._closed = true
    this._workerPromise = null
    this._manager.removeListener('connect', this._onConnect)
    this._manager.removeListener('disconnect', this._onDisconnect)
    // remove this channel from the list
    this._manager._channels.splice(this._manager._channels.indexOf(this), 1)
    this._manager = null
    await this._teardown()
    for (const msg of [...this._pending, ...this._unconfirmed]) {
      msg.reject(new Error('Channel closed'))
    }
    this.emit('close')
    this.removeAllListeners()
  }

  /**
   * Use this in the rare case that you want to manually trigger a reconnection to the amqp
   * server.
   * @returns {Promise<void>}
   */
  async forceReconnect() {
    if (!this._manager) return
    await this._manager.forceReconnect()
  }

  /**
   * The number of unsent messages queued on this channel.
   * @returns {number}
   */
  get length() {
    return this._pending.length
  }

  async _onConnect({ connection }) {
    this._teardown()
    await this._setup(connection)
  }

  _onDisconnect() {
    this._teardown()
  }

  /** Connection manager has been closed, so we should close too. */
  _onConnectionClose() {
    this.close()
  }

  /** Closeout and nullify the old connection. The returned promise need not be awaited. */
  _teardown() {
    if (this._channel) {
      try {
        this._channel.close()
      } catch (err) {
        Q.log.warn('Error closing old channel.', this.name, err)
      }
      this._channel = null
    }
  }

  /** Create and configure a new channel for the connection */
  async _setup(connection) {
    // don't wait for any pending promise as we may have multiple re-connects rapidly
    if (this._closed) return null

    const promise = this._setupInner(connection)
    this._setupPromise = promise
    promise
      .then(r => {
        // avoid setting to null if another setup process was initiated
        if (this._setupPromise === promise) this._setupPromise = null
        return r
      })
      .catch((err) => {
        if (this._setupPromise !== promise) return
        this._setupPromise = null
        Q.log.error('Failed setting up channel.', this.name, err)
        this.emit('error', err, { name: this.name })
        this.close()
      })
  }

  async _setupInner(connection) {
    const channel = await connection.createConfirmChannel()

    // define here so we can easily de-register it for `connect` event
    const reconnectLambda = err => {
      if (channel !== this._channel) {
        // channel has already been changed
        return
      }

      this._channel = null
      if (err) this.emit('error', err, { name: this.name })

      if (!this._manager) return // manager may be set to null on close
      // NOTE: forceReconnect will be called by every channel when disconnecting, but it should be
      //  a null op for all but the first call as it will be in the process of connecting already.
      this._manager.forceReconnect() // asnyc, so just gets the ball rolling
      // when reconnect occurs, it will cause a connect event which will re-establish this channel
    }

    channel.on('close', reconnectLambda)
    channel.on('error', err => {
      console.log('error received')
      channel.removeListener('close', reconnectLambda)
      reconnectLambda(err)
    })

    // run all configurators
    await Promise.all(this._configurators.map(cfgr => cfgr(channel, this)))

    // we will need to re-send all unconfirmed messages through the new channel
    if (this._unconfirmed.length) {
      this._pending = [...this._unconfirmed, ...this._pending]
      this._unconfirmed = []
    }

    // make it official
    this._channel = channel
    this.emit('connect')
    this._startWorker()
    return channel
  }

  /**
   * Ensure the worker "thread" is active. Will do nothing if it is already running or if there
   * are no messages to send.
   */
  _startWorker() {
    if (this._workerPromise || !this.length) return
    this._workerPromise = this._worker(++this._workerId)
      .catch(err => {
        Q.log.error('Worker encountered unhandled error', this.name, err)
        this.emit('error', err, { name: this.name })
      })
  }

  /** Worker "thread" function which runs asynchronously and consumes messages from the queue. */
  async _worker(id) {
    await 0 // need to wait for the `_workerPromise` to be set
    while (id === this._workerId && this._workerPromise && this.length && this._channel) {
      const channel = await this.waitForConnection()
      // make sure we are still permitted to work
      if (id !== this._workerId || !this._workerPromise) return

      // dequeue next message and process
      const msg = this._pending.shift()
      let waitForDrain = false
      try {
        this._unconfirmed.push(msg)
        waitForDrain = !channel[msg.type](...msg.args, this._onSendConfirmation)
        // wait for ack before resolving
      } catch (err) {
        // could not send, drop message
        msg.reject(err)
      }

      // we should wait before trying to send more messages
      if (waitForDrain) {
        await new Promise(resolve => {
          channel.once('close', resolve)
          channel.once('drain', function() {
            channel.removeListener('close', resolve)
            resolve(arguments)
          })
        })
      }
    }

    // on exit, set promise to null if it is "our" promise
    if (this._workerId === id) this._workerPromise = null
  }

  /** Called by amqplib once a message has been acked by the server. */
  _onSendConfirmation(err) {
    // handlers are called in order, so we know the next in line is being confirmed
    const msg = this._unconfirmed.shift()

    // we are done!
    if (!err) return msg.resolve()

    // we had an error :(
    if (!this._channel) {
      // channel was closed, put it back at the head of the queue
      this._pending.unshift(msg)
      this._startWorker()
    } else {
      // pass the error down, retrying will not help
      msg.reject(err)
    }
  }
}
