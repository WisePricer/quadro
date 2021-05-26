const amqp = require('amqplib')
const { EventEmitter } = require('events')
const { castArray } = require('lodash')
const AmqpChannelWrapper = require('./channel_wrapper')

const DEFAULT_HEARTBEAT_MS = 5 * 1000

/**
 * Time how long it takes to connect to each of the different URLs supplied and then
 * return list in order of ascending connection time.
 *
 * @param {any[]} urls
 * @returns {any[]} Sorted list of URLs by connection time
 */
async function urlsByPing(urls) {
  return (await Promise.map(urls, async u => {
    const start = Date.now()
    try {
      const conn = await amqp.connect(u)
      await conn.close()
      return [u, Date.now() - start]
    } catch (e) {}
    return [u, Infinity]
  }))
    .sort((a, b) => a[1] - b[1])
    .map(i => i[0])
}

/** Parse the string version of a connection URI for amqp and convert to a URL object. */
function parseUrl(url) {
  url = new URL(url)
  const obj = {}
  for (const k of ['hostname', 'port', 'username', 'password', 'local']) {
    obj[k] = url[k]
  }
  obj.protocol = url.protocol.split(':')[0]
  obj.vhost = url.pathname
  url.searchParams.forEach((value, name) => {
    obj[name] = value
  })

  for (const k in obj) {
    if (obj[k] === undefined || obj[k] === '') {
      delete obj[k]
    }
  }
  return obj
}

/**
 * Connection manager attempts to maintain a constant AMQP connection and will automatically attempt
 * reconnect if an issue occurs.
 * 
 * @event connect ({connection, url}) Emitted whenever we connect to a broker.
 * @event disconnect ({err}) Emitted whenever we disconnect from a broker.
 * @event closing Emitted when shutdown is initiated and no more messages will be accepted.
 * @event close Emitted when shutdown is complete.
 * @event blocked ({reason}) Emitted when the underlying connection emits a blocked message.
 * @event unblocked Emitted when the underlying connection emits an unblocked message.
 */
module.exports = class AmqpConnectionManager extends EventEmitter {
  /**
   * Create a RabbitMQ connection manager which maintains a connection at all times and
   * automatically reconnects as needed.
   *
   * @param {{amqp.Options.Connect | string}[]} urls An array of brokers to
   *  connect to. Takes connection objects defined by amqplib or URI strings.
   * @param {number?} options.heartbeatInterval Heartbeat interval in ms.
   * @param {number?} options.reconnectTimeout Time to wait before trying to reconnect. Defaults to
   *  options.heartbeatInterval.
   */
  constructor(urls, options = {}) {
    super()
    // intentionally does not allow setting heartbeat to 0
    this.heartbeatInterval = options.heartbeatInterval || DEFAULT_HEARTBEAT_MS
    this.reconnectTimeout = options.reconnectTimeout || this.heartbeatInterval

    /** List of url objects: {url: string, connectionOptions?: object} */
    this.urls = castArray(urls || [])
      .map(u => typeof u === 'string' ? parseUrl(u) : u)

    if (!this.urls || this.urls.length <= 0) throw new Error('At least one URL object is required!')

    /** Whether this "connection" has been closed. Once closed, it should not be re-opened. */
    this._closed = false
    /** List of amqplib channels */
    this._channels = []
    /** Current amqplib connection */
    this._connection = null
    /** Promise of the current connecting act */
    this._connectPromise = null

    // There will be one listener per channel, and there could be a lot of channels, so disable warnings from node.
    this.setMaxListeners(0)

    // initialize connection process
    this._connect()
  }

  /**
   * Generate a promise which resolves when this next gets connected. This does not need to be
   * called and awaited as part of the workflow.
   *
   * @returns {Promise<Connection>}
   */
  async waitForConnection() {
    if (this._closed) throw new Error('Connection manager closed')
    if (this._connection) return this._connection
    await new Promise((resolve, reject) => {
      const closeHandler = () => reject(new Error('Connection closed'))
      this.once('close', closeHandler)
      this.once('connect', function() {
        this.removeListener('close', closeHandler)
        resolve(arguments)
      })
    })
    return this._connection
  }

  /**
   * Close any active connections and prevent new connections from being created. Fully shutdown the
   * connection manager.
   *
   * @returns {Promise<void>}
   */
  async close() {
    if (this._closed) return
    this._closed = true
    this.emit('closing')

    try {
      await this._connectPromise
    } catch (err) {
      // ignore errors while closing
    }

    try {
      await Promise.all(this._channels.map(c => c.close()))
    } catch (err) {
      // ignore errors closing channels
    }

    this._channels = []
    this._closeConnection().catch((err) => { Q.log.warn('Error closing connecting', err) })
    this.emit('close')
    this.removeAllListeners()
  }

  /** Close the underlying connection */
  async _closeConnection() {
    if (!this._connection) return

    this._connection.removeAllListeners('close')
    const conn = this._connection
    this._connection = null
    await conn.close()
  }

  /**
   * Use this in the rare case that you want to manually trigger a reconnection to the amqp
   * server.
   */
  async forceReconnect() {
    if (!this._connection) {
      // reconnection is already in progress
      return
    }
    try {
      await this._closeConnection()
      this.emit('disconnect', { err: 'Manual reconnection triggered.' })
    } catch (err) {}
    this._connect()
  }

  /**
   * Check whether the connection is live presently.
   * @returns {boolean}
   */
  get connected() {
    return !!this._connection
  }

  /**
   * Create a new channel on this managed connection.
   *
   * @param {*} args See AmqpChannelWrapper constructor
   * @returns {AmqpChannelWrapper} A new channel.
   */
  createChannel(...args) {
    // channel registers itself with this, so this is effectively just a convince wrapper.
    return new AmqpChannelWrapper(this, ...args)
  }

  async _connect(wait = false) {
    if (this._connectPromise) return this._connectPromise
    if (this._closed || this.isConnected) return null
    this._connectPromise = new Promise(
      resolve => setTimeout(resolve, wait ? this.reconnectTimeout : 0)
    )
      .then(() => this._connectInner())
      .then(r => {
        // make sure it always gets reset
        this._connectPromise = null
        if (!this._connection) {
          // Catch case of a disconnect being thrown immediately after connecting, because
          // _connection is null, we know a disconnect event was already processed from the conn
          throw new Error('Connection dropped')
        }
        return r
      })
      .catch(async err => {
        // Ignore `Connection dropped` because it already emitted a disconnect event but could not
        // start the reconnect process naturally because this promise had not fully finished yet.
        if (err.message !== 'Connection dropped') {
          Q.log.error('Exception connecting to amqp server', err)
          this.emit('disconnect', { err })
        }
        this._connection = null
        this._connectPromise = null
        this._connect(true)
      })
  }

  async _connectInner() {
    if (this.urls.length <= 0) throw new Error('amqp-connection-manager: No servers found')

    // sort urls by "distance"
    const url = (await urlsByPing(this.urls))[0]
    let conn = null

    // setup connection
    conn = await amqp.connect({
      ...url,
      heartbeat: this.heartbeatInterval / 1000 // ms -> s
    })
    this._connection = conn

     // define here so we can easily de-register it for `connect` event
    const reconnectLambda = err => {
      if (this._connection !== conn) {
        // connection has been changed for some reason
        return
      }

      this._connection = null
      this.emit('disconnect', { err })
      this._connect(true)
    }

    // configure handlers
    conn.on('blocked', reason => this.emit('blocked', { reason }))
    conn.on('unblocked', () => this.emit('unblocked'))
    conn.on('close', reconnectLambda)
    conn.on('error', err => {
      // prevent the reconnect lambda from being called twice as a `close` event should be emitted
      // after an `error` event
      conn.removeListener('close', reconnectLambda)
      reconnectLambda(err)
    })

    // we did it!
    this.emit('connect', { connection: conn, url })
    return { connection: conn, url }
  }
}
