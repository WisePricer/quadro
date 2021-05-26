const amqp = require('../../lib/amqp')

Q.Errors.declare('PubsubConsumerAlreadyStartedError', 'One Consumer already started. No new consumer can be started again')

module.exports = class RabbitMqChannel {
  constructor(host) {
    this.host = host
    this.hostname = require('os').hostname()
    this.serviceName = Q.config.get('service.name', '[unknown]')
  }

  async initialize() {
    this.connection = await amqp.connect(this.host)
    this.connection.on('connect', () => Q.log.info('Connected to amqp server'))
    this.connection.on('disconnect', err => Q.log.warn('Disconnected from amqp server', err))
    this.connection.on('close', () => Q.log.info('AMQP connection closed'))
    this.channel = this.connection.createChannel()
  }

  /**
   * Publish a message to an exchange
   * @param {string} messageType (in this case also the exchange)
   * @param {object} message JSON serializable message content
   * @returns {Promise<true>}
   */
  async publish(messageType, message) {
    const msg = Buffer.from(JSON.stringify(message))
    await this.channel.publish(messageType, '', msg, { persistent: true })
    return true // legacy support
  }

  /**
   * Start listening to messages and register a listener.
   * @param {string} queueName Queue to listen to
   * @param {number} concurrency AMQP `prefetch` setting
   * @param {(Buffer) => Promise<*>} messageHandler
   * @returns {Promise<void>}
   */
  async startConsumer(queueName, concurrency, messageHandler) {
    if (this.queueName) {
      throw new Q.Errors.PubsubConsumerAlreadyStartedError({
        existingQueue: this.queueName,
        newQueue: queueName
      })
    }

    this.queueName = queueName
    this.concurrency = concurrency
    this.messageHandler = messageHandler

    if (!this.queueName) return
    await this.channel.addSetup(channel => Promise.all([
      // Make sure queue exists
      channel.assertQueue(this.queueName),
      // Set concurrency
      channel.prefetch(this.concurrency),
      // Start the consumer
      channel.consume(this.queueName, this.onMessage.bind(this))
    ]))

    await this.channel.waitForConnection()
    Q.log.info(`Listening to ${this.queueName}`)
  }

  /**
   * Called when a new message is received before calling the registered message handler.
   * @param {Buffer?} message
   * @returns {Promise<void>}
   */
  async onMessage(message) {
    // Message would be null if the connection is disconnected or channel is closed
    if (!message) {
      Q.log.error('RabbitMQ channel polled a undefined message.')
      // (only known case for this is if a queue is deleted and then recreated)
      await this.connection.forceReconnect()
      return
    }
    // Process message
    try {
      await this.messageHandler(message)
    } catch (err) {
      Q.log.error('Message handler threw an error!!!', err)
      // this should never happen because it would mean there is an error in the hub message
      // processor itself; if that happens a fix to quadro is needed.
      await this.channel.nack(message)
    }
    await this.channel.ack(message)
  }
}
