const RabbitMqChannel = require('./rabbitmq_channel')

module.exports = class RabbitMqMessageAdapter {
  constructor(log, config, messageProcessor = 'pubsub:hubMessageProcessor') {
    this.log = log
    this.config = config
    this.messageProcessor = messageProcessor
  }

  async initialize() {
    // If consumer configs are not present then don't initialize it
    let serviceName = this.config.get('service.name')
    let disableConsumer = this.config.get('service.consumer.disable', false)
    if (!serviceName || disableConsumer) {
      return
    }

    // Get the channel
    let channel = new RabbitMqChannel(this.config.get('service.messages.host'))
    await channel.initialize()

    // Get the queue name and concurrency
    let queueName = this.config.get('service.messages.input', `${serviceName}_in`)
    let concurrency = parseInt(this.config.get('service.messages.concurrency', 10))
    await channel.startConsumer(queueName, concurrency, this.handleProcessing.bind(this))
  }

  async handleProcessing(message) {
    return this.messageProcessor.handleProcessing(message)
  }
}
