const RabbitMqChannel = require('./pubsub/rabbitmq_channel')

module.exports = class Pubsub {
  constructor(log, request, config, prometheus) {
    this.log = log
    this.request = request
    this.config = config
    this.metrics = {
      publishedCount: new prometheus.Counter({
        name: `${prometheus.prefix}rabbitmq_published`,
        help: 'Total number of messages published to rabbitmq.',
        labelNames: ['messageType']
      }),
      missedCount: new prometheus.Counter({
        name: `${prometheus.prefix}rabbitmq_missed`,
        help: 'Total number of messages which could not be published.',
        labelNames: ['messageType']
      })
    }
  }

  async initialize() {
    if (this.config.get('service.messages.host')) {
      this.rabbitmqChannel = new RabbitMqChannel(this.config.get('service.messages.host'))
      return this.rabbitmqChannel.initialize()
    }
  }

  async publish(type, content, sendMethod = 'default') {
    let msg = {
      messageType: type,
      content
    }
    if (this.rabbitmqChannel && sendMethod !== 'http') {
      let published = await this.rabbitmqChannel.publish(msg.messageType, msg)
      if (!published) {
        this.metrics.missedCount.inc({messageType: msg.messageType})
        throw new Q.Errors.MessagePublishingFailed()
      }
      this.metrics.publishedCount.inc({messageType: msg.messageType})
    } else {
      let endpoint = this.config.get('quadro.pubsub.endpoint', 'http://hub:8080')
      await this.request({
        forever: true,
        method: 'POST',
        uri: `${endpoint}/api/v1/messages`,
        body: msg,
        json: true
      })
    }
  }
}

Q.Errors.declare('MessagePublishingFailed', 'Failed to publish message')
