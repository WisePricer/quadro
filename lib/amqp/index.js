const AmqpConnectionManager = require('./connection_manager')
const AmqpChannelWrapper = require('./channel_wrapper')

module.exports = {
  /**
   * Create a new, managed connection.
   * @param {*} args See AmqpConnectionManager constructor for arguments.
   * @returns {AmqpConnectionManager}
   */
  connect(...args) {
    return new AmqpConnectionManager(...args)
  },

  AmqpConnectionManager,
  AmqpChannelWrapper
}
