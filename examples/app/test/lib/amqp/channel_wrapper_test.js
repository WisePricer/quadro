/* eslint-disable no-unused-expressions */

const { FakeAmqpConnectionManager } = require('./fixtures')
const sinon = require('sinon')

const AmqpChannelWrapper = require('../../../../../lib/amqp/channel_wrapper')
const { expect } = require('chai')
describe('ChannelWrapper', function() {
  this.timeout(200) // reduce timeout since none of this connects to actual network
  let cm

  beforeEach(() => {
    cm = new FakeAmqpConnectionManager()
  })

  it('should work if there are no setup functions', async () => {
    const ch = new AmqpChannelWrapper(cm)
    cm.simulateConnect()
    await ch.waitForConnection()
    expect(!!ch._channel).is.true
  })

  it('should run all setup functions on connect', async () => {
    const setup1 = sinon.spy(() => Promise.resolve())
    const setup2 = sinon.spy(() => Promise.resolve())

    const ch = new AmqpChannelWrapper(cm, setup1)
    ch.addSetup(setup2) // do not await since it will not resolve until after connection is established
    cm.simulateConnect()
    await ch.waitForConnection()

    expect(setup1).to.have.been.calledOnce
    expect(setup2).to.have.been.calledOnce
  })

  it('should run all setup functions on reconnect', async () => {
    const setup1 = sinon.spy(() => Promise.resolve())
    const setup2 = sinon.spy(() => Promise.resolve())
    const ch = new AmqpChannelWrapper(cm, setup1)
    ch.addSetup(setup2)

    cm.simulateConnect()
    await ch.waitForConnection()
    cm.simulateDisconnect()
    cm.simulateConnect()
    await ch.waitForConnection()

    expect(setup1).to.have.been.calledTwice
    expect(setup2).to.have.been.calledTwice
  })

  it('should pass correct arguments to configurators', async () => {
    let args = []
    const ch = new AmqpChannelWrapper(cm, function() { args = arguments; return Promise.resolve() })
    cm.simulateConnect()
    await ch.waitForConnection()
    expect(args[0]).to.equal(ch._channel)
    expect(args[1]).to.equal(ch)
  })

  it('should emit an error if a setup function throws when connecting', async () => {
    const setup = sinon.spy(() => Promise.reject(new Error('Boom!')))
    const errors = []
    let closed = false
    const ch = new AmqpChannelWrapper(cm, setup)
    ch.on('close', () => closed = true)
    ch.on('error', e => errors.push(e))
    cm.simulateConnect()
    try {
      await ch.waitForConnection()
      expect(0).equals(1)
    } catch (err) { /** expected to throw */ }
    expect(setup).has.been.calledOnce
    expect(errors).is.length(1)
    expect(closed).is.true
  })

  it('should emit error if setup function throws once connected', async () => {
    const setup = sinon.spy(() => Promise.reject(new Error('Boom!')))
    const errors = []
    let closed = false
    const ch = new AmqpChannelWrapper(cm)
    ch.on('close', () => closed = true)
    ch.on('error', e => errors.push(e))
    cm.simulateConnect()
    await ch.waitForConnection()
    try {
      await ch.addSetup(setup)
      expect(0).equals(1)
    } catch (err) { /** expected to throw */ }
    expect(setup).has.been.calledOnce
    expect(errors).is.length(1)
    expect(closed).is.true
  })

  it('immediately returns from waitForConnection if already connected', async () => {
    const ch = new AmqpChannelWrapper(cm)
    cm.simulateConnect()
    await ch.waitForConnection()

    let t = false
    ch.waitForConnection().then(() => t = true)
    await 0 // cycle event loop
    expect(t).to.be.true
  })

  it('immediately runs setup if already connected', async () => {
    const ch = new AmqpChannelWrapper(cm)
    cm.simulateConnect()
    await ch.waitForConnection()

    let t = false
    ch.addSetup(() => Promise.resolve(t = true))
    // cycle event loop, takes multiple cycles on node < 12
    for (let i = 0; i < 5; ++i) await 0
    expect(t).to.be.true
  })

  it('should emit an error if amqplib refuses to create a channel for us', async () => {
    const errorHandler = sinon.spy(() => {})
    const ch = new AmqpChannelWrapper(cm)
    ch.on('error', errorHandler)
    // onConnect does not resolve when connected or failed, so need to cycle down below
    await ch._onConnect({
      connection: {
        async createConfirmChannel() {
          throw new Error('No chanel for you!')
        }
      }
    })
    await 0 // cycle event loop
    expect(errorHandler).to.have.been.calledOnce
  })

  it('should publish messages to the underlying channel', async () => {
    cm.simulateConnect()
    const ch = new AmqpChannelWrapper(cm)
    await ch.waitForConnection()

    let published = false
    ch._channel.on('publish', () => published = true)

    // test with all params
    ch._channel.once('publish', m => {
      expect(m.content).to.equal('testContent')
      expect(m.exchange).to.equal('myexchange')
      expect(m.routingKey).to.equal('someRoutingKey')
      expect(m.options).to.eql({ someOpt: 'hi' })
    })
    await ch.publish('myexchange', 'someRoutingKey', 'testContent', { someOpt: 'hi' })
    expect(published).is.true

    // test without option param
    published = false
    ch._channel.once('publish', m => {
      expect(m.content).to.equal('aThing')
      expect(m.exchange).to.equal('exchange2')
      expect(m.routingKey).to.equal('routingKey')
      expect(m.options).to.eql({})
    })
    await ch.publish('exchange2', 'routingKey', 'aThing')
    expect(published).is.true

    // test with wrong number of params (should fail)
    published = false
    try {
      await ch.publish('one', 'two', 'three', 'four', 'five')
      expect(false).to.be.true
    } catch (err) {}
    expect(published).to.be.false

    try {
      await ch.publish('one', 'two')
      expect(false).to.be.true
    } catch (err) {}
    expect(published).to.be.false
  })

  it('should send a message to queue in underlying channel', async () => {
    cm.simulateConnect()
    const ch = new AmqpChannelWrapper(cm)
    await ch.waitForConnection()

    let published = false
    ch._channel.on('sendToQueue', () => published = true)

    // test will all params
    ch._channel.once('sendToQueue', m => {
      expect(m.content).to.equal('testContent')
      expect(m.queue).to.equal('queue')
      expect(m.options).to.eql({ myOpt: 'world' })
    })
    await ch.sendToQueue('queue', 'testContent', { myOpt: 'world' })
    expect(published).is.true

    // test without option param
    published = false
    ch._channel.once('sendToQueue', m => {
      expect(m.content).to.equal('hello world!')
      expect(m.queue).to.equal('myQueue')
      expect(m.options).to.eql({})
    })
    await ch.sendToQueue('myQueue', 'hello world!')
    expect(published).is.true

    // test with wrong number of params
    published = false
    try {
      await ch.sendToQueue('one', 'two', 'three', 'four')
      expect(false).to.be.true
    } catch (err) {}
    expect(published).is.false

    try {
      await ch.sendToQueue('one')
      expect(false).to.be.true
    } catch (err) {}
    expect(published).is.false
  })

  it('should queue messages when disconnected and send on connect', async () => {
    const ch = new AmqpChannelWrapper(cm)

    let m1Sent = false
    let m2Sent = false
    let m1 = ch.sendToQueue('queue', 'content').then(() => m1Sent = true)
    let m2 = ch.sendToQueue('exchange', 'routingKey', 'content').then(() => m2Sent = true)

    await 0
    expect(m1Sent).to.be.false
    expect(m2Sent).to.be.false

    cm.simulateConnect()
    await ch.waitForConnection()
    await m1
    expect(m1Sent).to.be.true
    await m2
    expect(m2Sent).to.be.true
  })

  it('should queue messages if channel closes while trying to send', async () => {
    cm.simulateConnect()
    const ch = new AmqpChannelWrapper(cm)
    await ch.waitForConnection()

    let m1Sent = false
    let m2Sent = false
    let m1 = ch.sendToQueue('queue', 'content').then(() => m1Sent = true)
    let m2 = ch.sendToQueue('exchange', 'routingKey', 'content').then(() => m2Sent = true)

    cm.simulateDisconnect()

    await 0
    expect(m1Sent).to.be.false
    expect(m2Sent).to.be.false

    cm.simulateConnect()
    await ch.waitForConnection()
    await m1
    expect(m1Sent).to.be.true
    await m2
    expect(m2Sent).to.be.true
  })

  it('should wait for drain event if sending message returns false', async () => {
    cm.simulateConnect()
    const ch = new AmqpChannelWrapper(cm)
    await ch.waitForConnection()

    // sends normally...
    let m1Sent = false
    let m2Sent = false
    ch._channel.expectDrain = true
    let m1 = ch.sendToQueue('queue', 'content').then(() => m1Sent = true)
    await m1
    expect(m1Sent).to.be.true

    // worker should not be "frozen" until the drain event
    let m2 = ch.sendToQueue('exchange', 'routingKey', 'content').then(() => m2Sent = true)
    await new Promise(resolve => setTimeout(resolve, 20))
    expect(m2Sent).to.be.false
    ch._channel.emitDrain()
    await m2
    expect(m2Sent).to.be.true
  })

  it('should run all configurators before sending any queued messages', async () => {
    let configRun = false
    let msgSent = false
    const ch = new AmqpChannelWrapper(cm, () => {
      configRun = true
      expect(msgSent).to.be.false
    })
    const p = ch.sendToQueue('queue', 'content').then(() => {
      msgSent = true
      expect(configRun).to.be.true
    })
    cm.simulateConnect()
    await p
    expect(configRun).to.be.true
    expect(msgSent).to.be.true
  })

  it('should emit connect messages prior to sending queued messages', async () => {
    let cmConnected = false
    let chConnected = false
    const ch = new AmqpChannelWrapper(cm)
    cm.once('connect', () => cmConnected = true)
    ch.once('connect', () => chConnected = true)
    const p = ch.sendToQueue('queue', 'content')
    cm.simulateConnect()
    await p
    expect(cmConnected).is.true
    expect(chConnected).is.true
  })

  it('should proxy ack and nack to underlying channel', async () => {
    cm.simulateConnect()
    const ch = new AmqpChannelWrapper(cm)
    await ch.waitForConnection()

    ch.ack('a')
    ch.nack('b')
    // make sure they resolve instantly as we are already connected
    // node < 12 requires multiple cycles
    for (let i = 0; i < 5; ++i) await 0

    const channel = ch._channel
    expect(channel.ack).to.have.been.calledOnce
    expect(channel.ack.lastCall.args).to.eql(['a'])
    expect(channel.nack).to.have.been.calledOnce
    expect(channel.nack.lastCall.args).to.eql(['b'])
  })

  it('should proxy ack and nack to underlying channel when connection is established', async () => {
    const ch = new AmqpChannelWrapper(cm)

    const p = [ ch.ack('a'), ch.nack('b') ]
    cm.simulateConnect()
    await Promise.all(p)

    const channel = ch._channel
    expect(channel.ack).to.have.been.calledOnce
    expect(channel.ack.lastCall.args).to.eql(['a'])
    expect(channel.nack).to.have.been.calledOnce
    expect(channel.nack.lastCall.args).to.eql(['b'])
  })

  it('should close gracefully when connected', async () => {
    cm.simulateConnect()
    const ch = new AmqpChannelWrapper(cm)
    await ch.waitForConnection()
    const channel = ch._channel
    await ch.close()
    expect(channel.close).to.have.been.calledOnce
    expect(ch._closed).to.be.true
  })

  it('should close gracefully when not connected', async () => {
    cm.simulateConnect()
    const ch = new AmqpChannelWrapper(cm)
    await ch.waitForConnection()
    cm.simulateDisconnect()
    await ch.close()
    expect(ch._closed).to.be.true
  })

  it('should handle channel errors', async () => {
    cm.simulateConnect()
    const ch = new AmqpChannelWrapper(cm)
    let errors = 0
    ch.on('error', () => ++errors)

    await ch.waitForConnection()
    ch._channel.kill()
    await 0
    expect(cm._reconnecting).is.true
    expect(ch._channel).to.be.null
    expect(errors).to.equal(1)
  })
})
