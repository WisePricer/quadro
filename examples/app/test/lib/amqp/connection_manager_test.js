const proxyquire = require('proxyquire')
const { FakeAmqp } = require('./fixtures')
// const { fake } = require('sinon')

const fakeAmqplib = new FakeAmqp()
const URL = { protocol: 'amqp', hostname: 'localhost', vhost: '/', port: 5671 }

// need to override amqplib in the import of index, so double-layer proxyquire to accomplish
const _CM = proxyquire('../../../../../lib/amqp/connection_manager', { amqplib: fakeAmqplib })
const amqp = proxyquire('../../../../../lib/amqp', { './connection_manager': _CM })

describe('AmqpConnectionManager', function() {
  this.timeout(200) // reduce timeout since none of this connects to actual network
  let cm

  beforeEach(() => {
    fakeAmqplib.reset()
  })

  afterEach(() => {
    if (cm) cm.close()
  })

  it('parses URI strings correctly', async () => {
    cm = amqp.connect('amqp://myuser:somepass@localhost:5000?connectionSetting=324&setting2=hi')
    expect(cm.urls[0]).to.eql({
      protocol: 'amqp',
      username: 'myuser',
      password: 'somepass',
      hostname: 'localhost',
      port: '5000',
      connectionSetting: '324',
      setting2: 'hi'
    })
  })

  it('should establish a connection to a broker', async () => {
    cm = amqp.connect(URL)
    const conn = await cm.waitForConnection()
    expect(conn.url).to.eql({ ...URL, heartbeat: 5 })
  })

  it('should close a connection to a broker', async () => {
    cm = amqp.connect(URL)
    let close = false
    cm.on('close', () => close = true)

    await cm.waitForConnection()
    expect(close).to.be.false
    
    const conn = cm._connection
    expect(conn._closed).to.be.false
    await cm.close()
    expect(close).to.be.true
    expect(!!cm._connection).to.be.false
    expect(cm._closed).to.be.true
    expect(conn._closed).to.be.true
  })

  it('should close pending connections to a broker', async () => {
    // ensure that even if close is called while a connection is being established, the connection
    // still gets closed
    cm = amqp.connect(URL)
    let close = false
    cm.on('close', () => close = true)
    
    expect(!!cm._connection).to.be.false
    expect(!!cm._connectPromise).to.be.true

    const cp = cm._connectPromise
    await cm.close()
    const conn = (await cp).connection

    expect(close).to.be.true
    expect(!!cm._connection).to.be.false
    expect(cm._closed).to.be.true
    expect(conn._closed).to.be.true
  })

  it('should throw an error if no url is provided', async () => {
    expect(amqp.connect).to.throw('At least one URL object is required!')
  })

  it('should reconnect to the broker if the broker disconnects', async () => {
    cm = amqp.connect(URL, { reconnectTimeout: 10 })
    let connects = 0
    let disconnects = 0
    cm.on('connect', () => ++connects)
    cm.on('disconnect', () => ++disconnects)

    await new Promise(resolve => cm.once('connect', resolve))
    expect(connects).equals(1)
    expect(disconnects).equals(0)

    fakeAmqplib.kill()
    await new Promise(resolve => cm.once('connect', resolve))
    expect(connects).equals(2)
    expect(disconnects).equals(1)
  })

  it('should handle broker failing during connection process', (done) => {
    cm = amqp.connect(URL, { reconnectTimeout: 10 })
    let connects = 0
    let disconnects = 0
    cm.on('connect', () => ++connects)
    cm.on('disconnect', () => ++disconnects)

    cm.once('connect', () => {
      fakeAmqplib.kill()
      cm.once('connect', () => {
        expect(connects).to.equal(2)
        expect(disconnects).to.equal(1)
        done()
      })
    })
  })

  it('successfully forces a reconnect', async () => {
    cm = amqp.connect(URL, { reconnectTimeout: 10 })
    let connects = 0
    let disconnects = 0
    cm.on('connect', () => ++connects)
    cm.on('disconnect', () => ++disconnects)

    await cm.waitForConnection()
    cm.forceReconnect()
    await cm.waitForConnection()
    expect(connects).to.equal(2)
    expect(disconnects).to.equal(1)
  })

  it('should know if it is connected', async () => {
    cm = amqp.connect(URL, { reconnectTimeout: 10 })
    expect(cm.connected).to.be.false
    await new Promise(resolve => cm.once('connect', resolve))
    expect(cm.connected).to.be.true
    fakeAmqplib.kill()
    expect(cm.connected).to.be.false
    await new Promise(resolve => cm.once('connect', resolve))
    expect(cm.connected).to.be.true
  })

  it('should not reconnect after close', async () => {
    cm = amqp.connect(URL, { heartbeatInterval: 10 })
    let connects = 0
    cm.on('connect', () => ++connects)
    await cm.waitForConnection()
    cm.close()
    fakeAmqplib.kill()
    await new Promise(resolve => setTimeout(resolve, 50))
    expect(connects).to.equal(1)
  })

  it('should create and cleanup channel wrappers', async () => {
    cm = amqp.connect(URL)
    const ch = cm.createChannel(null, {name: 'test'})
    
    expect(cm._channels).to.be.length(1)
    expect(cm.listeners('connect')).to.be.length(1)
    expect(cm.listeners('disconnect')).to.be.length(1)
    
    await ch.close()
    expect(cm._channels).to.be.length(0)
    expect(cm.listeners('connect')).to.be.length(0)
    expect(cm.listeners('disconnect')).to.be.length(0)
  })

  it('should detect connection block/unblock', async () => {
    cm = amqp.connect(URL)
    let connects = 0
    let blocks = 0
    let unblocks = 0
    cm.on('connect', () => ++connects)
    cm.on('blocked', () => ++blocks)
    cm.on('unblocked', () => ++unblocks)

    await cm.waitForConnection()
    fakeAmqplib.simulateRemoteBlock()
    fakeAmqplib.simulateRemoteUnblock()
    await 0

    expect(connects).to.equal(1)
    expect(blocks).to.equal(1)
    expect(unblocks).to.equal(1)
  })
})
