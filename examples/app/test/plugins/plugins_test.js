/* eslint no-unused-expressions: 0 */

describe('Plugins', function() {
  it('loads the plugin', function() {
    expect(global.pluginLoaded).to.be.ok
  })

  it('loads plugin initializers', function() {
    expect(global.pluginInitializerStarted).to.be.ok
  })

  describe('configs', function() {
    it('loads configuration', function() {
      expect(Q.config.get('plugin_config.someKey')).to.equal('somePluginValue')
    })

    it('loads env specific configs', function() {
      expect(Q.config.get('plugin_test_config.testKey')).to.equal('pluginTestValue')
    })

    it('loads conflicting configs', function() {
      expect(Q.config.get('quadro.aws.awsTestKey')).to.equal('awsDummyValue')
      expect(Q.config.get('quadro.aws.region')).to.equal('us-east-5')
    })
  })

  describe('services', function() {
    it('loads plugin services', function() {
      let svc = Q.container.get('plugin:testSvc')
      expect(svc).to.be.ok
      expect(svc.whoAmI()).to.equal('plugin')
    })

    it('supports module @aliases property in plugins', function() {
      expect(Q.container.get('plugin-alias')).to.be.equal(Q.container.get('plugin:testSvc'))
    })
  })

  describe('tasks', function() {
    it('loads tasks', async function() {
      this.sinon.stub(process, 'exit').callsFake(_ => null)
      let taskRunner = await Q.container.getAsync('taskRunner')
      await taskRunner.run('plugin:someTask')
      expect(global.pluginTaskExecuted).to.be.true
      expect(process.exit).to.have.been.calledWith(0)
    })
  })
})
