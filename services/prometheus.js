module.exports = function(config) {
  const client = require('prom-client')
  const http = require('http')

  client.prefix = config.get('promethues.prefix', 'quadro_')

  if (!config.get('prometheus.disableDefaultCollection')) {
    client.collectDefaultMetrics({
      timeout: config.get('promotheus.timeout', 15000),
      prefix: client.prefix
    })
  }

  http.createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/metrics') {
      res.writeHead(200, { 'Content-Type': 'text/plain' })
      res.end(client.register.metrics())
    } else {
      res.writeHead(404).end()
    }
  }).listen(config.get('prometheus.port', 9230))

  return client
}
