# HTTP

## Directory layout

You should place your controllers in `/controllers` and route definitions in
`/routes`.

## Routes

Quadro uses [koa-router](https://github.com/alexmingoia/koa-router). You can
register any route as your normally do with koa-router:

```js
// routes/custom_routes.js
module.exports = function(router) {
  router.post('/hello', async function(ctx) {
    ctx.body = 'yep!'
  })
}
```

### Resources

You register a resource with:

```js
module.exports = function(router) {
  router.resource('orders')
}
```

#### Resources mapping

Resource actions are mapped as follows to url paths:

| action  | method | path
|---------|--------|-----
| index   | GET    | /resource
| show    | GET    | /resource/**:id**
| create  | POST   | /resource
| update  | PUT    | /resource/**:id**
| destroy | DELETE | /resource/**:id**

#### Example

```js
// /routes/orders.js
module.exports = function(router) {
  router.resource('/orders', 'orders')
  // same as
  router.resource('orders')
}
```

```js
// /controllers/orders_controller.js
module.exports = class {
  async show(ctx) {
    // ...
  }
}

```

*NOTE:* `.resource()` only registers REST paths that are handled by the controller.
In the example above - the controller has only `show` method - and thus will have
only `GET /orders/:id` registered

##### Upload a file

```js
async create(ctx) {
  try {
    const file = ctx.request.body.files.file
    const fileContent = await AsyncFs.readFile(file.path, 'utf8')
    ctx.body = fileContent
    ctx.status = 201
  } catch (e) {
    this.handleError(e, ctx)
  }
}
```


### Route Events
Before routes are added to the http server, the Quadro app fires the event "routes-will-load", with the payload { quadroHttpServer:(koa instance) }

After routes have been added, the app fires "routes-did-load", with the same payload (now modified with routes).



## Controllers

**Important! DO NOT STORE REQUEST STATE IN CONTROLLER INSTANCE** Controllers are singletons. Only one instance of the controller is created and it's method is used in the route.

Controllers should be placed in `/controllers` directory and have `_controller`
suffix.
