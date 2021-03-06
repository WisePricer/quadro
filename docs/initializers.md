# Initializers

Initializers are located in `initializers/` folder and are executed
at application startup.

Example:

```js
// initializers/runtime.js
module.exports = class {
  constructor({ runtime: runtimeConfig } = config) {
    this.config = runtimeConfig
  }

  // ...
}
```

To skip a initializer to be initialized in a specific environment then
add following in {environment}/quadro.yaml
```
initializers:
  runtime: false
```

Another example can be to skip all the jobs during test phase. To do that add
following in test/quadro.yaml
```
initializers:
  jobs: false
```
