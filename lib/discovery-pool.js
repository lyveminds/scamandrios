var
    Pool = require('./pool'),
    util = require('util')
    ;

var DiscoveryPool = module.exports = function DiscoveryPool()
{
    Pool.apply(this, arguments);
    this.checkDead = false;
};

util.inherits(DiscoveryPool, Pool);

// ...
