/*
    The dicovery pool does not bother to try to resurrect its dead connections.
    Instead, every N seconds it re-discovers the active node list from its seed
    node. If any nodes have left the ring, it removes them from its active client
    list. If any nodes are missing from its active list, it creates them.
*/

var
    _         = require('lodash'),
    assert    = require('assert'),
    discovery = require('./discovery'),
    P         = require('p-promise'),
    Pool      = require('./pool'),
    util      = require('util')
    ;

var DiscoveryPool = module.exports = function DiscoveryPool(seed, options)
{
    assert(seed, 'you must pass a `seed` option, which can be either a string or an object');
    options = options || {};
    options.hosts = options.hosts || [];

    Pool.call(this, options);

    this.seed      = seed;
    this.options   = options;
    this.checkDead = false;

    this.rediscoveryBound = _.bindKey(this, 'rediscovery');
};
util.inherits(DiscoveryPool, Pool);

DiscoveryPool.prototype._rediscoveryTimer = null;
DiscoveryPool.prototype.discoveryInProgress = false;

DiscoveryPool.prototype._connect = DiscoveryPool.prototype.connect; // save the inherited one
DiscoveryPool.prototype.connect = function connect()
{
    if (this.ready)
        return P(); // already connected

    var self = this;

    self.emit('log', 'auto-discovering cassandra nodes...');

    return this.discoverNodes()
    .then(function(hosts)
    {
        self.hosts = hosts;
        return self._connect();
    })
    .then(function(results)
    {
        if (!self._rediscoveryTimer)
            self._rediscoveryTimer = setTimeout(self.rediscoveryBound, self.monitorInterval);
        self.ready = true;
        return results;
    });
};

DiscoveryPool.prototype.discoverNodes = function discoverNodes()
{
    var self = this;
    var opts =
    {
        host:       this.seed,
        lookupSeed: this.options.lookupSeed,
    };

    return discovery.discover(opts)
    .then(function(nodes)
    {
        var hosts = _.map(nodes, function(i) { return i + ':9160'; });
        return hosts;
    });
};

DiscoveryPool.prototype.rediscovery = function rediscovery()
{
    if (this.discoveryInProgress)
        return;

    var self     = this,
        deferred = P.defer(),
        changed  = false,
        additions, deletions, current;
    this.discoveryInProgress = true;

    this.discoverNodes()
    .then(function(nodes)
    {
        current = nodes;

        additions = _.difference(current, self.hosts);
        deletions = _.difference(self.hosts, current);

        changed = !!additions.length || !!deletions.length;

        // Remove all exited nodes, close connections, clean up.
        var clientsToNuke = _.map(deletions, function(removed)
        {
            return _.find(self.clients, function(c) { return (c.host === removed.host && c.port === removed.port); });
        });
        _.pull(self.clients, clientsToNuke);
        _.each(clientsToNuke, function(removed)
        {
            removed.ready = false;
            removed.close();
        });

        if (changed)
        {
            self.emit('log', 'cassandra node changes; added: ' + JSON.stringify(additions) + '; removed: ' + JSON.stringify(deletions));
        }

        // The additions require opening connections.
        var actions = _.map(additions, function(h) { return self.resurrect(h); });
        return P.allSettled(actions);
    })
    .then(function(results)
    {
        self.hosts = current;
        self._rediscoveryTimer = setTimeout(self.rediscoveryBound, self.monitorInterval);
        self.discoveryInProgress = false;
        if (changed)
            deferred.resolve(current);
        else
            deferred.resolve(false);
    })
    .fail(function(err)
    {
        self.emit('log', 'cassandra node auto-discovery failed: ' + err.message);
        self.discoveryInProgress = false;
        deferred.reject(err);
    })
    .done();

    return deferred.promise;
};
