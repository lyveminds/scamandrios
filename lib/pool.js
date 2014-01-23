var
    _            = require('lodash'),
    EventEmitter = require('events').EventEmitter,
    P            = require('p-promise'),
    util         = require('util')
    ;

var Connection = require('./connection'),
    errors = require('./errors');

var Pool = module.exports = function Pool(options)
{
    options = Object(options);
    this.clients = [];
    this.dead = [];

    var hosts = options.hosts = 'hosts' in options || !('host' in options) ? options.hosts : [options.host];
    if (!Array.isArray(hosts))
        throw Error('ScamandriosError: Invalid hosts supplied for connection pool');

    _.extend(this, options, function (destinationValue, sourceValue)
    {
        return sourceValue == null ? destinationValue : sourceValue;
    });

    this.onHealthChangeBound = _.bindKey(this, 'onHealthChange');
    this.monitorConnectionsBound = _.bindKey(this, 'monitorConnections');
};
util.inherits(Pool, EventEmitter);

Pool.prototype.TTL = Connection.TTL;

Pool.prototype.ready            = false;
Pool.prototype.hostPoolSize     = 1;
Pool.prototype.closing          = false;
Pool.prototype.consistencylevel = 1;
Pool.prototype.checkDead        = true;
Pool.prototype.monitorInterval  = 60000;

Pool.prototype.hosts         = null;
Pool.prototype.clients       = null;
Pool.prototype.dead          = null;
Pool.prototype.timeout       = null;
Pool.prototype._monitorTimer = null;

Pool.prototype.keyspace         = null;
Pool.prototype.cqlVersion       = null;
Pool.prototype.user             = null;
Pool.prototype.password         = null;

Pool.prototype.getHost = function(clients)
{
    return clients[Math.floor(Math.random() * clients.length)];
};

Pool.prototype.createConnection = function(host)
{
    var options = _.pick(this, 'keyspace', 'user', 'password', 'timeout', 'cqlVersion', 'consistencylevel');
    options.host = host;
    options.TTL = this.TTL + Math.floor(Math.random() * 10000);
    var connection = new Connection(options);
    connection.on('health', this.onHealthChangeBound);
    connection.on('error', _.bind(this.emit, this, 'error'));
    return connection;
};

Pool.prototype.connect = function()
{
    if (this.ready)
        return P(); // already connected

    var deferred = P.defer(),
        self = this;

    var hosts = this.hosts,
        poolSize = this.hostPoolSize,
        pendingCount = hosts.length * poolSize;

    function handleSettle(isResolved, value)
    {
        pendingCount--;
        if (isResolved)
        {
            self.ready = true;
            deferred.resolve(value);
        }
        if (!pendingCount)
        {
            deferred.reject(errors.create({ 'name': 'NoAvailableNodesException', 'why': 'pool.connect() failed to find any nodes' }));
        }
    }

    _.forEach(hosts, function (host)
    {
        _.times(poolSize, function ()
        {
            var connection = self.createConnection(host);
            connection.connect().then(function (keyspace)
            {
                handleSettle(true, keyspace);
                if (keyspace)
                    keyspace.connection = self;
                self.clients.push(connection);
                self.emit('log', 'connection established to cassandra@' + host);
                if (self.closing)
                    connection.close();
            }, function (error)
            {
                self.emit('log', 'initial connection failed to cassandra@' + host);
                handleSettle(false);
                self.dead.push(host);
            });
        });
    });

    if (this.checkDead)
        this._monitorTimer = setTimeout(this.monitorConnectionsBound, this.monitorInterval);

    return deferred.promise;
};

Pool.prototype.onHealthChange = function onHealthChange(client)
{
    var self = this;

    if (client.healthy)
        return;

    var deadhost = client.host + ':' + client.port;
    self.emit('log', 'unhealthy node @' + deadhost);
    self.dead.push(deadhost);
    client.close();
    client.ready = false;
    _.remove(self.clients, function(c) { return (c.host === client.host && c.port === client.port); });
};

Pool.prototype.use = function(keyspace)
{
    var self = this;
    return P.allSettled(_.map(this.clients, function (client)
    {
        return client.use(keyspace).then(function (keyspace)
        {
            if (keyspace)
                keyspace.connection = self;
            return keyspace;
        });
    }));
};

Pool.prototype.assignKeyspace = function(keyspace)
{
    var self = this;

    return this.use(keyspace)
    .then(function(promises)
    {
        var rejection = _.find(promises, function(promise) { return promise.state == 'rejected' && promise.reason.name == 'ScamandriosNotFoundException'; });
        if (!rejection)
            return promises;

        return self.createKeyspace(keyspace)
        .then(function() { return self.use(keyspace); })
        .then(function(responses)
        {
            var fulfillment = _.find(responses, { 'state': 'fulfilled' });
            if (!fulfillment)
            {
                var selectError = new Error('Failed to create and select keyspace.');
                _.assign(selectError, { 'keyspace': keyspace, 'responses': responses });
                throw selectError;
            }

            return responses;
        });
    });
};

Pool.prototype.getConnection = function(getHost)
{
    var host = (_.isFunction(getHost) ? getHost : this.getHost).call(this, this.clients);
    if (!host)
    {
        this.emit('error', errors.create({ 'name': 'NoAvailableNodesException', 'why': 'node.getConnection() failed' }));
        return;
    }
    if (host.ready)
        return host;
    var clients = this.clients;
    for (var length = clients.length; length--;)
    {
        var client = clients[length];
        if (!client.ready)
        {
            clients.splice(length, 1);
            var deadhost = client.host + ':' + client.port;
            this.emit('log', 'getConnection() pushing host onto dead pool: ' + deadhost);
            this.dead.push(deadhost);
        }
    }
    return this.getConnection(getHost);
};

// If a node is alive in any useful sense, it will respond to this query.
// Nodes that time out or give bogus responses to this should go onto the
// dead list.
Pool.PING_QUERY = new Buffer('SELECT * FROM system.schema_keyspaces;');

Pool.prototype.hasConnectionTo =  function(hostport)
{
    var hps = _.map(this.clients, function(c) { return c.host + ':' + c.port; });
    return hps.indexOf(hostport) !== -1;
};

Pool.prototype.resurrect = function resurrect(host)
{
    var deferred = P.defer(),
        self = this;

    var connection = self.createConnection(host);

    connection.connect()
    .then(function()
    {
        self.emit('log', 'cassandra host rising from the dead: ' + host);
        if (!self.hasConnectionTo(host))
            self.clients.push(connection);
        deferred.resolve(connection);
    })
    .fail(function(err)
    {
        // self.emit('log', 'cassandra host still dead: ' + host + ':' + JSON.stringify(err));
        connection.close();
        self.dead.push(host);
        deferred.reject(host);
    }).done();

    return deferred.promise;
};

Pool.prototype.monitorConnections = function()
{
    var self = this;
    clearTimeout(this._monitorTimer);

    if (this.closing)
        return P();

    var dead = _.uniq(this.dead);
    this.dead = [];
    var actions = [];

    while (dead.length)
    {
        var host = dead.pop();
        if (this.hasConnectionTo(host))
            continue;

        actions.push(this.resurrect(host));
    }

    this._monitorTimer = setTimeout(this.monitorConnectionsBound, this.monitorInterval);
    return P.allSettled(actions);
};

Pool.prototype.close = function()
{
    var clients = this.clients,
        pendingClients = clients.length,
        self = this;

    this.closing = true;
    if (!pendingClients)
    {
        this.emit('close');
        return;
    }

    function onClose()
    {
        pendingClients--;
        if (!pendingClients)
            self.emit('close');
    }

    _.forEach(clients, function (client)
    {
        client.on('close', onClose).close();
    });
};

Pool.prototype.executeCQLAllClients = function(cqlBuffer, options)
{
    return P.all(_.map(this.clients, function (client)
    {
        return client.executeCQL(cqlBuffer, options);
    }));
};

Pool.prototype.health = function()
{
    var result =
    {
        unhealthy: _.clone(this.dead),
        healthy:   _.map(this.clients, function(c) { return c.host + ':' + c.port; })
    };

    return result;
};

_.each(['execute', 'executeCQL', 'cql', 'createKeyspace', 'dropKeyspace'], function (name)
{
    Pool.prototype[name] = function proxyToConnection()
    {
        var connection = this.getConnection();
        if (connection)
            return connection[name].apply(connection, arguments);
        var deferred = P.defer();
        deferred.reject(errors.create({ 'name': 'NoAvailableNodesException', 'why': 'pool proxy could not find any nodes to proxy through' }));
        return deferred.promise;
    };
});
