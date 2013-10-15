var
    _            = require('lodash'),
    EventEmitter = require('events').EventEmitter,
    P            = require('p-promise'),
    util         = require('util')
    ;

var Connection = require('./connection'),
    errors = require('./errors');

var MONITOR_INTERVAL = 30000;

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
};
util.inherits(Pool, EventEmitter);

Pool.prototype.TTL = Connection.TTL;

Pool.prototype.hostPoolSize     = 1;
Pool.prototype.closing          = false;
Pool.prototype.consistencylevel = 1;

Pool.prototype.hosts            = null;
Pool.prototype.clients          = null;
Pool.prototype.dead             = null;
Pool.prototype.timeout          = null;
Pool.prototype.monitorInterval     = null;

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
    connection.on('error', _.bind(this.emit, this, 'error'));
    return connection;
};

Pool.prototype.connect = function()
{
    if (this.monitorInterval)
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
            deferred.resolve(value);
        if (!pendingCount)
        {
            deferred.reject(errors.create({ 'name': 'NoAvailableNodesException', 'why': 'Could Not Connect To Any Nodes' }));
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

    this.monitorInterval = setInterval(this.monitorConnections.bind(this), MONITOR_INTERVAL);
    return deferred.promise;
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
        this.emit('error', errors.create({ 'name': 'NoAvailableNodesException', 'why': 'No Available Connections' }));
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
        P.reject(host);
    }).done();

    return deferred.promise;
}


Pool.prototype.monitorConnections = function()
{
    var self = this;
    if (this.closing || this.checkInProgress)
        return P('in progress');

    this.checkInProgress = true;
    var deferred = P.defer();

    function checkDead()
    {
        if (self.closing)
            return P();

        var dead = _.uniq(self.dead);
        self.dead = [];
        var actions = [];

        while (dead.length)
        {
            var host = self.dead.pop();
            if (!self.hasConnectionTo(host))
                continue;

            actions.push(self.resurrect(host));
        }

        return P.allSettled(actions);
    }

    function ping(client)
    {
        var timeout, d2 = P.defer();

        function addToDeadPool()
        {
            var deadhost = client.host + ':' + client.port;
            self.emit('log', 'ping timeout to cassandra@' + deadhost);
            self.dead.push(deadhost);
            client.close();
            client.ready = false;
            d2.resolve('bad');
        }

        client.executeCQL(Pool.PING_QUERY)
        .then(function(result)
        {
            clearTimeout(timeout);
            // self.emit('log', 'good ping to cassandra@' + client.host + ':' + client.port);
            d2.resolve('ok');
        })
        .fail(function(error)
        {
            self.emit('log', 'connection error to cassandra@' + client.host + ':' + client.port + ' ' + JSON.stringify(error));
            clearTimeout(timeout);
            addToDeadPool();
        }).done();

        timeout = setTimeout(addToDeadPool, 7500);
        return d2.promise;
    }

    var pings = _.map(self.clients, function(c) { return ping(c); } );

    P.all(pings)
    .then(function()
    {
        if (!self.dead.length)
            return true;

        return checkDead();
    })
    .then(function()
    {
        self.checkInProgress = false;
        deferred.resolve('monitor good');
    }).done();

    return deferred.promise;
};

Pool.prototype.close = function()
{
    var clients = this.clients,
        pendingClients = clients.length,
        self = this;

    this.closing = true;
    clearInterval(this.monitorInterval);

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
        deferred.reject(errors.create({ 'name': 'NoAvailableNodesException', 'why': 'Could Not Connect To Any Nodes' }));
        return deferred.promise;
    };
});
