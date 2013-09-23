var util = require('util'),
    EventEmitter = require('events').EventEmitter;

var P = require('p-promise'),
    _ = require('lodash');

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
                self.emit('log', 'connection established to cassandra@' + host);
                self.clients.push(connection);
                if (self.closing)
                    connection.close();
            }, function (error)
            {
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

Pool.prototype.monitorConnections = function()
{
    var self = this;
    if (this.closing)
        return;

    function checkDead()
    {
        if (self.closing)
            return;
        var dead = self.dead;
        if (dead.length)
        {
            var host = dead.pop(),
                connection = self.createConnection(host);
            return connection.connect()
            .then(function()
            {
                self.emit('log', 'cassandra host rising from the dead: ' + host);
                self.clients.push(connection);
            })
            .fail(function(err)
            {
                // self.emit('log', 'cassandra host still dead: ' + host + ':' + JSON.stringify(err));
                dead.push(host);
            }).done();
        }
    }

    function ping(client)
    {
        var timeout;

        function addToDeadPool()
        {
            var deadhost = client.host + ':' + client.port;
            self.emit('log', 'ping timeout to cassandra@' + deadhost);
            self.dead.push(deadhost);
            client.close();
            client.ready = false;
        }

        client.executeCQL(Pool.PING_QUERY)
        .then(function(result)
        {
            clearTimeout(timeout);
            // self.emit('log', 'good ping to cassandra@' + client.host + ':' + client.port);
        })
        .fail(function(error)
        {
            self.emit('log', 'connection error to cassandra@' + client.host + ':' + client.port + ' ' + JSON.stringify(error));
            clearTimeout(timeout);
            addToDeadPool();
        }).done();

        timeout = setTimeout(addToDeadPool, 7500);
    }

    _.each(self.clients, ping);
    if (self.dead.length)
        checkDead();
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
