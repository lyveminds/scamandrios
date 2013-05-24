var util = require('util'),
    EventEmitter = require('events').EventEmitter;

var P = require('p-promise'),
    _ = require('lodash');

var Connection = require('./connection'),
    errors = require('./errors');

var Pool = module.exports = function Pool(options)
{
    options = Object(options);
    this.clients = [];
    this.dead = [];

    var hosts = options.hosts = 'hosts' in options || !('host' in options) ? options.hosts : [options.host];
    if (!Array.isArray(hosts))
        throw Error('HelenusError: Invalid hosts supplied for connection pool');

    _.extend(this, options, function (destinationValue, sourceValue)
    {
        return sourceValue == null ? destinationValue : sourceValue;
    });
};

util.inherits(Pool, EventEmitter);

_.extend(Pool.prototype,
{
    'hostPoolSize': 1,
    'closing': false,
    'consistencylevel': 1,

    'hosts': null,
    'clients': null,
    'dead': null,
    'timeout': null,
    'retryInterval': null,

    'keyspace': null,
    'cqlVersion': null,

    'user': null,
    'password': null,

    'getHost': function getHost(clients)
    {
        return clients[Math.floor(Math.random() * clients.length)];
    },

    'createConnection': function createConnection(host)
    {
        var options = _.pick(this, 'keyspace', 'user', 'password', 'timeout', 'cqlVersion', 'consistencylevel');
        options.host = host;
        var connection = new Connection(options);
        connection.on('error', _.bind(this.emit, this, 'error'));
        return connection;
    },

    'connect': function connect()
    {
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
                self.monitorConnections();
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
                    if (self.closing)
                        connection.close();
                }, function (error)
                {
                    handleSettle(false);
                    self.dead.push(host);
                });
            });
        });

        return deferred.promise;
    },

    'use': function use(keyspace)
    {
        var self = this;
        return P.allSettled(_.map(this.clients, function (client)
        {
            return client.use(keyspace).then(function (keyspace)
            {
                if (keyspace)
                    keyspace.connection = self;
            });
        }));
    },

    'getConnection': function getConnection(getHost)
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
                this.dead.push(client.host + ':' + client.port);
            }
        }
        return this.getConnection(getHost);
    },

    'monitorConnections': function monitorConnections()
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
                return connection.connect().then(function ()
                {
                    self.clients.push(connection);
                }, function ()
                {
                    dead.push(host);
                    checkDead();
                });
            }
            self.retryInterval = setTimeout(checkDead, 5000);
        }
        this.retryInterval = setTimeout(checkDead, 5000);
    },

    'close': function close()
    {
        var clients = this.clients,
            pendingClients = clients.length,
            self = this;

        this.closing = true;
        clearTimeout(this.retryInterval);

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
    }
});

_.each(['execute', 'cql', 'createKeyspace', 'dropKeyspace'], function (name)
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
