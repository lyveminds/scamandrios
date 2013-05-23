var util = require('util'),
    EventEmitter = require('events');

var P = require('p-promise'),
    _ = require('lodash');

var Connection = require('./connection'),
    errors = require('./errors');

var Pool = module.exports = function Pool(options)
{
    options = Object(options);
    this.clients = [];
    this.dead = [];

    var hosts = this.hosts = options.hosts || !options.host ? options.hosts : options.host;
    if (!Array.isArray(hosts))
        throw Error('HelenusError: Invalid hosts supplied for connection pool');
};

function replyNotAvailable()
{
    var deferred = P.defer();
    deferred.reject(errors.create({ 'name': 'NoAvailableNodesException', 'why': 'Could Not Connect To Any Nodes' }));
    return deferred.promise;
}

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

    'connectTo': function connectTo(host)
    {
        var options = _.pick(this, 'keyspace', 'user', 'password', 'timeout', 'cqlVersion', 'consistencylevel').
            self = this;
        options.host = host;
        var connection = new Connection(options);
        connection.on('error', this.emit.bind(this, 'error'));
        return connection;
    },

    'connect': function connect()
    {
        var deferred = P.defer(),
            self = this;
        var hosts = this.hosts,
            poolSize = hosts.length * this.hostPoolSize,
            establishedConnections = 0,
            activeConnections = 0;

        function beginMonitoring()
        {
            if (establishedConnections == poolSize)
            {
                if (!activeConnections)
                    deferred.reject(errors.create({ 'name': 'NoAvailableNodesException', 'why': 'Could Not Connect To Any Nodes' }));
                self.monitorConnections();
            }
        }

        function connect(host)
        {
            var connection = self.connectTo(host);
            connection.connect().then(function (keyspace)
            {
                establishedConnections++;
                activeConnections++;
                if (keyspace)
                    keyspace.connection = self;
                self.clients.push(connection);
                if (activeConnections >= 1)
                    deferred.resolve();
                if (self.closing)
                    connection.close();
                beginMonitoring();
            }, function (error)
            {
                establishedConnections++;
                self.dead.push(host);
                beginMonitoring();
            });
        }

        for (var index = 0, length = hosts.length; index < length; index++)
        {
            for (var poolNode = this.hostPoolSize; poolNode--;)
            {
                connect(hosts[index]);
            }
        }
        return deferred.promise;
    },

    'use': function use(keyspace)
    {
        var self = this;
        var clients = _.map(this.clients, function (client)
        {
            return client.use(keyspace).then(function (keyspace)
            {
                if (keyspace)
                    keyspace.connection = self;
            });
        });
        return P.all(clients);
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
        for (var clients = this.clients, length = clients.length; length--;)
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

        function connect(host)
        {
            var connection = self.connectTo(host);
            connection.connect().then(function ()
            {
                self.clients.push(connection);
            }, function ()
            {
                self.dead.push(host);
            });
        }

        function checkDead()
        {
            if (self.closing)
                return;
            var dead = self.dead;
            if (dead.length)
            {
                return connect(dead.pop()).then(checkDead);
            }
            self.retryInterval = setTimeout(checkDead, 5000);
        }

        this.retryInterval = setTimeout(checkDead, 5000);
    },

    'close': function close()
    {
        var clients = this.clients,
            pendingClients = this.clients.length,
            self = this;

        this.closing = true;
        clearTimeout(this.retryInterval);

        if (!length)
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

        for (var length = pendingClients; length--;)
        {
            var client = clients[length];
            client.on('close', onClose).close();
        }
    }
});

_.each(['execute', 'cql', 'createKeyspace', 'dropKeyspace'], function (name)
{
    Connection.prototype[name] = function proxyToConnection()
    {
        var connection = this.getConnection();
        return connection ? connection[name].apply(connection, arguments) : replyNotAvailable();
    };
});