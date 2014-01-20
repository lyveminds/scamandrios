var util = require('util'),
    EventEmitter = require('events').EventEmitter,
    zlib = require('zlib');

var thrift = require('helenus-thrift'),
    P = require('p-promise'),
    _ = require('lodash');

var Cassandra = require('./cassandra/Cassandra'),
    ttype = require('./cassandra/cassandra_types'),
    Row = require('./row'),
    Keyspace = require('./keyspace'),
    errors = require('./errors'),
    helpers = require('./helpers');


var Connection = module.exports = function Connection(options)
{
    options = Object(options);
    if (!options.port && options.host && options.host.indexOf(':') > -1)
    {
        var split = options.host.split(':');
        options.host = split[0];
        options.port = split[1];
    }
    _.extend(this, options, function (destinationValue, sourceValue)
    {
        return sourceValue == null ? destinationValue : sourceValue;
    });

    this.proxyErrorBound = _.bindKey(this, 'proxyError');
    this.pingBound = _.bindKey(this, 'ping');
};

util.inherits(Connection, EventEmitter);

Connection.TTL = 20 * 60 * 1000;
Connection.PING_QUERY = new Buffer('SELECT * FROM system.schema_keyspaces;');

Connection.prototype.port = 9160;
Connection.prototype.host = 'localhost';

Connection.prototype.timeout = 3000;
Connection.prototype._connectTimeout = null;

Connection.prototype.pingInterval = 30000;
Connection.prototype._pingTimer = null;
Connection.prototype.healthy = false;

Connection.prototype.consistencylevel = 1;
Connection.prototype.ready = false;

Connection.prototype.user = Connection.prototype.password = null;
Connection.prototype.keyspace = null;
Connection.prototype.cqlVersion = '3.0.0';

Connection.prototype.proxyError = function proxyError(error)
{
    this.healthy = false;
    this.emit('error', error);
};

function createDeflate(buffer)
{
    var deferred = P.defer();
    zlib.deflate(buffer, function (error, result)
    {
        if (error)
            return deferred.reject(error);
        deferred.resolve(result);
    });
    return deferred.promise;
}

Connection.prototype.connect = function connect()
{
    var deferred = P.defer(),
        self = this;

    var connection = this._connection = thrift.createConnection(this.host, this.port),
        client = this._client = thrift.createClient(Cassandra, connection);

    connection.on('error', onError);
    function onError(error)
    {
        var connection = self._connection;

        connection.removeListener('error', onError);
        connection.removeListener('close', onClose);
        connection.removeListener('connect', onConnect);

        clearTimeout(self._connectTimeout);
        connection.connection.destroy();

        self.healthy = false;
        deferred.reject(error);
    }

    connection.on('close', onClose);
    function onClose()
    {
        var connection = self._connection;

        connection.removeListener('error', onError);
        connection.removeListener('connect', onConnect);

        clearTimeout(self._connectTimeout);
        self.ready = false;
        self.healthy = false;
        self.emit('close');
    }

    connection.on('connect', onConnect);
    function onConnect(error)
    {
        connection.removeListener('connect', onConnect);

        if (error)
        {
            connection.removeListener('error', onError);
            return deferred.reject(error);
        }

        self.authenticate().then(function ()
        {
            self.emit('log', 'successful authentication');

            connection.removeListener('error', onError);
            connection.on('error', self.proxyErrorBound);

            clearTimeout(self._connectTimeout);
            return self.cqlVersion ? self.setCqlVersion() : P();
        }).then(function()
        {
            return self.selectCqlVersion().fail(deferred.reject);
        })
        .then(function(keyspace)
        {
            self._pingTimer = setTimeout(self.pingBound, self.pingInterval);
            self.healthy = true;
            deferred.resolve(keyspace);
        })
        .fail(function(error)
        {
            self.emit('log', 'authentication error; destroying connection');

            clearTimeout(self._connectTimeout);
            connection.removeListener('error', self.proxyErrorBound);

            var socket = connection.connection;
            socket.destroy();
            deferred.reject(error);
        }).done();
    }

    this._connectTimeout = setTimeout(function ()
    {
        deferred.reject(errors.create({ name: 'TimeoutException', why: 'Connection Timed Out' }));
        connection.connection.destroy();
    }, this.timeout);

    return deferred.promise;
};

Connection.prototype.refreshConnection = function refreshConnection()
{
    var self = this;

    self.emit('log', 'handling connection end-of-life');

    self._connection.removeAllListeners();
    self._connection.end();
    self._connection.connection.destroy();

    self.connect()
    .then(function()
    {
        self.emit('log', 'successfully reconnected after connection end-of-life');
        return 'OK';
    })
    .fail(function(err)
    {
        self.emit('log', 'reconnection error; throwing', err.message);
        throw(err);
    }).done();
};

Connection.prototype.authenticate = function authenticate()
{
    var deferred = P.defer(),
        self = this;
    var user = this.user, password = this.password;
    if (!user && !password)
    {
        deferred.resolve();
        return deferred.promise;
    }
    var authRequest = new ttype.AuthenticationRequest({ 'credentials': { 'username': user, 'password': password }});
    this._client.login(authRequest, function (error)
    {
        if (error)
            return deferred.reject(errors.create(error));
        deferred.resolve();
    });
    return deferred.promise;
};

Connection.prototype.setCqlVersion = function setCqlVersion()
{
    var deferred = P.defer(),
        connection = this._connection.connection;
    this._client.set_cql_version(this.cqlVersion, function (error)
    {
        if (error)
        {
            connection.destroy();
            return deferred.reject(errors.create(error));
        }
        deferred.resolve();
    });
    return deferred.promise;
};

Connection.prototype.selectCqlVersion = function selectCqlVersion()
{
    var deferred = P.defer(),
        self = this;
    this.ready = true;
    this._client.describe_version(function (error, version)
    {
        if (error)
            return deferred.reject(error);
        self.version = version.split('.');
        deferred.resolve();
    });
    return deferred.promise.then(function ()
    {
        var keyspace = self.keyspace;
        if (keyspace == null)
            return;
        return self.use(keyspace);
    });
};

Connection.prototype.execute = function execute()
{
    var args = [].slice.call(arguments, 0),
        deferred = P.defer(),
        command = args.shift();
    args.push(function onReturn(error, results)
    {
        if (error)
            return deferred.reject(error);
        deferred.resolve(results);
    });
    var client = this._client;
    client[command].apply(client, args);
    return deferred.promise;
};

Connection.prototype.executeCQL = function executeCQL(cqlBuffer, options)
{
    var deferred = P.defer(),
        self = this;

    options || (options = {});

    var useGzip = options.gzip === true,
        query = (useGzip ? createDeflate : P)(cqlBuffer);

    var queryResult = query.then(function (queryString)
    {
        var args =
        [
            queryString,
            ttype.Compression[useGzip ? 'GZIP' : 'NONE']
        ];
        if (self.cqlVersion == '3.0.0' && self.version[0] == '19' && self.version[1] > '33')
        {
            args.unshift('execute_cql3_query');
            args.push(self.consistencylevel);
        }
        else
        {
            args.unshift('execute_cql_query');
        }

        return self.execute.apply(self, args);
    });

    return queryResult.then(function (result)
    {
        switch (result.type)
        {
            case ttype.CqlResultType.ROWS:
                var schema = result.schema;
                return _.map(result.rows, function (row)
                {
                    return new Row(row, schema);
                });
            case ttype.CqlResultType.INT:
                return result.num;
            case ttype.CqlResultType.VOID:
                return null;
        }
    });
};

Connection.prototype.cql = function cql(cmd, args, options)
{
    if (!Array.isArray(args))
    {
        options = args;
        args = null;
    }

    var cqlBuffer = new Buffer(args ? helpers.escape(cmd, args, this.cqlVersion) : cmd);
    return this.executeCQL(cqlBuffer, options);
};

Connection.prototype.useKeyspace = function use(keyspace)
{
    var deferred = P.defer(),
        self = this;
    this._client.describe_keyspace(keyspace, function (error, definition)
    {
        if (error)
            return deferred.reject(errors.create(error));
        self._client.set_keyspace(keyspace, function (error)
        {
            if (error)
                return deferred.reject(errors.create(error));
            deferred.resolve(new Keyspace(self, definition));
        });
    });
    return deferred.promise;
};
Connection.prototype.use = Connection.prototype.useKeyspace;

// Uses if it already exists, creates if it does not.
Connection.prototype.assignKeyspace = function assignKeyspace(keyspace, options)
{
    var self = this;

    return self.useKeyspace(keyspace)
    .fail(function(err)
    {
        if (err.name !== 'ScamandriosNotFoundException')
            throw err;

        return self.createKeyspace(keyspace, options).then(function()
        {
            return self.useKeyspace(keyspace);
        });
    }).then(function(ks)
    {
        return ks;
    });
};

Connection.prototype.createKeyspace = function createKeyspace(keyspace, options)
{
    var deferred = P.defer(),
        self = this;

    if (!keyspace)
    {
        deferred.reject(errors.create({ 'name': 'InvalidNameError', 'why': 'Keyspace name not specified' }));
        return deferred.promise;
    }

    options = Object(options);
    var args =
    {
        'name': keyspace,
        'strategy_class': options.strategyClass || 'SimpleStrategy',
        'strategy_options': options.strategyOptions || {},
        'replication_factor': options.replication || 1,
        'durable_writes': options.durable || true,
        'cf_defs': []
    };
    if (args.strategy_class === 'SimpleStrategy' && !args.strategy_options.replication_factor)
        args.strategy_options.replication_factor = '' + args.replication_factor;

    var ksdef = new ttype.KsDef(args);
    this._client.system_add_keyspace(ksdef, function (error, response)
    {
        if (error)
            return deferred.reject(errors.create(error));
        deferred.resolve(response);
    });
    return deferred.promise;
};

Connection.prototype.dropKeyspace = function dropKeyspace(keyspace, options)
{
    var deferred = P.defer();
    if (!keyspace)
    {
        deferred.reject(errors.create({ 'name': 'InvalidNameError', 'why': 'Keyspace name not specified' }));
        return deferred.promise;
    }
    this._client.system_drop_keyspace(keyspace, function (error, response)
    {
        if (error)
            return deferred.reject(errors.create(error));
        deferred.resolve(response);
    });
    return deferred.promise;
};

Connection.prototype.close = function close()
{
    clearTimeout(this._connectTimeout);
    clearTimeout(this._pingTimer);
    clearTimeout(this._pingTimeout);

    this._connection.removeAllListeners();
    this._connection.end();
    this._connection.connection.destroy();

};

Connection.prototype.setHealthy = function setHealthy(healthy)
{
    healthy = !!healthy;
    if (this.healthy != healthy)
    {
        this.healthy = healthy;
        this.emit('health', this);
    }
};

Connection.prototype.ping = function ping()
{
    var self = this,
        deferred = P.defer();

    if (this._isPinging)
        return;

    this._isPinging = true;
    this._pingTimeout = setTimeout(function()
    {
        self._isPinging = false;
        self.setHealthy(false);
        deferred.reject(new Error('Ping timed out.'));
    }, this.timeout);

    function updateHealth(healthy)
    {
        clearTimeout(self._pingTimeout);
        self._isPinging = false;
        self.setHealthy(healthy);
        self._pingTimer = setTimeout(self.pingBound, self.pingInterval);
    }

    self.executeCQL(Connection.PING_QUERY)
    .then(function()
    {
        updateHealth(true);
        deferred.resolve('good');
    }, function(err)
    {
        updateHealth(false);
        deferred.reject(err);
    })
    .done();

    return deferred.promise;
};
