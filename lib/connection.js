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
    errors = require('./errors');

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
};

util.inherits(Connection, EventEmitter);

Connection.prototype.port = 9160;
Connection.prototype.host = 'localhost';
Connection.prototype.timeout = 3000;
Connection.prototype.consistencylevel = 1;
Connection.prototype.ready = false;

Connection.prototype.user = Connection.prototype.password = null;
Connection.prototype.keyspace = null;
Connection.prototype.cqlVersion = '3.0.0';


var reservedWords = ['ADD', 'ALTER', 'AND', 'ANY', 'APPLY', 'ASC', 'AUTHORIZE', 'BATCH', 'BEGIN', 'BY', 'COLUMNFAMILY', 'CREATE', 'DELETE', 'DESC', 'DROP', 'EACH_QUORUM', 'FROM', 'GRANT', 'IN', 'INDEX', 'INSERT', 'INTO', 'KEYSPACE', 'LIMIT', 'LOCAL_QUORUM', 'MODIFY','NORECURSIVE', 'OF', 'ON', 'ONE', 'ORDER', 'PRIMARY', 'QUORUM', 'REVOKE', 'SCHEMA', 'SELECT', 'SET', 'TABLE', 'THREE', 'TOKEN', 'TRUNCATE', 'TWO', 'UPDATE', 'USE', 'USING', 'WHERE', 'WITH'];

function quote(string)
{
    return "'" + string.replace(/\'/g, "''") + "'";
}

function escapeType(value, stack, version)
{
    if (value == null)
        return 'NULL';
    if (_.isString(value))
        return quote(value);
    if (_.isNumber(value) || _.isBoolean(value))
        return isFinite(value) ? String(value) : 'NULL';
    if (Buffer.isBuffer(value))
        return value.toString('hex');
    if (_.isDate(value))
    {
        if (version === '3.0.0')
            return value.getTime();
        else
            return quote(value.toISOString());
    }

    if (typeof value != 'object' || _.contains(stack, value))
        return 'NULL';

    stack.push(value);
    var results;

    if (Array.isArray(value))
    {
        var length = value.length;
        results = Array(length);

        while (length--)
            results[length] = escapeType(value[length], stack);

        if (value._iset)
            results = '{' + results.join(', ') + '}';
        else
            results = '[' + results.join(', ') + ']';
    }
    else
    {
        results = [];
        _.forOwn(value, function(member, property)
        {
            results.push(quote(property) + ':' + escapeType(member, stack));
        });
        results = '{' + results.join(', ') + '}';
    }

    stack.pop();
    return results;
}

function escapeCQL(val, version)
{
    return escapeType(val, [], version);
}

function formatCQL(cql, params, version)
{
    params = params.slice(0);

    //replace a %% with a % to maintain backward compatibility with util.format
    cql = cql.replace(/%%/, '%');

    //remove existing quotes around parameters in case the user has already wrapped them
    cql = cql.replace(/'(\?|%[sjd])'/g, '$1');

    //escape the params and format the CQL string
    cql = cql.replace(/\?|%[sjd]/g, function ()
    {
        if (!params.length)
            throw errors.create(new Error('not enough parameters given to satisfy query format string'));
        return escapeCQL(params.shift(), version);
    });

    if (params.length)
        throw errors.create(new Error('too many parameters provided for query format string'));

    return cql;
}

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
        self = this,
        timer;
    var connection = this._connection = thrift.createConnection(this.host, this.port);
    connection.on('error', function (error)
    {
        clearTimeout(timer);
        deferred.reject(error);
    });
    connection.on('close', function ()
    {
        clearTimeout(timer);
        self.ready = false;
        self.emit('close');
    });
    var client = this._client = thrift.createClient(Cassandra, connection);
    connection.on('connect', function (error)
    {
        if (error)
            return deferred.reject(error);
        connection.removeAllListeners('error');
        connection.on('error', _.bind(self.emit, self, 'error'));
        self.authenticate().then(function ()
        {
            clearTimeout(timer);
            return self.cqlVersion ? self.setCqlVersion() : P();
        }).then(function ()
        {
            return self.selectCqlVersion().then(deferred.resolve, deferred.reject);
        }, function (error)
        {
            clearTimeout(timer);
            connection.connection.destroy();
            deferred.reject(error);
        });
    });
    timer = setTimeout(function ()
    {
        deferred.reject(errors.create({ name: 'TimeoutException', why: 'Connection Timed Out'}));
        connection.connection.destroy();
    }, this.timeout);
    return deferred.promise;
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

Connection.prototype.cql = function cql(cmd, args, options)
{
    var deferred = P.defer(),
        self = this;

    if (!Array.isArray(args))
    {
        options = args;
        args = null;
    }
    options = options || {};

    var cqlBuffer = new Buffer(args ? formatCQL(cmd, args, this.cqlVersion) : cmd);
    // console.log(cqlBuffer.toString());

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
    this._connection.end();
};
