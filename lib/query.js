var assert = require('assert');

var _ = require('lodash'),
    semver = require('semver');

function singleQuote(string)
{
    return "'" + String(string).replace(/\'/g, "''") + "'";
}

function doubleQuote(string)
{
    return '"' + String(string).replace(/\"/g, '""') + '"';
}

var Query = module.exports = function Query(query, options)
{
    assert(query, 'The `query` argument is required.');
    this.raw = query;

    if (options)
    {
        if (options.params)
            this.params(options.params);

        if (options.types)
            this.types(options.types);

        if (semver.valid(options.version))
            this.version = options.version;
    }

    this.nameToValue = {};
    this.nameToType = {};
    this.needsInterpolation = false;
    this.cached = '';
};

Query.paramPattern = /\{([^}]+?)\}/g;

Query.reservedMap =
{
    'ADD': 1, 'ALTER': 1, 'AND': 1, 'ANY': 1, 'APPLY': 1, 'ASC': 1,
    'AUTHORIZE': 1, 'BATCH': 1, 'BEGIN': 1, 'BY': 1, 'COLUMNFAMILY': 1,
    'CREATE': 1, 'DELETE': 1, 'DESC': 1, 'DROP': 1, 'EACH_QUORUM': 1,
    'FROM': 1, 'GRANT': 1, 'IN': 1, 'INDEX': 1, 'INSERT': 1, 'INTO': 1,
    'KEYSPACE': 1, 'LIMIT': 1, 'LOCAL_QUORUM': 1, 'MODIFY': 1, 'NORECURSIVE': 1,
    'OF': 1, 'ON': 1, 'ONE': 1, 'ORDER': 1, 'PRIMARY': 1, 'QUORUM': 1,
    'REVOKE': 1, 'SCHEMA': 1, 'SELECT': 1, 'SET': 1, 'TABLE': 1, 'THREE': 1,
    'TOKEN': 1, 'TRUNCATE': 1, 'TWO': 1, 'UPDATE': 1, 'USE': 1, 'USING': 1,
    'WHERE': 1, 'WITH': 1, 'LOCALE': 1
};

Query.columnTypes =
{
    'ascii': 1, 'bigint': 1, 'varint': 1, 'int': 1, 'blob': 1, 'boolean': 1,
    'counter': 1, 'decimal': 1, 'double': 1, 'float': 1, 'inet': 1, 'text': 1,
    'timestamp': 1, 'timeuuid': 1, 'uuid': 1, 'varchar': 1
};

var columnTypesPattern = 'ascii|(?:big|var)?int|blob|boolean|counter|decimal|double|float|inet|text|time(?:stamp|uuid)|uuid|varchar';

Query.collectionTypePattern = new RegExp('^(set|list)\\s*<\\s*(' + columnTypesPattern + ')\\s*>$', 'i');
Query.mapTypePattern = new RegExp('^map\\s*<\\s*(' + columnTypesPattern + ')\\s*,\\s*(' + columnTypesPattern + ')\\s*>$', 'i');

Query.prototype.raw = '';
Query.prototype.version = '3.0.0';
Query.prototype.needsInterpolation = false;

Query.prototype.params = function params(object)
{
    _.assign(this.nameToValue, object);
    this.needsInterpolation = true;
    return this;
};

Query.prototype.types = function types(object)
{
    _.assign(this.nameToType, object);
    this.needsInterpolation = true;
    return this;
};

Query.prototype.serializePrimitive = function serializePrimitive(value, type)
{
    assert(type, 'A type is required.');

    switch (type)
    {
        case 'uuid':
        case 'timeuuid':
        case 'inet':
            return value;

        case 'text':
        case 'varchar':
            return singleQuote(value);

        case 'ascii':
        case 'blob':
            var encoding = type == 'blob' ? 'hex' : 'ascii';
            return (Buffer.isBuffer(value) ? value : new Buffer(value)).toString(encoding);

        case 'double':
        case 'int':
        case 'varint':
        case 'bigint':
        case 'float':
        case 'decimal':
        case 'counter':
            return isFinite(value) ? String(value) : 'NULL';

        case 'boolean':
            return String(value);

        case 'timestamp':
            var timestamp = +value;

            if (!isFinite(timestamp))
                return 'NULL';

            return semver.gte(this.version, '3.0.0') ? timestamp : singleQuote(new Date(timestamp).toISOString());
    }
};

Query.prototype.serializeSet = function serializeSet(value, type)
{
    assert(Array.isArray(value), 'A set must be an array.');

    var length = value.length,
        results = Array(length);

    while (length--)
        results[length] = this.serializePrimitive(value[length], type);

    return '{' + results.join(', ') + '}';
};

Query.prototype.serializeList = function serializeList(value, type)
{
    assert(Array.isArray(value), 'A list must be an array.');

    var length = value.length,
        results = Array(length);

    while (length--)
        results[length] = this.serializePrimitive(value[length], type);

    return '[' + results.join(', ') + ']';
};

Query.prototype.serializeMap = function serializeMap(map, keyType, valueType)
{
    var self = this;

    assert(_.isObject(map), 'A map must be an object.');

    var results = Object.keys(map);

    for (var length = results.length; length--;)
    {
        var key = results[length];
        results[length] = self.serializePrimitive(key, keyType) + ':' + self.serializePrimitive(map[key], valueType);
    }

    return '{' + results.join(', ') + '}';
};

Query.prototype.toString = function toString()
{
    var self = this;

    if (!this.needsInterpolation)
        return this.cached;

    this.cached = this.raw.replace(Query.paramPattern, function(match, param)
    {
        var value = self.nameToValue[param];

        if (value == null)
            return 'NULL';

        var type = self.nameToType[param];
        if (!type)
            return Query.reservedMap[String(value).toUpperCase()] ? doubleQuote(value) : value;

        var collectionParts = Query.collectionTypePattern.exec(type);
        if (collectionParts)
        {
            var isSet = collectionParts[1] == 'set';
            return self[isSet ? 'serializeSet' : 'serializeList'](value, collectionParts[2]);
        }

        var mapParts = Query.mapTypePattern.exec(type);
        if (mapParts)
            return self.serializeMap(value, mapParts[1], mapParts[2]);

        if (Query.columnTypes[type])
            return self.serializePrimitive(value, type);

        throw new TypeError('Unrecognized type: ' + type);
    });

    this.needsInterpolation = false;
    return this.cached;
};

Query.prototype.toBuffer = function toBuffer()
{
    return new Buffer(this.toString());
};

Query.prototype.execute = function execute(connection, options)
{
    return connection.executeCQL(this.toBuffer(), options);
};
