var assert = require('assert');

var _ = require('lodash'),
    semver = require('semver');

var helpers = require('./helpers');

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

var reservedMap = { "ADD":1, "ALTER":1, "AND":1, "ANY":1, "APPLY":1, "ASC":1, "AUTHORIZE":1, "BATCH":1, "BEGIN":1, "BY":1, "COLUMNFAMILY":1, "CREATE":1, "DELETE":1, "DESC":1, "DROP":1, "EACH_QUORUM":1, "FROM":1, "GRANT":1, "IN":1, "INDEX":1, "INSERT":1, "INTO":1, "KEYSPACE":1, "LIMIT":1, "LOCAL_QUORUM":1, "MODIFY":1, "NORECURSIVE":1, "OF":1, "ON":1, "ONE":1, "ORDER":1, "PRIMARY":1, "QUORUM":1, "REVOKE":1, "SCHEMA":1, "SELECT":1, "SET":1, "TABLE":1, "THREE":1, "TOKEN":1, "TRUNCATE":1, "TWO":1, "UPDATE":1, "USE":1, "USING":1, "WHERE":1, "WITH":1}; 
var types = { 'ascii': 1, 'bigint': 1, 'varint': 1, 'int': 1, 'blob': 1, "boolean":1, "counter":1, "decimal":1, "double":1, "float":1, "inet":1, "text":1, "timestamp":1, "timeuuid":1, "uuid":1, "varchar":1 };

var patternCassandraTypes = 'ascii|(?:big|var)?int|blob|boolean|counter|decimal|double|float|inet|text|time(?:stamp|uuid)|uuid|varchar';

var regexType = new RegExp(patternCassandraTypes, 'i'),
    collectionType = new RegExp('^(set|list)\\s*<\\s*(' + patternCassandraTypes + ')\\s*>$', 'i'),
    mapType = new RegExp('^map\\s*<\\s*(' + patternCassandraTypes + ')\\s*,\\s*(' + patternCassandraTypes + ')\\s*>$', 'i');

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

function singleQuote(string)
{
    return "'" + string.replace(/\'/g, "''") + "'";
}

function doubleQuote(string)
{
    return '"' + string.replace(/\"/g, '""') + '"';
}

Query.prototype.serializePrimitive = function serializePrimitive(value, type)
{
    if (_.isString(value))
    {
        if (type == 'text')
            return singleQuote(value);

        if (type == 'uuid' || type == 'timeuuid')
            return value;
    }

    if (_.isNumber(value) || _.isBoolean(value))
        return isFinite(value) ? String(value) : 'NULL';

    if (Buffer.isBuffer(value))
        return value.toString('hex');

    if (_.isDate(value))
        return semver.gte(this.version, '3.0.0') ? value.getTime() : singleQuote(value.toISOString());
};

Query.prototype.serializeSet = function serializeSet(value, type)
{
    assert(Array.isArray(value), 'A set must be an array.');

    var length = value.length,
        results = Array(length);

    while (length--)
    {
        results[length] = this.serializePrimitive(value[length], type);
    }

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

    assert(_.isObject(map), 'A map must be an array.');

    var results = [];
    _.forOwn(map, function(value, key)
    {
        results.push(self.serializePrimitive(key, keyType) + ':' + self.serializePrimitive(value, valueType));
    });

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
            return reservedMap[String(value).toUpperCase()] ? doubleQuote(value) : value;

        var collectionParts = collectionType.exec(type);
        if (collectionParts)
        {
            var isSet = collectionParts[1] == 'set';
            return self[isSet ? 'serializeSet' : 'serializeList'](value, collectionParts[2]);
        }

        var mapParts = mapType.exec(type);
        if (mapParts)
            return self.serializeMap(value, mapParts[1], mapParts[2]);

        if (types[type])
            return self.serializePrimitive(value, type);

        throw new TypeError('Unrecognized type: ' + type);
    });

    return this.cached;
};

Query.prototype.toBuffer = function toBuffer()
{
    return new Buffer(this.toString());
};
