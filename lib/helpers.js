var assert = require('assert');

var _ = require('lodash');

var errors = require('./errors');

var reservedWords = ['ADD', 'ALTER', 'AND', 'ANY', 'APPLY', 'ASC', 'AUTHORIZE', 'BATCH', 'BEGIN', 'BY', 'COLUMNFAMILY', 'CREATE', 'DELETE', 'DESC', 'DROP', 'EACH_QUORUM', 'FROM', 'GRANT', 'IN', 'INDEX', 'INSERT', 'INTO', 'KEYSPACE', 'LIMIT', 'LOCAL_QUORUM', 'MODIFY','NORECURSIVE', 'OF', 'ON', 'ONE', 'ORDER', 'PRIMARY', 'QUORUM', 'REVOKE', 'SCHEMA', 'SELECT', 'SET', 'TABLE', 'THREE', 'TOKEN', 'TRUNCATE', 'TWO', 'UPDATE', 'USE', 'USING', 'WHERE', 'WITH'];
var regexUUID = /^[a-f\d]{8}-[a-f\d]{4}-[14][a-f\d]{3}-[89ab][a-f\d]{3}-[a-f\d]{12}$/i;

exports.quote = function quote(string)
{
    if (regexUUID.test(string))
        return string;

    return "'" + string.replace(/\'/g, "''") + "'";
};

function serialize(value, version, stack)
{
    if (value == null)
        return 'NULL';

    if (_.isString(value))
        return exports.quote(value);

    if (_.isNumber(value) || _.isBoolean(value))
        return isFinite(value) ? String(value) : 'NULL';

    if (Buffer.isBuffer(value))
        return value.toString('hex');

    if (_.isDate(value))
        return version === '3.0.0' ? value.getTime() : exports.quote(value.toISOString());

    if (typeof value != 'object' || _.contains(stack, value))
        return 'NULL';

    stack.push(value);
    var results;

    if (Array.isArray(value))
    {
        var length = value.length;
        results = Array(length);

        while (length--)
            results[length] = serialize(value[length], version, stack);

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
            results.push(exports.quote(property) + ':' + serialize(member, version, stack));
        });
        results = '{' + results.join(', ') + '}';
    }

    stack.pop();
    return results;
}

exports.stringify = function stringify(value, version)
{
    return serialize(value, version, []);
};

exports.escape = function escape(query, parameters, version)
{
    assert(_.isString(query), 'The `query` parameter must be a string.');

    if (version == null)
    {
        version = parameters;
        parameters = null;
    }

    var index = 0;

    query = query.replace(/%%/g, '%').replace(/'(\?|%[sjd])'/g, '$1').replace(/\?|%[sjd]/g, function()
    {
        if (!parameters || !parameters.length)
            throw errors.create(new Error('not enough parameters given to satisfy query format string'));

        return exports.stringify(parameters[index++], version);
    });

    if (parameters && index != parameters.length)
        throw errors.create(new Error('too many parameters provided for query format string'));

    return query;
};
