var util = require('util');

var _ = require('lodash');

var Column = require('./column'),
    Marshal = require('./marshal');

var Row = module.exports = function Row(data, schema)
{
    var row = [];
    row.__proto__ = Row.prototype;

    var defaultNameDeserializer = new Marshal(schema.default_name_type || row.nameType),
        defaultValueDeserializer = new Marshal(schema.default_value_type || row.valueType);

    var nameTypes = schema.name_types,
        valueTypes = schema.value_types;

    // Build a name-to-index lookup map for efficiency.
    var nameToIndex = _.reduce(data.columns, function(map, member)
    {
        var name = member.name;

        // `SELECT *` queries contain a vestigial `KEY` column.
        if (name == 'KEY')
            return map;

        // Individual columns may specify custom name and value deserializers.
        // `CompositeType` columns make use of the former.
        var deserializeNameType = nameTypes && nameTypes[name],
            deserializeValueType = valueTypes && valueTypes[name];

        var deserializeName = (deserializeNameType ? new Marshal(deserializeNameType) : defaultNameDeserializer).deserialize,
            deserializeValue = (deserializeValueType ? new Marshal(deserializeValueType) : defaultValueDeserializer).deserialize;

        var deserializedName = deserializeName(name),
            value = schema.noDeserialize ? member.value : deserializeValue(member.value),
            column = new Column(deserializedName, value, new Date(member.timestamp / 1000), member.ttl);

        map[deserializedName] = row.push(column) - 1;
        return map;
    }, {});

    Object.defineProperties(row,
    {
        'nameToIndex': { 'value': nameToIndex },
        'schema': { 'value': schema },
        'key': { 'value': data.key }
    });

    return row;
};

Row.prototype =
{
    '__proto__': Array.prototype,

    get count()
    {
        return this.length;
    }
};

Row.prototype.nameType = Row.prototype.valueType = 'BytesType';

/**
 * Returns a human-readable representation of the row.
 *
 * @returns {String} The string representation.
 */
Row.prototype.toString = Row.prototype.inspect = function toString()
{
    var self = this;

    var columns = [];
    _.forOwn(this.nameToIndex, function(columnIndex, name)
    {
        columns.push(name);
    });

    var key = Array.isArray(this.key) ? this.key.join(':') : this.key;
    return util.format("<Row: Key: '%s', ColumnCount: %s, Columns: [ '%s' ]>", key, this.length, columns);
};

/**
 * Retrieves a column by name.
 *
 * @param {String} name The column name.
 * @returns {Column} The column.
 */
Row.prototype.get = function get(name)
{
    return this[this.nameToIndex[name]];
};

/**
 * Iterates over the row, executing the `callback` function for each column in
 * the `row`. The `callback` is bound to the `context` and invoked with four
 * arguments: `(name, value, timestamp, ttl)`. If the `callback`'s arity is five
 * or greater, it will be invoked with three additional arguments:
 * `(column, index, row)`.
 *
 * @param {Function} callback The callback function.
 * @param {Mixed} [context] The `this` binding of the `callback`.
 */
var forEach = [].forEach;
Row.prototype.forEach = function each(callback, context)
{
    if (callback && typeof context != 'undefined')
        callback = _.bind(callback, context);

    function eachRow(column, index, row)
    {
        var name = column.name,
            value = column.value,
            timestamp = column.timestamp,
            ttl = column.ttl;

        if (callback.length == 4)
            return callback(name, value, timestamp, ttl);

        callback(name, value, timestamp, ttl, column, index, row);
    }

    return forEach.call(this, eachRow);
};

/**
 * Extracts columns from the `start` index up to, but not including, the
 * `end` index.
 *
 * @param {Number} [start] Indicates where to start the selection.
 * @param {Number} [end] Indicates where to end the selection.
 * @returns {Row} The new row.
 */
var slice = [].slice;
Row.prototype.slice = function indexSlice(start, end)
{
    var columns = slice.call(this, start, end);
    return new Row({ 'key': this.key, 'columns': columns }, this.schema);
};

/**
 * Extracts columns from the `start` name up to, but not including, the `end`
 * name. The comparisons are performed lexicographically.
 *
 * @param {String} [start] Indicates where to start the selection.
 * @param {String} [end] Indicates where to end the selection.
 * @returns {Row} The new row.
 */
Row.prototype.nameSlice = function nameSlice(start, end)
{
    var self = this,
        columns = [];

    start == null && (start = ' ');
    end == null && (end = '~');

    _.forOwn(this.nameToIndex, function(index, key)
    {
        if (key >= start && key < end)
            columns.push(self[index]);
    });

    return new Row({ 'key': this.key, 'columns': columns }, this.schema);
};

Row.fromThrift = function fromThrift(key, columns, columnFamily)
{
    var isCounter = columnFamily.isCounter,
        definition = columnFamily.definition;

    var schema =
    {
        'default_value_type': definition.default_validation_class,
        'default_name_type': definition.comparator_type
    };

    var valueTypes = schema.value_types = {},
        columnData = definition.column_metadata;

    if (Array.isArray(columnData))
    {
        _.each(columnData, function(datum)
        {
            valueTypes[datum.name] = datum.validation_class;
        });
    }

    // Counter columns are already deserialized.
    if (isCounter)
        schema.noDeserialize = true;

    var data =
    {
        'key': key,
        'columns': _.map(columns, function(column)
        {
            // TODO: Implement super columns.
            return column[isCounter ? 'counter_column' : 'column'];
        })
    };

    return new Row(data, schema);
};
