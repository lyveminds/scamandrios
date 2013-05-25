var
    _             = require('lodash'),
    Column        = require('./column'),
    CounterColumn = require('./counter_column'),
    Marshal       = require('./marshal'),
    P             = require('p-promise'),
    Row           = require('./row'),
    ttype         = require('./cassandra/cassandra_types'),
    util          = require('util')
    ;

var DEFAULT_READ_CONSISTENCY  = ttype.ConsistencyLevel.QUORUM;
var DEFAULT_WRITE_CONSISTENCY = ttype.ConsistencyLevel.QUORUM;

/**
 * Returns a column parent
 * When calculating the column parent of a standard column family,
 * the parent is simply the column family name.  When dealing with
 * super columns on the other hand, an optional name parameter may
 * be provided.
 *
 * @param {Object} cf A reference to the ColumnFamily
 * @param {Object} name The name of the column (optional)
 * @private
 * @memberOf ColumnFamily
 * @returns {Object} a Thrift ColumnParent object
 */
function columnParent(cf, column)
{
    var args = { column_family: cf.name };
    if (cf.isSuper && column)
        args.super_column = cf.columnMarshaller.serialize(column);
    return new ttype.ColumnParent(args);
}

/**
 * Returns a column path
 * As with the ColumnParent, the value of the ColumnPath depends on whether
 * this is a standard or super column family.  Both must specify the column
 * family name.  A standard column family may provide an optional column name
 * parameter.  In addition to the column name, a super column family may also
 * use a subcolumn parameter.
 *
 * @param {Object} cf A reference to the ColumnFamily
 * @param {Object} column The name of the column (optional)
 * @param {Object} subcolumn The name of the subcolumn (optional)
 * @private
 * @memberOf ColumnFamily a Thrift ColumnPath object
 */
function columnPath(cf, column, subcolumn)
{
    var args = { column_family: cf.name };

    if (column)
        args.column = cf.columnMarshaller.serialize(column);

    if (cf.isSuper && subcolumn)
        args.subcolumn = cf.subcolumnMarshaller.serialize(subcolumn);

    return new ttype.ColumnPath(args);
}

function getColumns(columns, options)
{
    var timestamp = new Date();
    options = Object(options);

    return _.map(columns, function(value, key)
    {
        if (value == null)
            value = '';

        return new Column(key, value, timestamp, options.ttl);
    });
}

function getSlicePredicate(options, serializer)
{
    var predicate = new ttype.SlicePredicate(),
        columns   = options.columns;
    if (Array.isArray(columns))
    {
        predicate.column_names = _.map(columns, function(col)
        {
            return serializer.serialize(col);
        });
    }
    else
    {
        var start      = options.start,
            end        = options.end,
            isReversed = !!options.reversed;

        predicate.slice_range = new ttype.SliceRange(
        {
            start:    start ? serializer.serialize(start, !isReversed) : '',
            finish:   end ? serializer.serialize(end, isReversed) : '',
            reversed: isReversed,
            count:    options.max
        });
    }

    return predicate;
}

/**
 * A convenience method to normalize the standard parameters used by
 * a thrift operation. The parameter list must contain a `key` parameter
 * as its first item.  The `column`, `subcolumn`, `options`, and
 * `callback` parameters are optional.
 * @param {Array} list The list of parameters
 * @private
 * @memberOf ColumnFamily
 * @returns {Object} a normalized version of the provided parameter values
 */
function normalizeParameters(list)
{
    list = _.toArray(list);
    return _.reduce(list, function(args, value, index)
    {
        if (_.isObject(value) && !Array.isArray(value))
        {
            var options     = args.options,
                timestamp   = value.timestamp,
                consistency = value.consistency || value.consistencyLevel;

            if (_.isDate(timestamp))
                options.timestamp = timestamp;
            if (consistency)
                options.consistency = consistency;

            _.defaults(options, value);
        }
        else if (index < 2)
            args[index ? 'subcolumn' : 'column'] = value;
        return args;
    }, {
        key: list.shift(),
        options:
        {
            timestamp: new Date(),
            consistency: DEFAULT_WRITE_CONSISTENCY
        }
    });
}

var ColumnFamily = module.exports = function ColumnFamily(keyspace, definition)
{
    var self    = this,
        isSuper = definition.column_type == 'Super';

    this.isSuper             = isSuper;
    this.isCounter           = definition.default_validation_class == 'org.apache.cassandra.db.marshal.CounterColumnType';
    this.keyspace            = keyspace;
    this.connection          = keyspace.connection;
    this.definition          = definition;
    this.name                = definition.name;
    this.columnMarshaller    = new Marshal(definition.comparator_type);
    this.subcolumnMarshaller = isSuper ? new Marshal(definition.subcomparator_type) : null;
    this.valueMarshaller     = new Marshal(definition.default_validation_class);
    this.keyMarshaller       = new Marshal(definition.key_validation_class);
    this.columnValidators    = {};

    _.each(definition.column_metadata, function(col)
    {
        col.name = self.columnMarshaller.deserialize(col.name);
        self.setColumnValidator(col.name, col.validation_class);
    });
};

ColumnFamily.prototype.setColumnValidator = function setColumnValidator(name, type)
{
    var marshalledColumn = this.columnMarshaller.serialize(name).toString('binary');
    this.columnValidators[marshalledColumn] = new Marshal(type);
};

ColumnFamily.prototype.getColumnValidator = function getColumnValidator(name)
{
    var marshalledColumn = this.columnMarshaller.serialize(name).toString('binary');
    return this.columnValidators[marshalledColumn];
};

/**
 * Performs a set command to the cluster
 *
 * @param {String} key The key for the row
 * @param {Object} columns The value for the columns as represented by JSON or an array of Column objects
 * @param {Object} options The options for the insert
 */
ColumnFamily.prototype.insert = function insert(key, columns, options)
{
    options = Object(options);

    var self        = this,
        consistency = options.consistency || options.consistencyLevel || DEFAULT_WRITE_CONSISTENCY;

    if (!Array.isArray(columns))
        columns = getColumns(columns, options);

    var batch         = {},
        marshalledKey = this.keyMarshaller.serialize(key).toString('binary'),
        definition    = batch[marshalledKey] = {};

    var mutations = _.map(columns, function(col)
    {
        var valueMarshaller = self.getColumnValidator(col.name) || self.valueMarshaller;
        return new ttype.Mutation(
        {
            column_or_supercolumn: new ttype.ColumnOrSuperColumn(
                { column: col.toThrift(self.columnMarshaller, valueMarshaller) }
            )
        });
    });

    definition[this.definition.name] = mutations;

    return this.connection.execute('batch_mutate', batch, consistency);
};

/**
 * Remove a single row or column
 * This function uses a variable-length paramter list.  Which parameters
 * are passed depends on which column path should be used for the
 * removal and whether this column family is a super column or not.
 *
 * @param {String} key The key for this row (required)
 * @param {Object} column The column name (optional)
 * @param {Object} subcolumn The subcolumn name (optional)
 * @param {Object} options The thrift options for this operation (optional)
 */
ColumnFamily.prototype.remove = function remove()
{
    var args          = normalizeParameters(arguments),
        self          = this,
        marshalledKey = this.keyMarshaller.serialize(args.key).toString('binary'),
        path          = columnPath(this, args.column, args.subcolumn);
    return this.connection.execute('remove', marshalledKey, path, args.options.timestamp * 1000, args.options.consistency);
};

/**
 * Counts the number of columns in a row by its key
 * @param {String} key The key to get
 * @param {Object} options Options for the get, can have start, end, max, consistencyLevel
 *   <ul>
 *     <li>
 *       start: the from part of the column name, for composites pass an array. By default the
 *       composite queries are inclusive, to make them exclusive pass an array of arrays where the
 *       inner array is [ value, false ].
 *     </li>
 *       start: the end part of the column name, for composites pass an array. By default the
 *       composite queries are inclusive, to make them exclusive pass an array of arrays where the
 *       inner array is [ value, false ].
 *     <li>reversed: {Boolean} to whether the range is reversed or not</li>
 *     <li>max: the max amount of columns to return</li>
 *     <li>columns: an {Array} of column names to get</li>
 *     <li>consistencyLevel: the read consistency level</li>
 *   </ul>
 */
ColumnFamily.prototype.count = function count(key, options)
{
    return this.get(key, _.defaults({ count: true }, options));
};

/**
 * Get a row by its key
 * @param {String} key The key to get
 * @param {Object} options Options for the get, can have start, end, max, consistencyLevel
 *   <ul>
 *     <li>
 *       start: the from part of the column name, for composites pass an array. By default the
 *       composite queries are inclusive, to make them exclusive pass an array of arrays where the
 *       inner array is [ value, false ].
 *     </li>
 *       start: the end part of the column name, for composites pass an array. By default the
 *       composite queries are inclusive, to make them exclusive pass an array of arrays where the
 *       inner array is [ value, false ].
 *     <li>reversed: {Boolean} to whether the range is reversed or not</li>
 *     <li>max: the max amount of columns to return</li>
 *     <li>columns: an {Array} of column names to get</li>
 *     <li>consistencyLevel: the read consistency level</li>
 *   </ul>
 */
ColumnFamily.prototype.get = function get(key, options)
{
    options = _.defaults(Object(options), { start: '', end: '' });

    var self          = this,
        consistency   = options.consistency || options.consistencyLevel || DEFAULT_READ_CONSISTENCY,
        marshalledKey = this.keyMarshaller.serialize(key).toString('binary'),
        predicate     = getSlicePredicate(options, this.columnMarshaller),
        isCount       = options.count === true;

    var command = isCount ? 'get_count' : 'get_slice';
    return this.connection.execute(command, marshalledKey, columnParent(self), predicate, consistency).then(function(value)
    {
        return isCount ? value : Row.fromThrift(key, value, self);
    });
};

/**
 * Truncates a ColumnFamily
 */
ColumnFamily.prototype.truncate = function truncate()
{
    return this.connection.execute('truncate', this.name);
};

/**
 * Gets rows by their indexed fields
 * @param {Object} query Options for the rows part of the get
 *   <ul>
 *     <li>fields: an array of objects that contain { column:column_name, operator: 'EQ', value:value }
 *       <ul>
 *         <li>column: {String} The name of the column with the index</li>
 *         <li>operator: {String} The operator to use, can be EQ, GTE, GT, LTE, ot LT</li>
 *         <li>value: {String} The value to query by</li>
 *       </ul>
 *     </li>
 *     <li>start: the start key to get</li>
 *     <li>max: the total amount of rows to return</li>
 *   </ul>
 * @param {Object} options Options for the get, can have start, end, max, consistencyLevel
 *   <ul>
 *     <li>start: the from part of the column name</li>
 *     <li>end: the to part of the column name</li>
 *     <li>max: the max amount of columns to return</li>
 *     <li>columns: an {Array} of column names to get</li>
 *     <li>consistencyLevel: the read consistency level</li>
 *   </ul>
 */
ColumnFamily.prototype.getIndexed = function getIndexed(query, options)
{
    options = _.defaults(Object(options), { start: '', end: '' });

    var self        = this,
        consistency = options.consistency || options.consistencyLevel || DEFAULT_READ_CONSISTENCY,
        predicate   = getSlicePredicate(options, this.columnMarshaller);

    var indexExpressions = _.map(query.fields, function(field)
    {
        var valueMarshaller = self.getColumnValidator(field.column) || self.valueMarshaller;
        return new ttype.IndexExpression(
        {
            column_name: self.columnMarshaller.serialize(field.column),
            op: ttype.IndexOperator[field.operator],
            value: valueMarshaller.serialize(field.value)
        });
    });

    var indexClause = new ttype.IndexClause(
    {
        expressions: indexExpressions,
        start_key:   query.start || '',
        count:       query.max || 100
    });

    return this.connection.execute('get_indexed_slices', columnParent(self), indexClause, predicate, consistency).then(function(value)
    {
        return _.map(value, function(row)
        {
            return Row.fromThrift(self.keyMarshaller.deserialize(row.key), row.columns, self);
        });
    });
};

/**
 * Increments a counter column.
 * @param  {Object}   key      Row key
 * @param  {Object}   column   Column name
 * @param  {Number}   value    Integer to increase by, defaults to 1 (optional)
 * @param  {Object}   options  The thrift options (optional)
 */
ColumnFamily.prototype.incr = function incr(key, column, value, options)
{
    if (_.isObject(value) && options == null)
    {
        options = value;
        value = 1;
    }
    else if (value == null)
    {
        options = null;
        value = 1;
    }
    options = Object(options);

    var batch         = {},
        marshalledKey = this.keyMarshaller.serialize(key).toString('binary'),
        definition    = batch[marshalledKey] = {};

    var counterColumn = new CounterColumn(column, value),
        mutations = [
            new ttype.Mutation(
            {
                column_or_supercolumn: new ttype.ColumnOrSuperColumn(
                    { counter_column: counterColumn.toThrift(this.columnMarshaller) }
                )
            })
        ];
    definition[this.definition.name] = mutations;

    var consistency = options.consistency || options.consistencyLevel || DEFAULT_WRITE_CONSISTENCY;
    return this.connection.execute('batch_mutate', batch, consistency);
};
