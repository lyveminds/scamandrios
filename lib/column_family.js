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

var DEFAULT_READ_CONSISTENCY = ttype.ConsistencyLevel.QUORUM;
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
    var args = {column_family: cf.name};
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
    var args = {column_family: cf.name};

    if (column)
      args.column = cf.columnMarshaller.serialize(column);

    if (cf.isSuper && subcolumn)
        args.subcolumn = cf.subcolumnMarshaller.serialize(subcolumn);

    return new ttype.ColumnPath(args);
}

function getColumns(columns, options)
{
    var result = [],
        timestamp = new Date();

    _.forOwn(columns, function(v, k)
    {
        if (_.isNull(v) || _.isUndefined(v))
            v = '';

        result.push(new Column(k, v, timestamp, options.ttl));
    });
}

function getSlicePredicate(options, serializer)
{
    var predicate = new ttype.SlicePredicate();

    if (Array.isArray(options.columns))
    {
        predicate.column_names = _.map(options.columns, function(col)
        {
            return serializer.serialize(col);
        });
    }
    else
    {
        var start = '', end = '';
        if (options.start)
            start = serializer.serialize(options.start, !options.reversed);
        if (options.end)
            end = serializer.serialize(options.end, !!options.reversed);

        predicate.slice_range = new ttype.SliceRange(
        {
            start:    start,
            finish:   end,
            reversed: options.reversed,
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
    var args = { };
    args.key = list.shift();

    for (var i = 0; i < list.length; i += 1)
    {
        var type = typeof list[i];
        if (type === 'function')
        {
            args.callback = list[i];
        }
        else if (type === 'object' && !Array.isArray(list[i]))
        {
            args.options = list[i];
        }
        else
        {
            if (i === 0) { args.column = list[i];}
            if (i === 1) { args.subcolumn = list[i];}
        }
    }

    return args;
}




function ColumnFamily(keyspace, definition)
{
    this.isSuper             = definition.column_type === 'Super';
    this.isCounter           = definition.default_validation_class === 'org.apache.cassandra.db.marshal.CounterColumnType';
    this.keyspace            = keyspace;
    this.connection          = keyspace.connection;
    this.definition          = definition;
    this.name                = definition.name;
    this.columnMarshaller    = new Marshal(definition.comparator_type);
    this.subcolumnMarshaller = this.isSuper ? new Marshal(definition.subcomparator_type) : null;
    this.valueMarshaller     = new Marshal(definition.default_validation_class);
    this.keyMarshaller       = new Marshal(definition.key_validation_class);
    this.columnValidators    = {};

    if (Array.isArray(definition.column_metadata))
    {
        _.each(definition.column_metadata, function(col)
        {
            col.name = this.columnMarshaller.deserialize(col.name);
            this.setColumnValidator(col.name, col.validation_class);
        });
    }
}

ColumnFamily.prototype.setColumnValidator = function(name, type)
{
    var binname = this.columnMarshaller.serialize(name).toString('binary');
    this.columnValidators[binname] = new Marshal(type);
};

ColumnFamily.prototype.getColumnValidator = function(name)
{
    var binname = this.columnMarshaller.serialize(name).toString('binary');
    return this.columnValidators[binname];
};

/**
 * Performs a set command to the cluster
 *
 * @param {String} key The key for the row
 * @param {Object} columns The value for the columns as represented by JSON or an array of Column objects
 * @param {Object} options The options for the insert
 */
ColumnFamily.prototype.insert = function(key, columns, options)
{
    if (!Array.isArray(columns))
        columns = getColumns(columns, options);

    var mutations = [];

    _.each(columns, function(col)
    {
        var valueMarshaller = this.getColumnValidator(col.name) || this.valueMarshaller;
        mutations.push(new ttype.Mutation(
        {
            column_or_supercolumn: ttype.ColumnOrSuperColumn({ column: col.toThrift(this.columnMarshaller, valueMarshaller)})
        }));
    });

    var consistency = options.consistency || options.consistencyLevel || DEFAULT_WRITE_CONSISTENCY;
    var binkey      = this.keyMarshaller.serialize(key).toString('binary');
    var batch       = {};
    batch[binkey] = {};
    batch[binkey][this.definition.name] = mutations;

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
ColumnFamily.prototype.remove = function()
{
    var args = normalizeParameters(Array.prototype.slice.apply(arguments));
    args.options = args.options || { };

    var marshalledKey = this.keyMarshaller.serialize(args.key).toString('binary'),
        path          = columnPath(this, args.column, args.subcolumn),
        timestamp     = args.options.timestamp || new Date(),
        consistency   = args.options.consistency || args.options.consistencyLevel || DEFAULT_WRITE_CONSISTENCY;

    return this.connection.execute('remove', marshalledKey, path, timestamp * 1000, consistency);
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
ColumnFamily.prototype.count = function(key, options)
{
    options       = options || {};
    options.count = true;

    return this.get(key, options);
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

ColumnFamily.prototype.get = function(key, options)
{
    var deferred = P.defer(),
        self = this;

    options = options || {};
    options.start = options.start || '';
    options.end = options.end || '';

    var consistency   = options.consistency || options.consistencyLevel || DEFAULT_READ_CONSISTENCY,
        marshalledKey = this.keyMarshaller.serialize(key).toString('binary'),
        predicate     = getSlicePredicate(options, this.columnMarshaller);

    var command = (options.count ? 'get_count' : 'get_slice');
    var result = this.connection.execute(command, marshalledKey, columnParent(this), predicate, consistency);

    result.then(function(value)
    {
        if (options.count)
            deferred.resolve(value);
        else
            deferred.resolve(Row.fromThrift(key, value, self));
    }, deferred.reject);


    return deferred.promise;
};


/**
 * Truncates a ColumnFamily
 */
ColumnFamily.prototype.truncate = function()
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
ColumnFamily.prototype.getIndexed = function(query, options)
{
    var deferred = P.defer(),
        self = this;

    options.start = options.start || '';
    options.end   = options.end || '';

    var consistency = options.consistency || options.consistencyLevel || DEFAULT_READ_CONSISTENCY;
    var predicate = getSlicePredicate(options, this.columnMarshaller);
    var indexExpressions = [];

    _.each(query.fields, function(field)
    {
        var  valueMarshaller = self.getColumnValidator(field.column) || self.valueMarshaller;
        indexExpressions.push(new ttype.IndexExpression(
        {
            column_name: this.columnMarshaller.serialize(field.column),
            op:          ttype.IndexOperator[field.operator],
            value:       valueMarshaller.serialize(field.value)
        }));
    });

    var indexClause = new ttype.IndexClause({
        expression: indexExpressions,
        start_key:  query.start || '',
        count:      query.max || 100
    });

    var result = this.connection.execute('get_indexed_slices', columnParent(self), indexClause, predicate, consistency);
    result.then(function(values)
    {
        var results = _.map(values, function(row)
        {
            return Row.fromThrift(self.keyMarshaller.deserialize(row.key), row.columns, self);
        });
    }, deferred.reject);

    return deferred.promise;
};

/**
 * Increments a counter column.
 * @param  {Object}   key      Row key
 * @param  {Object}   column   Column name
 * @param  {Number}   value    Integer to increase by, defaults to 1 (optional)
 * @param  {Object}   options  The thrift options (optional)
 */
ColumnFamily.prototype.incr = function (key, column, value, options)
{
    if (_.isObject(value))
    {
        options = value;
        value = 1;
    }
    else if (_.isUndefined(value))
    {
        options = {};
        value = 1;
    }
    else
        options = options || {};

    var consistency = options.consistency || options.consistencyLevel || DEFAULT_WRITE_CONSISTENCY;
    var col = new CounterColumn(column, value);

    var mutations = [
        new ttype.Mutation(
        {
            column_or_supercolumn: new new ttype.ColumnOrSuperColumn({ counter_column: col.toThrift(this.columnMarshaller) });
        });
    ];

    var marshalledKey = this.keyMarshaller.serialize(key).toString('binary');
    var batch = {};
    batch[marshalledKey] = {};
    batch[marshalledKey][this.definition.name] = mutations;

    return this.connection.execute('batch_mutate', batch, consistency);
};

module.exports = ColumnFamily;
