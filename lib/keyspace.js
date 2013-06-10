var
    _            = require('lodash'),
    ColumnFamily = require('./column_family'),
    Marshal      = require('./marshal'),
    P            = require('p-promise'),
    ttypes       = require('./cassandra/cassandra_types')
    ;

function Keyspace(connection, definition)
{
    this.connection = connection;
    this.name = definition.name;
    this.definition = definition;
    this.columnFamilies;

    this.withTables = this.getTables();
}

Keyspace.prototype.get = function(columnFamily)
{
    if (this.columnFamilies[columnFamily])
        return P(this.columnFamilies[columnFamily]);

    return this.describe().then(function(columnFamilies)
    {
        if (columnFamilies[columnFamily])
            return columnFamilies[columnFamily];

        var e = new Error('ColumnFamily ' + columnFamily + ' Not Found');
        e.name = 'ScamandriosNotFoundError';
        throw e;
    });
};

Keyspace.prototype.describe = function()
{
    var definition = this.connection.execute('describe_keyspace', this.name),
        self = this;
    return definition.then(function(result)
    {
        var columnFamilies = {};
        _.each(result.cf_defs, function(cf)
        {
            columnFamilies[cf.name] = new ColumnFamily(self, cf);
        });

        self.columnFamilies = columnFamilies;
        return columnFamilies;
    });
};

// Table caching conveniences


Keyspace.prototype.getTables = function()
{
    var self = this;

    if (this.columnFamilies)
        return P(this.columnFamilies);

    return self.describe();
};

// Up to you not to allow this to collide with anything.
Keyspace.prototype.getTableAs = function getTableAs(name, property)
{
   var self = this;

    if (this[property])
        return P(this[property]);

    return this.withTables
    .then(function(columnFamilies)
    {
        if (columnFamilies[name])
            self[property] = columnFamilies[name];

        return self[property];
    });
};

Keyspace.prototype.createTableAs = function createTableAs(name, property, options)
{
    var self = this;

    return this.getTableAs(name, property)
    .then(function(colfamily)
    {
        if (colfamily)
            return colfamily;

        var settings =
        {
            comment:                  options.description,
            key_alias:                options.key,
            key_validation_class:     'UTF8Type',
            comparator_type:          'UTF8Type',
            default_validation_class: 'UTF8Type',
            columns:                  options.columns
        };

        return self.createColumnFamily(name, settings)
        .then(function() { return self.get(name); })
        .then(function(colfamily)
        {
            self[property] = colfamily;
            return colfamily;
        });
    });
};

// end conveniences

/**
 * Creates a column family with options
 * @param {String} name The name of the column family to create
 * @param {Object} options The options for the columns family, options are:
 *  <ul>
 *    <li>column_type: Can be "Standard" or "Super" Defaults to "Standard" </li>
 *    <li>comparator_type: The default comparator type</li>
 *    <li>subcomparator_type: The default subcomparator type</li>
 *    <li>comment: A comment for the cf</li>
 *    <li>read_repair_chance: </li>
 *    <li>column_metadata: </li>
 *    <li>gc_grace_seconds: </li>
 *    <li>default_validation_class: </li>
 *    <li>min_compaction_threshold: </li>
 *    <li>max_compaction_threshold: </li>
 *    <li>replicate_on_write: </li>
 *    <li>merge_shards_chance: </li>
 *    <li>key_validation_class: </li>
 *    <li>key_alias: </li>
 *    <li>compaction_strategy: </li>
 *    <li>compaction_strategy_options: </li>
 *    <li>compression_options: </li>
 *    <li>bloom_filter_fp_chance: </li>
 *    <li>columns: Columns is an array of column options each element in the array is an object with these options:
 *      <ul>
 *        <li>name: *REQUIRED* The name of the column</li>
 *        <li>validation_class: *REQUIRED* The validation class. Defaults to BytesType</li>
 *        <li>index_type: The type of index</li>
 *        <li>index_name: The name of the index</li>
 *        <li>index_options: The options for the index, </li>
 *      </ul>
 *    </li>
 *  </ul>
 * @param {Function} callback The callback to invoke once the column family has been created
 */
Keyspace.prototype.createColumnFamily = function(name, options)
{
    var meta;
    options = options || {};

    var comparator = options.comparator_type || 'BytesType';

    if (options.columns && Array.isArray(options.columns))
    {
        var marshaller = new Marshal(comparator);
        meta = [];

        meta = _.map(options.columns, function(col)
        {
            return new ttypes.ColumnDef(
            {
                name:             marshaller.serialize(col.name),
                validation_class: col.validation_class,
                index_type:       col.index_type,
                index_name:       col.index_name,
                index_options:    col.index_options
            });
        });
    }

    var cfdef = new ttypes.CfDef(
    {
        keyspace:                    this.name,
        name:                        name,
        column_type:                 options.column_type || 'Standard',
        comparator_type:             comparator,
        subcomparator_type:          options.subcomparator_type,
        comment:                     options.comment,
        read_repair_chance:          options.read_repair_chance || 1,
        column_metadata:             meta,
        gc_grace_seconds:            options.gc_grace_seconds,
        default_validation_class:    options.default_validation_class,
        min_compaction_threshold:    options.min_compaction_threshold,
        max_compaction_threshold:    options.max_compaction_threshold,
        replicate_on_write:          options.replicate_on_write,
        merge_shards_chance:         options.merge_shards_chance,
        key_validation_class:        options.key_validation_class,
        key_alias:                   options.key_alias,
        compaction_strategy:         options.compaction_strategy,
        compaction_strategy_options: options.compaction_strategy_options,
        compression_options:         options.compression_options,
        bloom_filter_fp_chance:      options.bloom_filter_fp_chance
    });

    return this.connection.execute('system_add_column_family', cfdef);
};


Keyspace.prototype.dropColumnFamily = function(name)
{
    return this.connection.execute('system_drop_column_family', name);
};


module.exports = Keyspace;
