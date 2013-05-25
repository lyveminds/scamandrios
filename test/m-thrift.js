/*global describe:true, it:true, before:true, after:true */

var
    chai = require('chai'),
    assert = chai.assert,
    expect = chai.expect,
    chaiAsPromised = require('chai-as-promised'),
    util = require('util'),
    scamandrios = require('../'),
    P = require('p-promise'),
    _ = require('lodash')
    ;

require('mocha-as-promised')();
chai.should();
chai.use(chaiAsPromised);

var config = require('./helpers/thrift'),
    system = require('./helpers/connection'),
    badSystem = require('./helpers/bad_connection');

describe('thrift', function()
{
    function closePool(pool)
    {
        var deferred = P.defer();
        pool.on('close', deferred.resolve);
        pool.close();
        return deferred.promise;
    }

    var conn, keySpace, cfComposite, cfCounter, cfStandard, rowStandard;

    describe('connections & bad connections', function()
    {

        it('pool.connect without keyspace', function()
        {
            conn = new scamandrios.ConnectionPool(system);
            return conn.connect().should.be.fulfilled.should.eventually.not.be.ok;
        });

        it('pool.close without keyspace', function()
        {
            return closePool(conn).should.be.fulfilled;
        });

        it('pool.connect with keyspace', function()
        {
            system.keyspace = 'system';
            conn = new scamandrios.ConnectionPool(system);
            var promise = conn.connect().should.be.fulfilled;

            return promise.should.eventually.have.property('definition').then(function(keyspace)
            {
                return keyspace.definition.name;
            }).should.become('system');
        });

        it('bad pool connect', function()
        {
            var badPool = new scamandrios.ConnectionPool(badSystem);
            badPool.on('error', function(error)
            {
                expect(error).to.exist;
            });
            var promise = badPool.connect();
            return promise.should.be.rejected.fail(function()
            {
                return badPool.createKeyspace(config.keyspace);
            }).should.be.rejected.fail(function()
            {
                return badPool.dropKeyspace(config.keyspace);
            }).should.be.rejected.fail(function()
            {
                return closePool(badPool);
            }).should.be.fulfilled;
        });

        it('pool.createKeyspace', function()
        {
            var promise = conn.createKeyspace(config.keyspace);
            return promise.should.be.fulfilled;
        });

        it('pool.use', function()
        {
            var promise = conn.use(config.keyspace);
            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an('array'),
                promise.should.eventually.have.property('length', system.hostPoolSize),
                promise.then(function(value)
                {
                    return (keySpace = value[0].value);
                }).should.eventually.be.an.instanceof(scamandrios.Keyspace)
            ]);
        });
    });

    describe('keyspaces', function()
    {
        it('keyspace.createColumnFamily', function()
        {
            var promise = keySpace.createColumnFamily(config.cf_standard, config.cf_standard_options);
            return promise.should.be.fulfilled;
        });

        it('keyspace.createColumnFamily with composite type', function()
        {
            var promise = keySpace.createColumnFamily(config.cf_standard_composite, config.cf_standard_composite_options);
            return promise.should.be.fulfilled;
        });

        it('keyspace.createSuperColumnFamily', function()
        {
            var promise = keySpace.createColumnFamily(config.cf_supercolumn, config.cf_supercolumn_options);
            return promise.should.be.fulfilled;
        });

        it('keyspace.createCounterFamily', function()
        {
            var promise = keySpace.createColumnFamily(config.cf_counter, config.cf_counter_options);
            return promise.should.be.fulfilled;
        });

        it('keyspace.createColumFamily reversed', function()
        {
            var promise = keySpace.createColumnFamily(config.cf_reversed, config.cf_reversed_options);
            return promise.should.be.fulfilled;
        });

        it('keyspace.createColumFamily compositeNestedReversed', function()
        {
            var promise = keySpace.createColumnFamily(config.cf_composite_nested_reversed, config.cf_composite_nested_reversed_options);
            return promise.should.be.fulfilled;
        });

        it('keyspace.get standard', function()
        {
            var promise = keySpace.get(config.cf_standard);
            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.ColumnFamily),
                promise.should.eventually.have.property('isSuper', false)
            ]);
        });

        it('keyspace.get composite', function()
        {
            var promise = keySpace.get(config.cf_standard_composite);
            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.ColumnFamily),
                promise.then(function(value)
                {
                    cfComposite = value;
                })
            ]);
        });

        it('keyspace.get supercolumn', function()
        {
            var promise = keySpace.get(config.cf_supercolumn);
            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.ColumnFamily),
                promise.should.eventually.have.property('isSuper', true)
            ]);
        });

        it('keyspace.get counter', function()
        {
            var promise = keySpace.get(config.cf_counter);
            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.ColumnFamily),
                promise.should.eventually.have.property('isCounter', true),
                promise.then(function(value)
                {
                    cfCounter = value;
                })
            ]);
        });

        it('keyspace.get from index/cache', function()
        {
            var promise = keySpace.get(config.cf_standard);
            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.ColumnFamily),
                promise.then(function(value)
                {
                    cfStandard = value;
                })
            ]);
        });

        it('keyspace.get invalid cf', function()
        {
            var promise = keySpace.get(config.cf_invalid);
            return promise.should.be.rejected.with(Error, /ColumnFamily cf_invalid_test Not Found/);
        });
    });

    describe('column family gets/inserts', function()
    {
        it('cf.insert standard', function()
        {
            var promise = cfStandard.insert(config.standard_row_key, config.standard_insert_values);
            return promise.should.be.fulfilled;
        });

        it('cf.insert standard with custom consistency level', function()
        {
            var promise = cfStandard.insert(config.standard_row_key, config.standard_insert_values, { 'consistency': scamandrios.ConsistencyLevel.ANY });
            return promise.should.be.fulfilled;
        });

        it('cf.insert standard into composite cf', function()
        {
            var key = ['åbcd', new scamandrios.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c')],
                column = new scamandrios.Column([12345678912345, new Date(1326400762701)], 'some value');

            cfComposite.setColumnValidator(column.name, 'UTF8Type');
            var promise = cfComposite.insert(key, [column]);
            return promise.should.be.fulfilled;
        });

        it('counter cf.incr', function()
        {
            var column = '1234', key = 'åbcd', promise = cfCounter.incr(key, column, 1337);
            return promise.should.be.fulfilled;
        });

        it('standard cf.get', function()
        {
            var row = cfStandard.get(config.standard_row_key);
            return P.all(
            [
                row.should.be.fulfilled,
                row.should.eventually.be.an.instanceof(scamandrios.Row),
                row.should.eventually.have.property('count', 4),
                row.should.eventually.have.property('key', config.standard_row_key),
                row.then(function(row)
                {
                    return _.pluck([row.get('one'), row.get('two'), row.get('three'), row.get('four')], 'value');
                }).should.become(['a', 'b', 'c', '']),
                // See simplereach/helenus#4.
                row.then(function(row)
                {
                    var timeStamp = +new Date();
                    return row.get('one').timestamp <= timeStamp && row.get('one').timestamp >= timeStamp - 1000;
                }).should.eventually.be.ok,
                row.then(function(row)
                {
                    rowStandard = row;
                })
            ]);
        });

        it('standard cf.get for composite column family', function()
        {
            var key = ['åbcd', new scamandrios.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c')],
                promise = cfComposite.get(key);

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.Row),
                promise.then(function(row)
                {
                    return row.get([12345678912345, new Date(1326400762701)]).value;
                }).should.become('some value')
            ]);
        });

        it('standard cf.get with options', function()
        {
            var promise = cfStandard.get(config.standard_row_key, config.standard_get_options);
            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.Row),
                promise.should.eventually.have.property('count', 1),
                promise.should.eventually.have.property('key', config.standard_row_key),
                promise.then(function(row)
                {
                    return row.get('one').value;
                }).should.become('a')
            ]);
        });

        it('standard cf.get count', function()
        {
            var promise = cfStandard.count(config.standard_row_key, config.standard_get_options);
            return promise.should.be.fulfilled.should.become(1);
        });

        it('standard cf with composite column slice', function()
        {
            var values =
            [
                new scamandrios.Column([1, new Date(1)], 'a'),
                new scamandrios.Column([2, new Date(2)], 'b'),
                new scamandrios.Column([3, new Date(3)], 'c'),
                new scamandrios.Column([4, new Date(4)], 'd'),
                new scamandrios.Column([5, new Date(5)], 'e'),
                new scamandrios.Column([6, new Date(6)], 'f'),
                new scamandrios.Column([7, new Date(7)], 'g')
            ];
            var key = ['comp_range_1', new scamandrios.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c')];

            var promise = cfComposite.insert(key, values).then(function()
            {
                return cfComposite.get(key, { 'start': [3], 'end': [5] });
            });

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.have.property('count', 3),
                promise.then(function(row)
                {
                    return _.map(row, function(column)
                    {
                        return column.name[0];
                    });
                }).should.become([3, 4, 5])
            ]);
        });

        it('standard cf with exclusive composite column slice', function()
        {
            var values =
            [
                new scamandrios.Column([1, new Date(1)], 'a'),
                new scamandrios.Column([2, new Date(2)], 'b'),
                new scamandrios.Column([3, new Date(3)], 'c'),
                new scamandrios.Column([4, new Date(4)], 'd'),
                new scamandrios.Column([5, new Date(5)], 'e'),
                new scamandrios.Column([6, new Date(6)], 'f'),
                new scamandrios.Column([7, new Date(7)], 'g')
            ];
            var key = ['comp_range_1', new scamandrios.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c')];

            var promise = cfComposite.insert(key, values).then(function()
            {
                return cfComposite.get(key, { 'start': [[3, false]], 'end': [[5, false]] });
            });

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.have.property('count', 1),
                promise.then(function(row)
                {
                    return row[0].name[0];
                }).should.become(4)
            ]);
        });

        it('cf.get standard with custom CL', function()
        {
            var promise = cfStandard.get(config.standard_row_key, { 'consistency': scamandrios.ConsistencyLevel.ONE }).should.be.fulfilled;
            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.Row),
                promise.should.eventually.have.property('key', config.standard_row_key),
                promise.then(function(row)
                {
                    return row.get('one').value;
                }).should.become('a')
            ]);
        });

        it('cf.get standard with columns names', function()
        {
            var promise = cfStandard.get(config.standard_row_key, config.standard_get_names_options);
            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.Row),
                promise.should.eventually.have.property('count', 2),
                promise.should.eventually.have.property('key', config.standard_row_key),
                promise.then(function(row)
                {
                    return _.pluck([row.get('one'), row.get('three')], 'value');
                }).should.become(['a', 'c'])
            ]);
        });
    });

    describe('composite column families', function()
        {
        it('cf.get composite with column names', function()
        {
            var key = ['åbcd', new scamandrios.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c')],
                columns = [[12345678912345, new Date(1326400762701)]],
                promise = cfComposite.get(key, { 'columns': columns });

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.Row),
                promise.then(function(row)
                {
                    return row.get.apply(row, columns).value;
                }).should.become('some value')
            ]);
        });

        it('cf.get counter with column names', function()
        {
            var key = 'åbcd',
                columns = ['1234'],
                promise = cfComposite.get(key, { 'columns': columns });

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.Row),
                promise.then(function(row)
                {
                    return row.get.apply(row, columns).value;
                }).should.become(1337)
            ]);
        });

        it('cf.get can error', function()
        {
            var promise = cfStandard.get(config.standard_row_key, config.standard_get_options_error);
            return P.all(
            [
                promise.should.be.rejected.with(Error),
                promise.fail(_.identity).should.eventually.have.property('name', 'InvalidRequestException').then(function(error)
                {
                    return error.why;
                }).should.become('range finish must come after start in the order of traversal')
            ]);
        });

        it('cf.get can get with index', function()
        {
            var key = config.standard_row_key + '-utf8',
                query = { 'fields': [{ 'column': 'index-test', 'operator': 'EQ', 'value': 'åbcd' }] },
                options = { 'index-test': 'åbcd' };

            var promise = cfStandard.insert(key, options).then(function()
            {
                return cfStandard.getIndexed(query);
            });

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an('array'),
                promise.should.eventually.have.property('length', 1),
                promise.then(function(rows)
                {
                    return rows[0].get('index-test').value;
                }).should.become('åbcd')
            ]);
        });

        // ...
        function insertType(key, value)
        {
            var qualifiedRow = config.standard_row_key + '-' + key,
                qualifiedColumn = key + '-test',
                options = {};
            options[qualifiedColumn] = value;

            var promise = cfStandard.insert(qualifiedRow, options).then(function()
            {
                return cfStandard.get(qualifiedRow);
            });

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.be.an.instanceof(scamandrios.Row),
                promise.then(function(row)
                {
                    return row.get(qualifiedColumn).value;
                })
            ]).then(function(promises)
            {
                return promises[2];
            });
        }

        it('cf.get can get BytesType', function()
        {
            var buffer = new Buffer([0, 0xff, 0x20]);
            return insertType('bytes', buffer).should.eventually.be.an.instanceof(Buffer).then(function(value)
            {
                return value.toString('hex');
            }).should.become(buffer.toString('hex'));
            /*
            var key = config.standard_row_key + '-bytes',
                options = { 'bytes-test':  },
                promise = cfStandard.insert(key, options);
            return promise.then(function()
            {
                return cfStandard.get(key);
            }).should.be.fulfilled.then(function(row)
            {
                return row.get('bytes-test').value;
            }).should.eventually.be.an.instanceof(Buffer).then(function(value)
            {
                return value.toString('hex');
            }).should.become(options['bytes-test'].toString('hex'));*/
        });

        it('cf.get can get LongType', function()
        {
            var value = 123456789012345;
            return insertType('long', value).should.become(value);
            /*
            var key = config.standard_row_key + '-long',
                options = { 'long-test':  },
                promise = cfStandard.insert(key, options);

            return promise.then(function()
            {
                return cfStandard.get(key);
            }).should.be.fulfilled.then(function(row)
            {
                return row.get('long-test').value;
            }).should.become(123456789012345);*/
        });

        it('cf.get can get IntegerType', function()
        {
            var value = 1234;
            return insertType('integer', value).should.become(value);
            /*
            var key = config.standard_row_key + '-integer',
                options = { 'integer-test': 1234 },
                promise = cfStandard.insert(key, options);

            return promise.then(function()
            {
                return cfStandard.get(key);
            }).should.be.fulfilled.then(function(row)
            {
                return row.get('integer-test').value;
            }).should.become(1234);*/
        });

        it('cf.get can get UTF8Type', function()
        {
            var value = 'åbcd';
            return insertType('utf8', value).should.become(value);
            /*
            var key = config.standard_row_key + '-utf8',
                options = { 'utf8-test': 'åbcd' },
                promise = cfStandard.insert(key, options);

            return promise.then(function()
            {
                return cfStandard.get(key);
            }).should.be.fulfilled.then(function(row)
            {
                return row.get('utf8-test').value;
            }).should.become('åbcd');*/
        });

        it('cf.get can get AsciiType', function()
        {
            var value = 'abcd';
            return insertType('ascii', value).should.become(value);
            /*
            var key = config.standard_row_key + '-ascii',
                options = { 'ascii-test': 'abcd' },
                promise = cfStandard.insert(key, options);

            return promise.then(function()
            {
                return cfStandard.get(key);
            }).should.be.fulfilled.then(function(row)
            {
                return row.get('ascii-test').value;
            }).should.become('abcd');*/
        });

        it('cf.get can get LexicalUUIDType', function()
        {
            var value = new scamandrios.UUID();
            return insertType('lexicaluuid', value).should.eventually.be.an.instanceof(scamandrios.UUID).then(function(value)
            {
                return value.hex.length;
            }).should.become(36);
        });

        it('cf.get can get TimeUUIDType', function()
        {
            var value = new scamandrios.TimeUUID();
            return insertType('timeuuid', value).should.eventually.be.an.instanceof(scamandrios.TimeUUID).then(function(value)
            {
                return value.hex.length;
            }).should.become(36);
        });

        it('cf.get can get Float', function()
        {
            var value = 1234.1234130859375;
            return insertType('float', value).should.become(value);
        });

        it('cf.get can get Double', function()
        {
            var value = 123456789012345.1234;
            return insertType('double', value).should.become(value);
        });

        it('cf.get can get Date', function()
        {
            var value = new Date(1326400762701);
            return insertType('date', value).should.eventually.be.an.instanceof(Date).then(function(value)
            {
                return value.getTime();
            }).should.become(value.getTime());
        });

        it('cf.get can get Boolean', function()
        {
            var value = true;
            return insertType('boolean', value).should.become(value);
        });
    });

    describe('rows', function()
    {
        it('`nameSlice`', function(done)
        {
            var row = rowStandard.nameSlice('a', 's');
            expect(row).to.be.an.instanceof(scamandrios.Row);
            expect(Array.isArray(row)).to.be.ok;
            expect(row).to.have.property('count', 2);
            expect(row).to.have.property('key', config.standard_row_key);
            expect(row.get('one')).to.have.property('value', 'a');
            expect(row.get('four')).to.have.property('value', '');
            done();
        });

        it('`slice`', function(done)
        {
            var row = rowStandard.slice(1, 3);
            expect(row).to.be.an.instanceof(scamandrios.Row);
            expect(Array.isArray(row)).to.be.ok;
            expect(row).to.have.property('count', 2);
            expect(row).to.have.property('key', config.standard_row_key);
            expect(row.get('one')).to.have.property('value', 'a');
            expect(row.get('three')).to.have.property('value', 'c');
            done();
        });

        it('`toString` and `inspect`', function(done)
        {
            var string = String(rowStandard);
            expect(string).to.be.a('string');
            expect(string).to.equal("<Row: Key: 'standard_row_1', ColumnCount: 4, Columns: [ 'four,one,three,two' ]>");
            done();
        });

        it('`forEach`', function(done)
        {
            var index = -1;
            var values =
            [
                { 'name': 'four', 'value': '' },
                { 'name': 'one', 'value': 'a' },
                { 'name': 'three', 'value': 'c' },
                { 'name': 'two', 'value': 'b' }
            ];
            rowStandard.forEach(function(name, value, timeStamp, ttl)
            {
                index++;
                expect(values[index].name).to.equal(name);
                expect(values[index].value).to.equal(value);
                expect(timeStamp).to.be.an.instanceof(Date);
                expect(ttl).to.be.null;
            });
            done();
        });
    });

    describe('removing', function()
    {
        it('remove standard cf column', function()
        {
            var promise = cfStandard.remove(config.standard_row_key, 'one', { 'timestamp': new Date() }).then(function()
            {
                return cfStandard.get(config.standard_row_key);
            });

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.have.property('count', 3)
            ]);
        });

        it('remove composite cf column', function()
        {
            var key = ['åbcd', new scamandrios.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c')],
                column = [12345678912345, new Date(1326400762701)];

            var promise = cfStandard.remove(key, column, { 'timestamp': new Date() }).then(function()
            {
                return cfStandard.get(key);
            });

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.have.property('count', 0)
            ]);
        });

        it('remove standard cf row', function()
        {
            var promise = cfStandard.remove(config.standard_row_key, { 'timestamp': new Date() }).then(function()
            {
                return cfStandard.get(config.standard_row_key);
            });

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.should.eventually.have.property('count', 0)
            ]);
        });

        it('remove standard cf truncate', function()
        {
            var promise = cfStandard.truncate();
            return promise.should.be.fulfilled;
        });

        it('keyspace.dropColumnFamily', function()
        {
            var promise = keySpace.dropColumnFamily(config.cf_standard);
            return promise.should.be.fulfilled;
        });

        it('pool.dropKeyspace', function()
        {
            var promise = conn.dropKeyspace(config.keyspace);
            return promise.should.be.fulfilled;
        });
    });

    after(function()
    {
        return closePool(conn).should.be.fulfilled;
    });
});
