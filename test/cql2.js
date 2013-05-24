/*global describe: true, it: true, before: true, after: true */

var chai = require('chai'),
    chaiAsPromised = require('chai-as-promised'),
    P = require('p-promise');

require('mocha-as-promised')();

var scamandrios = require('../');

var poolSettings = require('./helpers/connection.json'),
    badSettings = require('./helpers/bad_connection.json'),
    commands = require('./helpers/cql2.json'),
    canSelectCQLVersion = require('./helpers/can_select_cql_version');

var assert = chai.assert,
    expect = chai.expect;

chai.should();
chai.use(chaiAsPromised);

describe('CQL 2', function ()
{
    var pool;

    before(function ()
    {
        poolSettings.cqlVersion = '2.0.0';
        return canSelectCQLVersion(poolSettings).then(function (canSelect)
        {
            if (!canSelect)
            {
                console.error('The `cqlVersion` cannot be set.');
                delete poolSettings.cqlVersion;
            }
            pool = new scamandrios.ConnectionPool(poolSettings);
            return pool.connect().should.be.fulfilled;
        });
    });

    describe('#connect()', function ()
    {
        it('should reject connections to nonexistent endpoints', function ()
        {
            var badPool = new scamandrios.ConnectionPool(badSettings);
            badPool.on('error', function (error)
            {
                expect(error).to.exist;
            });

            return badPool.connect().should.be.rejected.then(function ()
            {
                return badPool.cql(commands['create_ks#cql']).should.be.rejected;
            }).then(function ()
            {
                badPool.close();
            });
        });

        it('can create a keyspace', function ()
        {
            return pool.cql(commands['create_ks#cql']).should.be.fulfilled;
        });

        it('can use a keyspace', function ()
        {
            return pool.cql(commands['use#cql']).should.be.fulfilled;
        });

        it('can create a column family', function ()
        {
            return pool.cql(commands['create_cf#cql']).should.be.fulfilled;
        });

        it('can create a column family holding counters', function ()
        {
            return pool.cql(commands['create_counter_cf#cql']).should.be.fulfilled;
        });

        it('can create a column family with a reversed comparator', function ()
        {
            return pool.cql(commands['create_reversed_cf#cql']).should.be.fulfilled;
        });

        it('can update a value in a column family', function ()
        {
            return pool.cql(commands['update#cql']).should.be.fulfilled;
        });

        it('can update a value in a reversed column family', function ()
        {
            return pool.cql(commands['update_reversed#cql']).should.be.fulfilled;
        });

        it('can increment a counter column value', function ()
        {
            return pool.cql(commands['incr#cql']).should.be.fulfilled;
        });

        it('can read a record from a column family', function ()
        {
            var promise = pool.cql(commands['select#cql']).should.be.fulfilled;
            return promise.should.eventually.have.property('length', 1).then(function (value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function (row)
            {
                return row.get('foo').value;
            }).should.eventually.equal('bar');
        });

        it('can read a record from a reversed column family', function ()
        {
            var promise = pool.cql(commands['select_reversed#cql']).should.be.fulfilled;
            return promise.should.eventually.have.property('length', 1).then(function (value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function (row)
            {
                return row.get('foo').value;
            }).should.eventually.equal('bar');
        });

        it('can request all records from a column family', function ()
        {
            var promise = pool.cql(commands['select*#cql']).should.be.fulfilled;
            return promise.should.eventually.have.property('length', 1).then(function (value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function (row)
            {
                return row.get('foo').value;
            }).should.eventually.equal('bar');
        });

        it('can read a record from a counter column family', function ()
        {
            var promise = pool.cql(commands['select_counter#cql']).should.be.fulfilled;
            return promise.should.eventually.have.property('length', 1).then(function (value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function (row)
            {
                return row.get('foo').value;
            }).should.eventually.equal(10);
        });

        it('can increment a column value and retrieve the updated value', function ()
        {
            var promise = pool.cql(commands['incr#cql']).should.be.fulfilled.then(function ()
            {
                return pool.cql(commands['select_counter#cql']).should.be.fulfilled;
            });
            return promise.should.eventually.have.property('length', 1).then(function (value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function (row)
            {
                return row.get('foo').value;
            }).should.eventually.equal(20);
        });

        it('can return results for malformed queries', function ()
        {
            var select = "SELECT foo FROM cql_test WHERE KEY='?'",
                promise = pool.cql(select, ["'foobar"]).should.be.fulfilled;
            var row = promise.should.eventually.have.property('length', 1).then(function (value)
            {
                return value[0];
            });
            return row.should.eventually.be.an.instanceof(scamandrios.Row).
                       should.eventually.have.property('key', "'foobar").
                       should.eventually.have.property('count', 0);
        });

        it('can return the number of rows matching a query', function ()
        {
            var promise = pool.cql(commands['count#cql']).should.be.fulfilled;
            return promise.should.eventually.have.property('length', 1).then(function (value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function (row)
            {
                return row.get('count').value;
            }).should.eventually.equal(1);
        });

        it('can reject invalid CQL', function ()
        {
            var promise = pool.cql(commands['error#cql']);

            return P.all(
            [
                promise.should.be.rejected,
                promise.fail(function (error)
                {
                    return error;
                }).should.eventually.have.property('name', 'InvalidRequestException').then(function (error)
                {
                    return error.why.length;
                }).should.eventually.be.above(0)
            ]);
        });

        it('can return query results with `gzip` enabled', function ()
        {
            var promise = pool.cql(commands['count#cql'], { 'gzip': true }).should.be.fulfilled;
            return promise.should.eventually.have.property('length', 1).then(function (value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function (row)
            {
                return row.get('count').value;
            }).should.eventually.equal(1);
        });

        it('can delete a column from a row', function ()
        {
            var rows = pool.cql(commands['delete#cql']).should.be.fulfilled.then(function ()
            {
                return pool.cql(commands['select2#cql'], commands['select2#vals']).should.be.fulfilled;
            });
            return rows.should.eventually.have.property('length', 1).then(function (value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function (row)
            {
                return row.count;
            }).should.eventually.equal(0);
        });

        it('can drop a column family', function ()
        {
            return pool.cql(commands['drop_cf#cql']).should.be.fulfilled;
        });

        it('can drop a keyspace', function ()
        {
            return pool.cql(commands['drop_ks#cql']).should.be.fulfilled;
        });

        it('throws if too many CQL parameters are given', function ()
        {
            expect(function ()
            {
                pool.cql(commands['select2#cql'], [1, 2, 3, 4, 5, 6]);
            }).to.throw(/Too Many Parameters Given/);
        });

        it('throws if too few CQL parameters are given', function ()
        {
            expect(function ()
            {
                pool.cql(commands['select2#cql'], []);
            }).to.throw(/Too Few Parameters Given/);
        });

        after(function ()
        {
            var deferred = P.defer();
            pool.on('close', deferred.resolve);
            pool.close();
            return deferred.promise;
        });
    });
});
