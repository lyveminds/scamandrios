/*global describe: true, it: true, before: true, after: true */

var _              = require('lodash'),
    demand         = require('must'),
    chai           = require('chai'),
    chaiAsPromised = require('chai-as-promised'),
    P              = require('p-promise');

require('mocha-as-promised')();

var scamandrios = require('../');

var poolSettings        = _.clone(require('./helpers/connection.json')),
    badSettings         = require('./helpers/bad_connection.json'),
    commands            = require('./helpers/cql2.json'),
    canSelectCQLVersion = require('./helpers/can_select_cql_version');

var assert = chai.assert,
    expect = chai.expect;

chai.should();
chai.use(chaiAsPromised);

describe('CQL 2', function()
{
    var pool;

    before(function()
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

    describe('#connect()', function()
    {
        it('should reject connections to nonexistent endpoints', function()
        {
            var badPool = new scamandrios.ConnectionPool(badSettings);
            badPool.on('error', function (error)
            {
                expect(error).to.exist;
            });

            return badPool.connect().should.be.rejected.then(function()
            {
                return badPool.cql(commands['create_ks#cql']).should.be.rejected;
            }).then(function()
            {
                badPool.close();
            });
        });

        it('can create a keyspace', function()
        {
            return pool.cql(commands['create_ks#cql']).should.be.fulfilled;
        });

        it('can use a keyspace', function()
        {
            return pool.executeCQLAllClients(new Buffer(commands['use#cql'])).should.be.fulfilled;
        });

        it('can create a column family', function()
        {
            return pool.cql(commands['create_cf#cql']).should.be.fulfilled;
        });

        it('can create a column family holding counters', function()
        {
            return pool.cql(commands['create_counter_cf#cql']).should.be.fulfilled;
        });

        it('can create a column family with a reversed comparator', function()
        {
            return pool.cql(commands['create_reversed_cf#cql']).should.be.fulfilled;
        });

        it('can update a value in a column family', function()
        {
            return pool.cql(commands['update#cql']).should.be.fulfilled;
        });

        it('can update a value in a reversed column family', function()
        {
            return pool.cql(commands['update_reversed#cql']).should.be.fulfilled;
        });

        it('can increment a counter column value', function()
        {
            return pool.cql(commands['incr#cql']).should.be.fulfilled;
        });

        it('can read a record from a column family', function(done)
        {
            pool.cql(commands['select#cql'])
            .then(function(result)
            {
                result.length.must.equal(1);
                var row = result[0];
                row.must.be.instanceof(scamandrios.Row);
                row.get('foo').value.must.equal('bar');

                done();
            }).done();
        });

        it('can read a record from a reversed column family', function(done)
        {
            pool.cql(commands['select_reversed#cql'])
            .then(function(result)
            {
                result.length.must.equal(1);
                var row = result[0];
                row.must.be.instanceof(scamandrios.Row);
                row.get('foo').value.must.equal('bar');

                done();
            }).done();
        });

        it('can request all records from a column family', function(done)
        {
            pool.cql(commands['select*#cql'])
            .then(function(result)
            {
                result.length.must.equal(1);
                var row = result[0];
                row.must.be.instanceof(scamandrios.Row);
                row.get('foo').value.must.equal('bar');

                done();
            }).done();
        });

        it('can read a record from a counter column family', function(done)
        {
            pool.cql(commands['select_counter#cql'])
            .then(function(result)
            {
                result.length.must.equal(1);
                var row = result[0];
                row.must.be.instanceof(scamandrios.Row);
                row.get('foo').value.must.equal(10);

                done();
            }).done();
        });

        it('can increment a column value and retrieve the updated value', function(done)
        {
            pool.cql(commands['incr#cql'])
            .then(function()
            {
                return pool.cql(commands['select_counter#cql']);
            })
            .then(function(result)
            {
                result.length.must.equal(1);
                var row = result[0];
                row.must.be.instanceof(scamandrios.Row);
                row.get('foo').value.must.equal(20);

                done();
            }).done();
        });

        it('can return results for malformed queries', function(done)
        {
            var select = "SELECT foo FROM cql_test WHERE KEY='?'";

            pool.cql(select, ["'foobar"])
            .then(function(result)
            {
                result.length.must.equal(1);
                var row = result[0];
                row.must.be.instanceof(scamandrios.Row);
                row.must.have.property('key');
                row.must.have.property('length');

                row.key.must.equal("'foobar");
                row.length.must.equal(0);

                done();
            }).done();
        });

        it('can return the number of rows matching a query', function(done)
        {
            pool.cql(commands['count#cql'])
            .then(function(result)
            {
                result.length.must.equal(1);
                var row = result[0];
                row.must.be.instanceof(scamandrios.Row);
                row.get('count').value.must.equal(1);

                done();
            }).done();
        });

        it('can reject invalid CQL', function(done)
        {
            pool.cql(commands['error#cql'])
            .then(function(result)
            {
                throw new Error('this query should have resulted in an error!');
            })
            .fail(function(err)
            {
                err.must.have.property('name');
                err.name.must.equal('InvalidRequestException');
                err.why.must.match(/no viable alternative/);

                done();
            }).done();
        });

        it('can return query results with `gzip` enabled', function(done)
        {
            pool.cql(commands['count#cql'], { 'gzip': true })
            .then(function(result)
            {
                result.length.must.equal(1);
                var row = result[0];
                row.must.be.instanceof(scamandrios.Row);
                row.get('count').value.must.equal(1);

                done();
            }).done();
        });

        it('can delete a column from a row', function(done)
        {
            pool.cql(commands['delete#cql'])
            .then(function(result)
            {
                return pool.cql(commands['select2#cql'], commands['select2#vals']);s
            })
            .then(function(result)
            {
                result.length.must.equal(1);
                var row = result[0];
                row.must.be.instanceof(scamandrios.Row);
                row.length.must.equal(0);

                done();
            }).done();
        });

        it('can drop a column family', function()
        {
            return pool.cql(commands['drop_cf#cql']).should.be.fulfilled;
        });

        it('can drop a keyspace', function()
        {
            return pool.cql(commands['drop_ks#cql']).should.be.fulfilled;
        });

        it('throws if too many CQL parameters are given', function()
        {
            expect(function()
            {
                pool.cql(commands['select2#cql'], [1, 2, 3, 4, 5, 6]);
            }).to.throw(/too many parameters provided for query format string/);
        });

        it('throws if too few CQL parameters are given', function()
        {
            expect(function()
            {
                pool.cql(commands['select2#cql'], []);
            }).to.throw(/not enough parameters given to satisfy query format string/);
        });

        after(function()
        {
            var deferred = P.defer();
            pool.on('close', deferred.resolve);
            pool.close();
            return deferred.promise.should.be.fulfilled;
        });
    });
});
