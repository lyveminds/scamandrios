/*global describe:true, it:true, before:true, after:true */

var
    chai = require('chai'),
    assert = chai.assert,
    expect = chai.expect,
    should = chai.should(),
    chaiAsPromised = require('chai-as-promised'),
    util = require('util'),
    scamandrios = require('../index'),
    P = require('p-promise')
    ;

require('mocha-as-promised')();
chai.use(chaiAsPromised);

var
    badConfig = require('./helpers/bad_connection'),
    poolConfig = require('./helpers/connection'),
    config = require('./helpers/cql3')
    ;



// --- todo

function canSelectCQLVersion(poolConfig, callback)
{
    var conn = new scamandrios.Connection(poolConfig);
    conn.connect(function(err)
    {
        var canSelect = !(err && err.toString().indexOf('set_cql_version') !== -1);
        conn.on('close', function()
        {
            callback(canSelect);
        });
        conn.close();
    });
}



describe('cql3', function()
{

    var conn;
    poolConfig.cqlVersion = '3.0.0';

    before(function()
    {
        conn = new scamandrios.ConnectionPool(poolConfig);
        var promise = conn.connect();
        return promise.should.be.fulfilled;

        // TODO should try to select cql version to see if these tests are relevant
    });

    describe('connection and keyspaces', function()
    {

        it('returns an error on a bad connection', function()
        {
            var badConn = new scamandrios.ConnectionPool(badConfig);
            var promise = badConn.connect();
            return promise.should.be.rejected;
        });

        it('can create a keyspace', function()
        {
            var testquery;
            if ((conn.clients[0].version[0] === '19') && (conn.clients[0].version[1] < '34'))
                testquery = config['create_ks#cql_v1'];
            else
                testquery = config['create_ks#cql'];

            var promise = conn.cql(testquery);
            return promise.should.be.fulfilled;
        });

        it('can use a keyspace', function()
        {
            var testquery = config['use#cql'];
            var promise = conn.cql(testquery);
            return promise.should.be.fulfilled;
        });
    });

    describe('static column families', function()
    {
        it('can create a static column family', function()
        {
            var testquery = config['static_create_cf#cql'];
            var promise = conn.cql(testquery);
            return promise.should.be.fulfilled;
        });

        it('static count CF create column family', function()
        {
            var testquery = config['static_create_cnt_cf#cql'];
            var promise = conn.cql(testquery);
            return promise.should.be.fulfilled;
        });

        it('update', function()
        {
            var testquery = config['static_update#cql'];
            var promise = conn.cql(testquery);
            return promise.should.be.fulfilled;
        });

        it('static counter CF update', function()
        {
            var testquery = config['static_update_cnt#cql'];
            var promise = conn.cql(testquery);
            return promise.should.be.fulfilled;
        });

        it('select', function()
        {
            var testquery = config['static_select#cql'];
            var promise = conn.cql(testquery);

            return promise.then(function(result)
            {
                result.length.should.equal(1);
                (result[0] instanceof scamandrios.Row).should.equal(true);
                console.log(result[0]);
                var count = result[0].get('cnt');
                count.should.be.an('object');
                count.should.have.property('value')
                count.value.should.equal(10);
                return true;
            });
        });

        it('select *', function()
        {
            var testquery = config['static_select*#cql'];
            var promise = conn.cql(testquery);

            return promise.then(function(result)
            {
                result.length.should.equal(1);
                (result[0] instanceof scamandrios.Row).should.equal(true);
                result[0].get('foo').value.should.equal('bar');
                return true;
            });
        });

        it('static counter CF select', function()
        {
            var testquery = config['static_select_cnt#cql'];
            var promise = conn.cql(testquery);

            return promise.then(function(result)
            {
                result.length.should.equal(1);
                (result[0] instanceof scamandrios.Row).should.equal(true);
                result[0].get('cnt').value.should.equal(10);
                return true;
            });
        });

        it('test cql static counter CF incr and select', function()
        {
            var testquery = config['static_update_cnt#cql'];
            var promise = conn.cql(testquery);
            return promise.then(function(v1)
            {
                var p2 = conn.cql(config['static_select_cnt#cql']);
                p2.then(function(result)
                {
                    result.length.should.equal(1);
                    (result[0] instanceof scamandrios.Row).should.equal(true);
                    result[0].get('cnt').value.should.equal(20);
                    return true;
                });
            });
        });

        // continue here
        it('select with bad user input', function()
        {
            var promise = conn.cql("SELECT foo FROM cql_test WHERE id='?'", ["'foobar"]);
            return promise.should.be.fulfilled;
        });

        it('count', function()
        {
            var promise = conn.cql(config['static_count#cql']);
            return promise.then(function(result)
            {
                result.length.should.equal(1);
                result[0].get('count').value.should.equal(1);
                return true;
            });
        });

        it('error', function()
        {
            var promise = conn.cql(config['error#cql']);
            return promise.then(function(res)
            {
                // should not arrive here
                should.not.exist(res);
            }, function(error)
            {
                error.name.should.equal('InvalidRequestException');
                error.message.length.should.be.above(0);
                return true;
            });
        });

        it('count with gzip', function()
        {
            var promise = conn.cql(config['static_count#cql'], { gzip:true });
            return promise.then(function(result)
            {
                result.length.should.equal(1);
                result[0].get('count').should.equal(1);
                return true;
            });
        });

        it('delete', function()
        {
            var promise = conn.cql(config['static_delete#cql']);
            return promise.then(function(resp)
            {
                var promise2 = conn.cql(config['static_select2#cql'], config['static_select2#vals']);
                promise2.should.be.fulfilled;
                return true;
            });
        });

        it('drop a static column family', function()
        {
            var promise = conn.cql(config['static_drop_cf#cql']);
            return promise.should.be.fulfilled;
        });
    });

    describe('dynamic column families', function()
    {
        it('can create a dynamic column family', function()
        {
            var promise = conn.cql(config['dynamic_create_cf#cql']);
            return promise.should.be.fulfilled;
        });

        it('can update 1', function()
        {
            var promise = conn.cql(config['dynamic_update#cql'], config['dynamic_update#vals1']);
            return promise.should.be.fulfilled;
       });

        it('can update 2', function()
        {
            var promise = conn.cql(config['dynamic_update#cql'], config['dynamic_update#vals2']);
            return promise.should.be.fulfilled;
        });

        it('can update 3', function()
        {
            var promise = conn.cql(config['dynamic_update#cql'], config['dynamic_update#vals3']);
            return promise.should.be.fulfilled;
        });

        it('can select by row', function()
        {
            var promise = conn.cql(config['dynamic_select1#cql']);
            return promise.then(function(result)
            {
                result.length.should.equal(2);
                (result[0] instanceof scamandrios.Row).should.equal(true);
                (result[1] instanceof scamandrios.Row).should.equal(true);
                result[0].get('ts').value.getTime().should.equal(new Date('2012-03-01').getTime());
                result[1].get('ts').value.getTime().should.equal(new Date('2012-03-02').getTime());
                return true;
            });
        });
    });

    describe('dense composite column family', function()
    {
        it('can create a dense composite CF', function()
        {
            var promise = conn.cql(config['dense_create_cf#cql']);
            return promise.should.be.fulfilled;
        });

        it('can update 1', function()
        {
            var promise = conn.cql(config['dense_update#cql'], config['dense_update#vals1']);
            return promise.should.be.fulfilled;
        });

        it('can update 2', function()
        {
            var promise = conn.cql(config['dense_update#cql'], config['dense_update#vals2']);
            return promise.should.be.fulfilled;
        });

        it('can update 3', function()
        {
            var promise = conn.cql(config['dense_update#cql'], config['dense_update#vals3']);
            return promise.should.be.fulfilled;
        });


        it('can select by row', function()
        {
            var promise = conn.cql(config['dense_select1#cql']);
            return promise.then(function(result)
            {
                result.length.should.equal(2);

                result[0].length.should.equal(2);
                result[0].get('ts').value.getTime().should.equal(new Date('2012-03-02').getTime());
                result[0].get('port').value.should.equal(1337);

                result[1].length.should.equal(2);
                result[1].get('ts').value.getTime().should.equal(new Date('2012-03-01').getTime());
                result[1].get('port').value.should.equal(8080);
                return true;
            });
        });

        it('can select by row and column', function()
        {
            var promise = conn.cql(config['dense_select2#cql']);

            promise.then(function(result)
            {
                result.length.should.equal(1);
                result[0].length.should.equal(4);
                result[0].get('userid').value.should.equal(10);
                result[0].get('ip').value.should.equal('192.168.1.1');
                result[0].get('port').value.should.equal(1337);
                result[0].get('ts').value.getTime().should.equal(new Date('2012-03-02').getTime());
                return true;
            });
        });
    });

    describe('sparse composite column family', function()
    {
        it('can create column family', function()
        {
            var promise = conn.cql(config['sparse_create_cf#cql']);
            return promise.should.be.fulfilled;
        });

        it('can update 1', function()
        {
            var promise = conn.cql(config['sparse_update#cql'], config['sparse_update#vals1']);
            return promise.should.be.fulfilled;
        });

        it('can update 2', function()
        {
            var promise = conn.cql(config['sparse_update#cql'], config['sparse_update#vals2']);
            return promise.should.be.fulfilled;
        });

        it('can update 3', function()
        {
            var promise = conn.cql(config['sparse_update#cql'], config['sparse_update#vals3']);
            return promise.should.be.fulfilled;
        });

        it('can select by row', function()
        {
            var promise = conn.cql(config['sparse_select1#cql']);

            return promise.then(function(result)
            {
                result.length.should.equal(1),
                (result[0] instanceof scamandrios.Row).should.equal(true);
                result[0].length.should.equal(3);
                result[0].get('posted_at').value.getTime().should.equal(new Date('2012-03-02').getTime());
                result[0].get('body').value.should.equal('body text 3');
                result[0].get('posted_by').value.should.equal('author3');
                return true;
            });
        });

/*

        it('can select by row and column', function()
        {
            var promise = conn.cql(config['sparse_select2#cql'];

            assert.strictEqual(res.length, 1);
            assert.ok(res[0] instanceof Helenus.Row);
            assert.strictEqual(res[0].length, 2);
            assert.strictEqual(res[0].get('body').value, 'body text 1');
            assert.strictEqual(res[0].get('posted_by').value, 'author1');
        }),
*/
    });

    describe('dropping keyspaces', function()
    {
        it('can drop keyspace', function()
        {
            var promise = conn.cql(config['drop_ks#cql']);
            return promise.should.be.fulfilled;
        });
    });


    after(function(done)
    {
        conn.close();
        done();
    });

});

