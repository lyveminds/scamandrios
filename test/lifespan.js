/*global describe:true, it:true, before:true, after:true */

var
    chai = require('chai'),
    assert = chai.assert,
    expect = chai.expect,
    should = chai.should(),
    _ = require('lodash'),
    P = require('p-promise'),
    util = require('util')
    ;

var
    scamandrios = require('../index'),
    Connection = scamandrios.Connection,
    poolConfig = _.clone(require('./helpers/connection'), true)
    ;

describe('connection lifespan', function()
{
    before(function()
    {
        // mangle the pool object a bit
        poolConfig.cqlVersion = '3.0.0';
        poolConfig.host = poolConfig.hosts[0];
    });

    it('Connection.TTL should default to 20 minutes', function()
    {
        Connection.should.have.property('TTL');
        assert.equal(Connection.TTL, 20 * 60 * 1000, 'connection TTL is not 20 minutes');
    });

    it('options passed to the constructor can override the TTL', function()
    {
        var myopts = _.extend({}, poolConfig);
        myopts.TTL = 5;
        var c = new Connection(myopts);

        assert.equal(c.TTL, myopts.TTL);
    });

    it('should set a timer on a successful connection', function(done)
    {
        var c = new Connection(poolConfig);
        c.connect()
        .then(function()
        {
            assert.ok(c.connectionLifespan);
            assert.ok(c.connectionLifespan._idleTimeout);
        })
        .fail(function(err)
        {
            console.log(err);
            should.not.exist(err);
        }).done(function()
        {
            c.close();
            done();
        });
    });

    it('should tear down & rebuild the connection at the end of the TTL', function(done)
    {
        var myopts = _.extend({}, poolConfig);
        myopts.TTL = 500;
        var c = new Connection(myopts);
        var original;

        c.on('log', function(message)
        {
            if (message === 'successfully reconnected after connection end-of-life')
            {
                assert.ok(original !== c._connection);
                c.close();
                done();
            }
        });

        c.connect()
        .then(function()
        {
            original = c._connection;
        })
        .fail(function(err)
        {
            console.log(err);
            should.not.exist(err);
        }).done();
    });

    it('pools should jitter the lifespan of their connections', function()
    {
        if (poolConfig.hosts.length < 2)
        {
            poolConfig.hosts.push(poolConfig.hosts[0]);
            poolConfig.hosts.push(poolConfig.hosts[0]);
            poolConfig.hosts.push(poolConfig.hosts[0]);
            poolConfig.hosts.push(poolConfig.hosts[0]);
            poolConfig.hosts.push(poolConfig.hosts[0]);
        }

        var pool = new scamandrios.ConnectionPool(poolConfig);
        var ttls = _.map(pool.hosts, function(h)
        {
            return pool.createConnection(h).TTL;
        });

        assert.ok(_.all(ttls, _.isNumber));
        assert.ok(_.all(ttls, function(n) { return n >= Connection.TTL; }));
        assert.equal(ttls.length, _.uniq(ttls).length);
    });
});
