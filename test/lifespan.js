/*global describe:true, it:true, before:true, after:true */

var
    chai   = require('chai'),
    assert = chai.assert,
    expect = chai.expect,
    should = chai.should(),
    _      = require('lodash'),
    P      = require('p-promise'),
    util   = require('util'),
    sinon  = require('sinon')
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

describe('connection monitor', function()
{
    it('should check the health of all connections', function(done)
    {
        this.timeout(5000);
        var pool = new scamandrios.ConnectionPool(poolConfig);

        var connections = 0;
        pool.on('log', function(msg)
        {
            if (msg.match(/connection established/))
            {
                connections++;
                if (connections === poolConfig.hosts.length)
                    runTest();
            }
        });

        function runTest()
        {
            pool.monitorConnections()
            .then(function(result)
            {
                assert.isArray(pool.dead, 'dead pool is not array');
                assert.equal(result, 'monitor done', 'unexpected message');
                assert.equal(pool.checkInProgress, false, 'checkInProgress is still true');
                done();
            }).done();
        }

        pool.connect();
    });


    it('rechecks hosts that were dead on first connect', function(done)
    {
        this.timeout(10000);

        var configWithRinger = _.clone(poolConfig, true);
        configWithRinger.hosts.push('10.0.0.1:9137');

        var pool = new scamandrios.ConnectionPool(configWithRinger);

        var connections = 0;
        pool.on('log', function(msg)
        {
            if (msg.match(/connection established/))
                connections++;
            else if (msg.match(/initial connection failed/))
                connections++;

            if (connections === configWithRinger.hosts.length)
                runTest();
        });

        function runTest()
        {
            pool.monitorConnections()
            .then(function(result)
            {
                assert.isArray(pool.dead, 'dead pool is not array');
                assert.equal(pool.dead.length, 1, 'expected 1 dead');
                assert.equal(pool.clients.length, configWithRinger.hosts.length - 1, 'dead client still in the list!');
                assert.equal(pool.checkInProgress, false, 'checkInProgress is still true');
                return pool.close();
            })
            .then(function(r)
            {
                done();
            }).done();
        }

        pool.connect();
    });

    function failWhaleQuery(buf)
    {
        return P.reject('fail whale');
    }

    it('notices when hosts go dead', function(done)
    {
        this.timeout(10000);

        var configWithStub = _.clone(poolConfig, true);
        var pool = new scamandrios.ConnectionPool(configWithStub);

        var connections = 0;
        var errorsLogged = 0;
        var resurrections = 0;
        pool.on('log', function(msg)
        {
            if (msg.match(/connection established/))
            {
                connections++;
                if (connections === poolConfig.hosts.length)
                    runTest();
            }
            else if (msg.match(/ping timeout/))
                errorsLogged++;
            else if (msg.match(/rising from the dead/))
                resurrections++;
            else
                console.log(msg);
        });

        function runTest()
        {
            // replace client #1 with a failing stub
            var stubbed = pool.clients[0];
            var stub = sinon.stub(stubbed, 'executeCQL', failWhaleQuery);

            pool.monitorConnections()
            .then(function(result)
            {
                assert.isArray(pool.dead, 'dead pool is not array');
                assert.equal(errorsLogged, 1, 'expected at least one error');
                assert.equal(resurrections, 1, 'expected one resurrection');
                assert.equal(pool.dead.length, 0, 'expected 0 dead');
                assert.equal(pool.checkInProgress, false, 'checkInProgress is still true');
                done();
            })
            .fail(function(err)
            {
                console.log(err);
            }).done();
        }

        pool.connect();
    });
});
