/*global describe:true, it:true, before:true, after:true */

var
    _      = require('lodash'),
    demand = require('must'),
    P      = require('p-promise'),
    util   = require('util'),
    sinon  = require('sinon')
    ;

var
    scamandrios = require('../index'),
    Connection = scamandrios.Connection,
    poolConfig = _.clone(require('./helpers/connection'), true)
    ;

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
                pool.must.have.property('dead');
                pool.dead.must.be.an.array();
                result.must.be.an.array();
                result.length.must.equal(0);
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
                pool.must.have.property('dead');
                pool.dead.must.be.an.array();
                pool.dead.length.must.equal(1);
                pool.clients.length.must.equal(configWithRinger.hosts.length - 1);
                return pool.close();
            })
            .then(function(r)
            {
                done();
            }).done();
        }

        pool.connect();
    });

    it('individual clients start ping timers', function(done)
    {
        var pieces = poolConfig.hosts[0].split(':');
        var conn = new scamandrios.Connection({ host: pieces[0], port: pieces[1] });

        demand(conn._pingTimer).be.null();
        conn.connect()
        .then(function()
        {
            conn._pingTimer.must.be.truthy(done);
            conn.close();
            done();
        }).done();
    });

    it('ping is called when the timer fires', function(done)
    {
        this.timeout(5000);
        var pieces = poolConfig.hosts[0].split(':');
        scamandrios.Connection.prototype.pingInterval = 500;

        var conn = new scamandrios.Connection({ host: pieces[0], port: pieces[1] });
        conn.on('ping', function(healthy)
        {
            healthy.must.be.true();
            conn.close();
            done();
        });

        conn.connect()
        .then(function()
        {
            // no-op
        }).done();
    });

    function failWhaleQuery(buf)
    {
        return P.reject('fail whale');
    }

    it('notices when client pings fail', function(done)
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
                if (connections === configWithStub.hosts.length)
                    runTest();
            }
            else if (msg.match(/unhealthy node/))
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

            stubbed.ping()
            .then(function()
            {
                throw new Error('this ping was supposed to fail!');
            })
            .fail(function(err)
            {
                pool.must.have.property('dead');
                pool.dead.must.be.an.array();
                pool.dead.length.must.equal(1);
                errorsLogged.must.equal(1);

                done();
            }).done();
        }

        pool.connect();
    });
});
