/*global describe:true, it:true, before:true, after:true */

var
    _           = require('lodash'),
    demand      = require('must'),
    P           = require('p-promise'),
    scamandrios = require('../index'),
    sinon       = require('sinon'),
    util        = require('util')
    ;

var seedNode = '172.16.9.37:9160';
var proxySeed = 'cassint.discovery.inf.blackpearlsystems.net:9160';
var fullRing = [ '172.16.38.120:9160', '172.16.22.114:9160', '172.16.9.37:9160' ];

if (process.env.TRAVIS)
{
    seedNode = '10.0.0.1';
    proxySeed = 'localhost';
    fullRing = [ '10.0.0.1:9160' ];
}

var DiscoveryPool = require('../lib/discovery-pool');

describe('DiscoveryPool', function()
{
    describe('constructor', function()
    {
        it('requires a seed node in its constructor', function()
        {
            function shouldThrow() { return new DiscoveryPool(); }

            shouldThrow.must.throw(/must pass a `seed` option/);
        });

        it('can be constructed', function()
        {
            var pool = new DiscoveryPool(seedNode, {});
            pool.must.be.an.object();
            pool.must.be.instanceof(DiscoveryPool);
        });
    });

    describe('discoverNodes()', function()
    {
        it('returns a promise that resolves to an array', function(done)
        {
            var pool = new DiscoveryPool(seedNode, {});
            var p = pool.discoverNodes();

            p.must.be.an.object();
            p.must.have.property('then');
            p.then.must.be.a.function();

            p.then(function(hosts)
            {
                hosts.must.be.an.array();
                hosts.length.must.be.above(0);
                done();
            });
        });
    });

    describe('connect()', function()
    {
        it('calls discoverNodes() and the original connect()', function(done)
        {
            var pool = new DiscoveryPool(seedNode, {});
            var discoverSpy = sinon.spy(pool, 'discoverNodes');
            var connectSpy = sinon.spy(pool, '_connect');

            pool.connect()
            .then(function(result)
            {
                discoverSpy.called.must.be.true();
                connectSpy.called.must.be.true();
                pool.hosts.must.eql(fullRing);

                pool.close();
                done();
            }).done();
        });

        it('starts the rediscovery timer', function(done)
        {
            var pool = new DiscoveryPool(seedNode, {});

            pool.connect()
            .then(function(result)
            {
                pool._rediscoveryTimer.must.be.truthy();

                pool.close();
                done();
            }).done();
        });
    });

    describe('rediscovery()', function()
    {
        it('calls discoverNodes()', function(done)
        {
            var pool = new DiscoveryPool(seedNode, {});
            var discoverSpy = sinon.spy(pool, 'discoverNodes');

            pool.rediscovery()
            .then(function()
            {
                discoverSpy.called.must.be.true();
                done();
            });
        });

        it('updates its host list on node change', function(done)
        {
            var pool = new DiscoveryPool(seedNode, {});
            var stub = sinon.stub(pool, 'discoverNodes', function()
            {
                return P([ '172.16.38.120:9160', '172.16.22.114:9160' ]);
            });

            pool.rediscovery()
            .then(function(hosts)
            {
                hosts.must.be.an.array();
                hosts.length.must.equal(2);
                pool.hosts.length.must.equal(hosts.length);
                pool.clients.length.must.equal(hosts.length);

                pool.close();
                done();
            }).done();
        });
    });
});
