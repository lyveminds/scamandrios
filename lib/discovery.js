// Given an initial node, do a query to discover additional nodes.
// Offers conveniences for just returning a list of nodes and for
// creating a pool & returning that.

var
    _          = require('lodash'),
    assert     = require('assert'),
    P          = require('p-promise'),
    Connection = require('./connection'),
    Pool       = require('./pool')
    ;


var DISCOVER_Q = new Buffer('select peer from system.peers');
var SYSTEM_Q = new Buffer('use system');

exports.discover = function(node)
{
    var opts;
    if (typeof node === 'string')
        opts = { host: node };
    else
        opts = node;
    if (!opts.cqlVersion)
        opts.cqlVersion = '3.0.0';

    var seed = new Connection(opts);
    var nodes = [];

    return seed.connect()
    .then(function()
    {
        return seed.cql(SYSTEM_Q);
    })
    .then(function(ignored)
    {
        return seed.cql(DISCOVER_Q);
    })
    .then(function(reply)
    {
        reply.forEach(function(row)
        {
            nodes.push(row.get('peer').value);
        });

        if (nodes.indexOf(opts.host) === -1)
            nodes.push(opts.host);

        return seed.close();
    })
    .then(function(ignored)
    {
        return nodes;
    });
};

exports.discoverPool = function(seed, options)
{
    assert(seed, 'You must pass a seed host to discoverPool');
    assert(options && (typeof options === 'object'), 'You must pass a connection options object to discoverPool');

    return exports.discover(seed)
    .then(function(hosts)
    {
        options.hosts = _.map(hosts, function(i) { return i + ':9160'; });

        return new Pool(options);
    });
};
