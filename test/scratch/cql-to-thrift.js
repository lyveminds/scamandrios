var scamandrios = require('../'),
    connection = new scamandrios.Connection({ 'keyspace': 'test', 'cqlVersion': '3.0.0' });

connection.connect().then(function()
{
    return connection.cql('CREATE TABLE testmap (foo text PRIMARY KEY, bars map<text, boolean>) WITH caching=\'ALL\'');
})
.then(function()
{
    return connection.cql('INSERT INTO testmap (foo, bars) VALUES (?, ?)', ['random', { 'kc@kc.be': true }]);
})
.then(function()
{
    return connection.cql("SELECT * FROM testmap WHERE foo = 'random'");
})
.then(function(rows)
{
    console.log(rows[0][1].value);
}, function(error)
{
    console.log('OH NOES', error, error.stack);
})
.then(function()
{
    return connection.cql('DROP TABLE testmap');
})
.then(function()
{
    connection.close();
});
