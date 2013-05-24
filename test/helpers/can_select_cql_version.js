var scamandrios = require('../../'),
    P = require('p-promise');

module.exports = function canSelectCQLVersion(options)
{
    var connection = new scamandrios.Connection(options),
        deferred = P.defer();
    connection.on('close', deferred.resolve);
    function handleSettle(error)
    {
        connection.close();
        return deferred.promise.then(function ()
        {
            return !(/set_cql_version/.test(error));
        });
    }
    return connection.connect().then(handleSettle, handleSettle);
};
