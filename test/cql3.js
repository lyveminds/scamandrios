/*global describe:true, it:true, before:true, after:true */

var
    chai = require('chai'),
    assert = chai.assert,
    expect = chai.expect,
    should = chai.should(),
    chaiAsPromised = require('chai-as-promised'),
    util = require('util'),
    scamandrios = require('../index'),
    P = require('p-promise'),
    _ = require('lodash')
    ;

require('mocha-as-promised')();
chai.use(chaiAsPromised);

var
    badConfig = require('./helpers/bad_connection'),
    poolConfig = _.clone(require('./helpers/connection'), true),
    config = require('./helpers/cql3'),
    canSelectCQLVersion = require('./helpers/can_select_cql_version')
    ;

describe('cql3', function()
{

    var conn;
    before(function()
    {
        poolConfig.cqlVersion = '3.0.0';
        conn = new scamandrios.ConnectionPool(poolConfig);
        return conn.connect().should.be.fulfilled;
    });

    describe('connection and keyspaces', function()
    {

        it('returns an error on a bad connection', function()
        {
            var badConn = new scamandrios.ConnectionPool(badConfig);
            badConn.on('error', function(error)
            {
                expect(error).to.exist;
            });
            var promise = badConn.connect();
            return promise.should.be.rejected.then(function()
            {
                badConn.close();
            });
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
            var testquery = new Buffer(config['use#cql']);
            var promise = conn.executeCQLAllClients(testquery);
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
            var promise = conn.cql(testquery).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 1).then(function(value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function(row)
            {
                return row.get('foo');
            }).should.eventually.have.property('value', 'bar');
        });

        it('select *', function()
        {
            var testquery = config['static_select*#cql'];
            var promise = conn.cql(testquery).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 1).then(function(value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function(row)
            {
                return row.get('foo');
            }).should.eventually.have.property('value', 'bar');
        });

        it('static counter CF select', function()
        {
            var testquery = config['static_select_cnt#cql'];
            var promise = conn.cql(testquery).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 1).then(function(value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function(row)
            {
                return row.get('cnt');
            }).should.eventually.have.property('value', 10);
        });

        it('test cql static counter CF incr and select', function()
        {
            var testquery = config['static_update_cnt#cql'];
            var promise = conn.cql(testquery).should.be.fulfilled;

            return promise.then(function(v1)
            {
                var p2 = conn.cql(config['static_select_cnt#cql']);
                return p2.should.eventually.have.property('length', 1).then(function(value)
                {
                    return value[0];
                });
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function(row)
            {
                return row.get('cnt');
            }).should.eventually.have.property('value', 20);
        });

        // continue here
        it('select with bad user input', function()
        {
            var promise = conn.cql("SELECT foo FROM cql_test WHERE id='?'", ["'foobar"]);
            return promise.should.be.fulfilled;
        });

        it('count', function()
        {
            var promise = conn.cql(config['static_count#cql']).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 1).then(function(value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function(row)
            {
                return row.get('count');
            }).should.eventually.have.property('value', 1);
        });

        it('error', function()
        {
            var promise = conn.cql(config['error#cql']);

            return P.all(
            [
                promise.should.be.rejected.with(Error),
                promise.fail(_.identity).should.eventually.have.property('name', 'InvalidRequestException').then(function(error)
                {
                    return error.why.length;
                }).should.eventually.be.above(0)
            ]);
        });

        it('count with gzip', function()
        {
            var promise = conn.cql(config['static_count#cql'], { gzip:true }).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 1).then(function(value)
            {
                return value[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).then(function(row)
            {
                return row.get('count').value;
            }).should.eventually.equal(1);
        });

        it('delete', function()
        {
            var promise = conn.cql(config['static_delete#cql']).should.be.fulfilled;
            return promise.then(function(resp)
            {
                var promise2 = conn.cql(config['static_select2#cql'], config['static_select2#vals']);
                return promise2.should.be.fulfilled;
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
            function getTimeFromRow(row)
            {
                return row.get('ts').value.getTime();
            }

            var promise = conn.cql(config['dynamic_select1#cql']).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 2).then(function(results)
            {
                var timeStamps = _.map(results, function(result)
                {
                    var assertion = P(result).should.eventually.be.an.instanceof(scamandrios.Row);
                    return assertion.then(getTimeFromRow);
                });
                return P.all(timeStamps);
            }).should.become([new Date('2012-03-01').getTime(), new Date('2012-03-02').getTime()]);
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
            function getDataFromRow(row)
            {
                return [row.get('ts').value.getTime(), row.get('port').value];
            }

            var promise = conn.cql(config['dense_select1#cql']).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 2).then(function(results)
            {
                var timeStamps = _.map(results, function(result)
                {
                    var assertion = P(result).should.eventually.be.an.instanceof(scamandrios.Row).should.eventually.have.property('length', 2);
                    return assertion.then(getDataFromRow);
                });
                return P.all(timeStamps);
            }).should.become([[new Date('2012-03-02').getTime(), 1337], [new Date('2012-03-01').getTime(), 8080]]);
        });

        it('can select by row and column', function()
        {
            var promise = conn.cql(config['dense_select2#cql']).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 1).then(function(result)
            {
                return result[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).should.eventually.have.property('length', 4).then(function(result)
            {
                var values =
                {
                    'userid': result.get('userid').value,
                    'ip': result.get('ip').value,
                    'port': result.get('port').value,
                    'ts': result.get('ts').value.getTime()
                };
                return values;
            }).should.become(
            {
                'userid': 10,
                'ip': '192.168.1.1',
                'port': 1337,
                'ts': new Date('2012-03-02').getTime()
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
            var promise = conn.cql(config['sparse_select1#cql']).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 1).then(function(result)
            {
                return result[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).should.eventually.have.property('length', 3).then(function(result)
            {
                var values =
                {
                    'posted_at': result.get('posted_at').value.getTime(),
                    'body': result.get('body').value,
                    'posted_by': result.get('posted_by').value
                };
                return values;
            }).should.become(
            {
                'posted_at': new Date('2012-03-02').getTime(),
                'body': 'body text 3',
                'posted_by': 'author3'
            });
        });

        it('can select by row and column', function()
        {
            var promise = conn.cql(config['sparse_select2#cql']).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 1).then(function(result)
            {
                return result[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).should.eventually.have.property('length', 2).then(function(result)
            {
                var values =
                {
                    'body': result.get('body').value,
                    'posted_by': result.get('posted_by').value,
                };
                return values;
            }).should.become(
            {
                'body': 'body text 1',
                'posted_by': 'author1'
            });
        });

    });

    describe('uuids', function()
    {
        it('can create column family', function()
        {
            var promise = conn.cql(config['uuid_create_cf#cql']);
            return promise.should.be.fulfilled;
        });

        it('can update 1', function()
        {
            var promise = conn.cql(config['uuid_update#cql'], config['uuid_update#vals1']);
            return promise.should.be.fulfilled;
        });

        it('can update 2', function()
        {
            var promise = conn.cql(config['uuid_update#cql'], config['uuid_update#vals2']);
            return promise.should.be.fulfilled;
        });

        it('can update 3', function()
        {
            var promise = conn.cql(config['uuid_update#cql'], config['uuid_update#vals3']);
            return promise.should.be.fulfilled;
        });

        it('can update 4', function()
        {
            var promise = conn.cql(config['uuid_update#cql'], config['uuid_update#vals4']);
            return promise.should.be.fulfilled;
        });

        it('can select by UUID', function()
        {
            return P.all(
            [
                conn.cql(config['uuid_select1#cql']),
                conn.cql(config['uuid_select2#cql']),
                conn.cql(config['uuid_select3#cql']),
                conn.cql(config['uuid_select4#cql'])
            ])
            .should.be.fulfilled
            .then(function(results)
            {
                var columns = _.reduce(results, function(memo, result)
                {
                    return memo.concat(_.map(result, function(row)
                    {
                        return { 'v1': row.get('v1').value, 'v4': row.get('v4').value };
                    }));
                }, []);

                return _.isEqual(columns,
                [
                    {
                        'v4': new scamandrios.UUID('f7c563bb-3414-4e87-a719-a08be67eba24'),
                        'v1': new scamandrios.TimeUUID('44071c10-ec58-11e2-8dfa-6f0e4ebf00b7')
                    },

                    {
                        'v4': new scamandrios.UUID('2942c3c6-2096-48b4-b005-bb922de819e7'),
                        'v1': new scamandrios.TimeUUID('4d18e310-ec58-11e2-8dfa-6f0e4ebf00b7')
                    },
                    {
                        'v4': new scamandrios.UUID('c6d5f879-658d-4a3e-8bab-ce825c792c7e'),
                        'v1': new scamandrios.TimeUUID('52789e90-ec58-11e2-8dfa-6f0e4ebf00b7')
                    },
                    {
                        'v4': new scamandrios.UUID('b2668afb-788a-4a61-90da-863d21fdb73d'),
                        'v1': new scamandrios.TimeUUID('58fc1990-ec58-11e2-8dfa-6f0e4ebf00b7')
                    }
                ]);
            }).should.become(true)
        });
    });

    describe('integers', function()
    {
        it('can create column family', function()
        {
            var promise = conn.cql(config['integers_create_cf#cql']);
            return promise.should.be.fulfilled;
        });

        it('can update 1', function()
        {
            var promise = conn.cql(config['integers_update#cql'], config['integers_update#vals1']);
            return promise.should.be.fulfilled;
        });

        it('can update 2', function()
        {
            var promise = conn.cql(config['integers_update#cql'], config['integers_update#vals2']);
            return promise.should.be.fulfilled;
        });

        it('can update 3', function()
        {
            var promise = conn.cql(config['integers_update#cql'], config['integers_update#vals3']);
            return promise.should.be.fulfilled;
        });

        it('can select positive numbers', function()
        {
            var promise = conn.cql(config['integers_select1#cql']).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 1).then(function(result)
            {
                return result[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).should.eventually.have.property('length', 3).then(function(result)
            {
                return _.pluck([result.get('number'), result.get('longnumber'), result.get('varnumber')], 'value');
            }).should.become([1, 25, 36]);
        });

        it('can select negative numbers', function()
        {
            var promise = conn.cql(config['integers_select2#cql']).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 1).then(function(result)
            {
                return result[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).should.eventually.have.property('length', 3).then(function(result)
            {
                return _.pluck([result.get('number'), result.get('longnumber'), result.get('varnumber')], 'value');
            }).should.become([-1, -25, -36]);
        });

        it('can select negative numbers with 3 byte varint', function()
        {
            var promise = conn.cql(config['integers_select3#cql']).should.be.fulfilled;

            return promise.should.eventually.have.property('length', 1).then(function(result)
            {
                return result[0];
            }).should.eventually.be.an.instanceof(scamandrios.Row).should.eventually.have.property('length', 3).then(function(result)
            {
                return _.pluck([result.get('number'), result.get('longnumber'), result.get('varnumber')], 'value');
            }).should.become([-2, -25, -8388607]);
        });
    });

    describe('timestamps', function()
    {
        it('can serialize & deserialize javascript date objects', function(done)
        {
            var now = new Date();
            var then = new Date(2008, 10, 12);

            conn.cql(config.dates_create_cf)
            .then(function(result)
            {
                return conn.cql(config.dates_insert, [1, then, now]);
            })
            .then(function(result)
            {
                return conn.cql(config.dates_select, [1]);
            })
            .then(function(results)
            {
                results.length.should.equal(1);
                var row = results[0];

                var created = row.get('created').value;
                var modified = row.get('modified').value;

                created.should.be.a('date');
                modified.should.be.a('date');

                assert.equal(then.getTime(), created.getTime());
                assert.equal(now.getTime(), modified.getTime());

                done();
            })
            .fail(function(err)
            {
                console.log(err);
                should.not.exist(err);
            })
            .done();
        });
    });

    describe('collections', function()
    {
        it('can create column family', function()
        {
            var promise = conn.cql(config['collections_create_cf#cql']);
            return promise.should.be.fulfilled;
        });

        it('can create a column family containing a set', function()
        {
            var promise = conn.cql(config['collections_create_cf2#cql']);
            return promise.should.be.fulfilled;
        });

        it('can update 1', function()
        {
            var promise = conn.cql(config['collections_update#cql'], config['collections_update#vals1']);
            return promise.should.be.fulfilled;
        });

        it('can update 2', function()
        {
            var promise = conn.cql(config['collections_update#cql'], config['collections_update#vals2']);
            return promise.should.be.fulfilled;
        });

        it('can update a column containing a set', function()
        {
            var promise = conn.cql(config['collections_update2#cql']);
            return promise.should.be.fulfilled;
        });

        it('can deserialize sets', function()
        {
            var promise = conn.cql(config['collections_select3#cql']);

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.then(function(rows)
                {
                    var eachRow = _.map(rows, function(row)
                    {
                        var name = row.get('name'),
                            lucky = row.get('lucky');

                        return _.pluck([name, lucky], 'value');
                    });

                    return eachRow;
                }).should.become([['Moe', [13, 27, 34]]])
            ]);
        });

        it('can insert into set<text> column', function()
        {
            var promise = conn.cql("INSERT INTO sets (name, actors) VALUES ('Moe', {'Moe Howard', 'Chris Diamantopoulos'})");
            return promise.should.be.fulfilled;
        });

        it('can select rows containing maps and lists', function()
        {
            var promise = conn.cql(config['collections_select1#cql']);

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.then(function(rows)
                {
                    var eachRow = _.map(rows, function(row)
                    {
                        var name = row.get('name'),
                            services = row.get('services'),
                            activities = row.get('activities');

                        return _.pluck([name, services, activities], 'value');
                    });

                    return eachRow;
                }).should.become([config['collections_update#vals1']])
            ]);
        });

        it('can select rows with sets that contain a different number of elements', function()
        {
            var promise = conn.cql(config['collections_select2#cql']);

            return P.all(
            [
                promise.should.be.fulfilled,
                promise.then(function(rows)
                {
                    var eachRow = _.map(rows, function(row)
                    {
                        var name = row.get('name'),
                            services = row.get('services'),
                            activities = row.get('activities');

                        return _.pluck([name, services, activities], 'value');
                    });

                    return eachRow;
                }).should.become([config['collections_update#vals2']])
            ]);
        });
    });

    describe('dropping keyspaces', function()
    {
        it('can drop keyspace', function()
        {
            var promise = conn.cql(config['drop_ks#cql']);
            return promise.should.be.fulfilled;
        });
    });

    after(function()
    {
        var deferred = P.defer();
        conn.on('close', deferred.resolve);
        conn.close();
        return deferred.promise.should.be.fulfilled;
    });

});
