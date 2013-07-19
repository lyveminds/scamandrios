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

var Query = require('../lib/query');

describe('Query', function()
{
    it('constructor', function()
    {
        var query = new Query('INSERT INTO {family} ({c1}, {c2}, {c3}, {c4}) VALUES ({v1}, {v2}, {v3}, {v4})');

        query.params(
        {
            'family': 'accounts',

            'c1': 'name',
            'c2': 'email',
            'c3': 'password',
            'c4': 'id',

            'v1': 'Hermione Granger',
            'v2': 'hgranger@hogwarts.co.uk',
            'v3': '1sdfai129faskdjfls9',
            'v4': 'f7edeae4-3b40-4866-a07e-d74daf0dd56b'
        });

        query.types(
        {
            'v1': 'text',
            'v2': 'text',
            'v3': 'text',
            'v4': 'uuid'
        });

        String(query).should.equal("INSERT INTO accounts (name, email, password, id) VALUES ('Hermione Granger', 'hgranger@hogwarts.co.uk', '1sdfai129faskdjfls9', f7edeae4-3b40-4866-a07e-d74daf0dd56b)");
    });

    describe('#toString()', function()
    {
        it('can interpolate sets', function()
        {
            var setQuery = new Query('UPDATE {family} SET {c1} = {id}, {c2} = {emails}');
            setQuery.params(
            {
                'family': 'accounts',
                'c1': 'id',
                'c2': 'emails',
                'id': 'f7edeae4-3b40-4866-a07e-d74daf0dd56b',
                'emails': ['dmalfoy@hogwarts.co.uk', 'draco@malfoymanor.name']
            }).types(
            {
                'id': 'uuid',
                'emails': 'set<text>'
            });

            String(setQuery).should.equal("UPDATE accounts SET id = f7edeae4-3b40-4866-a07e-d74daf0dd56b, emails = {'dmalfoy@hogwarts.co.uk', 'draco@malfoymanor.name'}");
        });

        it('can interpolate lists', function()
        {
            var listQuery = new Query('INSERT INTO {family} ({c1}, {c2}) VALUES ({v1}, {v2})');
            listQuery.params(
            {
                'family': 'accounts',
                'c1': 'protagonists',
                'c2': 'antagonists',

                'v1': ['Harry', 'Hermione', 'Ron', 'Fred', 'George'],
                'v2': ['Voldemort', 'Bellatrix', 'Pettigrew', 'Malfoy']
            }).types(
            {
                'v1': 'list<text>',
                'v2': 'list<text>'
            });

            String(listQuery).should.equal("INSERT INTO accounts (protagonists, antagonists) VALUES (['Harry', 'Hermione', 'Ron', 'Fred', 'George'], ['Voldemort', 'Bellatrix', 'Pettigrew', 'Malfoy'])");
        });

        it('can interpolate maps', function()
        {
            var q = new Query('INSERT INTO {family} ({c1}, {c2}) VALUES ({v1}, {v2})');
            q.params(
            {
                'family': 'accounts',
                'c1': 'protagonists',
                'c2': 'antagonists',

                'v1': { 'Harry': true, 'Hermione': true, 'Ron': true, 'Fred': false, 'George': true, 'Lupin': false },
                'v2': { 'Voldemort': false, 'Bellatrix': false, 'Pettigrew': false, 'Malfoy': true }
            }).types(
            {
                'v1': 'map<text, boolean>',
                'v2': 'map<text, boolean>'
            });

            String(q).should.equal("INSERT INTO accounts (protagonists, antagonists) VALUES ({'Harry':true, 'Hermione':true, 'Ron':true, 'Fred':false, 'George':true, 'Lupin':false}, {'Voldemort':false, 'Bellatrix':false, 'Pettigrew':false, 'Malfoy':true})");
        });

        it('can interpolate strings', function()
        {
            var q = new Query('INSERT INTO {family} ({c1}, {c2}) VALUES ({v1}, {v2})');
            q.params(
            {
                'family': 'accounts',

                'c1': 'GRANT',
                'v1': "Alastor 'Mad-Eye' Moody",

                'c2': 'three',
                'v2': "'Fluffy'"
            }).types(
            {
                'v1': 'text',
                'v2': 'text'
            });

            String(q).should.equal("INSERT INTO accounts (\"GRANT\", \"three\") VALUES ('Alastor ''Mad-Eye'' Moody', '''Fluffy''')");
        });

        it('can interpolate dates', function()
        {
            var params = { family: 'accounts', c1: 'birthday', v1: new Date(Date.UTC(1988, 6, 31)) };
            var types = { v1: 'timestamp' };
            var raw = 'INSERT INTO {family} ({c1}) VALUES ({v1})';

            var q3 = new Query(raw).params(params).types(types);
            String(q3).should.equal('INSERT INTO accounts (birthday) VALUES (586310400000)');

            var q2 = new Query(raw, { version: '2.0.0' }).params(params).types(types);

            String(q2).should.equal("INSERT INTO accounts (birthday) VALUES ('1988-07-31T00:00:00.000Z')");
        });

        it('can interpolate numbers', function()
        {
            var q1 = new Query('INSERT INTO accounts ({c1}) VALUES ({v1})').params({ c1: 'trolls', v1: 2.5 }).types({ v1: 'double' });
            String(q1).should.equal('INSERT INTO accounts (trolls) VALUES (2.5)');

            var q1 = new Query('INSERT INTO accounts ({c1}) VALUES ({v1})').params({ c1: 'trolls', v1: 2 }).types({ v1: 'varint' });
            String(q1).should.equal('INSERT INTO accounts (trolls) VALUES (2)');

            var q1 = new Query('INSERT INTO accounts ({c1}) VALUES ({v1})').params({ c1: 'trolls', v1: NaN }).types({ v1: 'double' });
            String(q1).should.equal('INSERT INTO accounts (trolls) VALUES (NULL)');
        });

        it('can interpolate uuids', function()
        {
            var q1 = new Query('INSERT INTO accounts ({c1}) VALUES ({v1})').params({ c1: 'id', v1: 'f7edeae4-3b40-4866-a07e-d74daf0dd56b' }).types({ v1: 'uuid' });
            String(q1).should.equal('INSERT INTO accounts (id) VALUES (f7edeae4-3b40-4866-a07e-d74daf0dd56b)');

            var q2 = new Query('INSERT INTO accounts ({c1}) VALUES ({v1})').params({ c1: 'id', v1: 'b2e94590-f0c2-11e2-98cd-335ee480028e' }).types({ v1: 'uuid' });
            String(q2).should.equal('INSERT INTO accounts (id) VALUES (b2e94590-f0c2-11e2-98cd-335ee480028e)');
        });

        it('can interpolate timeuuids', function()
        {
            var q2 = new Query('INSERT INTO accounts ({c1}) VALUES ({v1})').params({ c1: 'id', v1: 'b2e94590-f0c2-11e2-98cd-335ee480028e' }).types({ v1: 'timeuuid' });
            String(q2).should.equal('INSERT INTO accounts (id) VALUES (b2e94590-f0c2-11e2-98cd-335ee480028e)');
        });

        it('can interpolate booleans', function()
        {
            var q2 = new Query('INSERT INTO accounts ({c1}, {c2}) VALUES ({v1}, {v2})').params({ c1: 'yes', v1: true, c2: 'no', v2: false }).types({ v1: 'boolean', v2: 'boolean' });
            String(q2).should.equal('INSERT INTO accounts (yes, no) VALUES (true, false)');
        });

        it('can interpolate buffers', function()
        {
            var q2 = new Query('INSERT INTO accounts ({c1}) VALUES ({v1})').params({ c1: 'document', v1: new Buffer('Hello!') }).types({ v1: 'blob' });
            String(q2).should.equal('INSERT INTO accounts (document) VALUES (48656c6c6f21)');
        });
    });
});
