/*global describe:true, it:true, before:true, after:true */

var
		chai = require('chai'),
		assert = chai.assert,
		expect = chai.expect,
		util = require('util'),
		scamandrios = require('../')
		;

chai.should();

describe('units', function()
{
	it('UUID successfully wraps node-uuid v4', function()
	{
		var v4 = new scamandrios.UUID('e59578a0-bf3f-47b4-bcf0-94f9279271cc');
		var v4buf = new Buffer([0xe5, 0x95, 0x78, 0xa0, 0xbf, 0x3f, 0x47, 0xb4, 0xbc, 0xf0, 0x94, 0xf9, 0x27, 0x92, 0x71, 0xcc]);

		v4.hex.should.equal('e59578a0-bf3f-47b4-bcf0-94f9279271cc');
		v4.toBinary().should.equal(v4buf.toString('binary'));
		v4.toString().should.equal('e59578a0-bf3f-47b4-bcf0-94f9279271cc');
	});

	it('also wraps uuid-v1', function()
	{
		var v1 = new scamandrios.TimeUUID.fromTimestamp(new Date(1326400762701)),
			v1buf = v1.toBuffer();

		v1.hex.should.equal(new scamandrios.TimeUUID.fromBinary(v1buf.toString('binary')).hex);
	});

});
