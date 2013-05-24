/*global describe:true, it:true, before:true, after:true */

var
    chai = require('chai'),
    assert = chai.assert,
    expect = chai.expect,
    chaiAsPromised = require('chai-as-promised'),
    util = require('util'),
    scamandrios = require('../'),
    P = require('p-promise'),
    _ = require('lodash')
    ;

require('mocha-as-promised')();
chai.should();
chai.use(chaiAsPromised);

describe('thrift', function()
{
    // ...
});