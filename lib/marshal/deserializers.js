var _ = require('lodash');

var UUID = require('../uuid').UUID,
    TimeUUID = require('../uuid').TimeUUID;

/**
 * Deserializers for various types
 * @class
 */
var Deserializers = {};

var decodeLongFromBuffer = function(buf) {
    var negative = buf.readInt8(0) < 0;
    if(negative) {
      //TODO: replace this with a better negative reading function that does up to 53 bytes
      return buf.readInt32BE(4);
    } else {
      return parseInt(buf.toString('hex'), 16);
    }
};

Deserializers.decodeInteger = function(bits, val, signed){
  if(signed === null || signed === undefined){
    signed = false;
  }
  if(val === null || val === undefined){
    return null;
  }

  //this approach does not work above 32 bits (or perhaps 53)
  var hex = (Buffer.isBuffer(val) ? val : new Buffer(val, 'binary')).toString('hex');
  var x = parseInt(hex, 16);
  var max = Math.pow( 2, bits );
  return signed && x >= max / 2 ? x - max : x;
};

/**
 * Does binary decoding
 * @static
 * @param {String} val The binary string to decode
 * @returns {Buffer} a buffer containing the bytes decoded from the string
 */
Deserializers.decodeBinary = function(val){
  if(val === null || val === undefined){
    return null;
  }

  return Buffer.isBuffer(val) ? val : new Buffer(val, 'binary');
};

/**
 * Decodes a Long (Int64)
 * @static
 * @param {String} val The binary string to decode
 * @returns {Number} The number value decoded from the binary string
 */
Deserializers.decodeLong = function(val){
  if(val === null || val === undefined){
      return null;
  }

  var buf = Buffer.isBuffer(val) ? val : new Buffer(val, 'binary');
  return decodeLongFromBuffer(buf);
};

/**
 * Decodes a 32bit signed Integer
 * @param {String} val The binary string to decode
 * @returns {Number} The number value decoded from the binary string
 */
Deserializers.decodeInt32 = function(val){
  if(val === null || val === undefined){
    return null;
   }
   return (Buffer.isBuffer(val) ? val : new Buffer(val, 'binary')).readInt32BE(0, true);
};

/** 
 * Decodes a variable length signed integer
 * @param {String} val The binary string to decode
 * @returns {Number} The number value decoded from the binary string
 */
Deserializers.decodeVarInt  = function(val){
  if(val === null || val === undefined){
    return null;
  }
  var buf = Buffer.isBuffer(val) ? val : new Buffer(val, 'binary');
  switch(buf.length) {
    case 1:
      return buf.readInt8(0);
    case 2:
      return buf.readInt16BE(0);
    //3 is handled by default
    case 4:
      return buf.readInt32BE(0);
    case 8:
      return decodeLongFromBuffer(buf);
    default:
      return Deserializers.decodeInteger(buf.length*8, val, true);
  }
};

/**
 * Decodes for UTF8
 * @param {String} val The binary string to decode
 * @returns {String} The utf8 value decoded from the binary string
 */
Deserializers.decodeUTF8 = function(val){
  if(val === null || val === undefined){
    return null;
  }

  return (Buffer.isBuffer(val) ? val : new Buffer(val, 'binary')).toString('utf8');
};

/**
 * Decodes for Ascii
 * @param {String} val The binary string to decode
 * @returns {String} The ascii value decoded from the binary string
 */
Deserializers.decodeAscii = function(val){
  if(val === null || val === undefined){
    return null;
  }

  return (Buffer.isBuffer(val) ? val : new Buffer(val, 'binary')).toString('ascii');
};

/**
 * Decode a Double Precision Floating Point Type
 * @param {String} val The binary string to decode
 * @returns {Number} The number value decoded from the binary string
 */
Deserializers.decodeDouble = function(val){
  if(val === null || val === undefined){
    return null;
  }

  var buf = Buffer.isBuffer(val) ? val : new Buffer(val, 'binary');
  return buf.readDoubleBE(0);
};

/**
 * Decode a Single Precision Floating Point Type
 * @param {String} val The binary string to decode
 * @returns {Number} The number value decoded from the binary string
 */
Deserializers.decodeFloat = function(val){
  if(val === null || val === undefined){
    return null;
  }

  var buf = Buffer.isBuffer(val) ? val : new Buffer(val, 'binary');
  return buf.readFloatBE(0);
};

/**
 * Decodes a boolean type
 * @param {String} val The binary string to decode
 * @returns {Boolean} The boolean value decoded from the binary string
 */
Deserializers.decodeBoolean = function(val){
  if(val === null || val === undefined){
    return null;
  }

  var buf = Buffer.isBuffer(val) ? val : new Buffer(val, 'binary');

  if(buf[0] === 0){
    return false;
  } else if(buf[0] === 1) {
    return true;
  }
};

/**
 * Decodes a Date object
 * @param {String} val The binary string to decode
 * @returns {Date} The date value decoded from the binary string
 */
Deserializers.decodeDate = function(val){
  if(val === null || val === undefined){
    return null;
  }

  var date = Deserializers.decodeLong(val);
  return new Date(date);
};

/**
 * Decodes a UUID Object
 * @param {String} val The binary string to decode
 * @returns {UUID} The uuid value decoded from the binary string
 */
Deserializers.decodeUUID = function(val){
  if(val === null || val === undefined){
    return null;
  }

  return UUID.fromBinary(val);
};

/**
 * Decodes a TimeUUID Object
 * @param {String} val The binary string to decode
 * @returns {TimeUUID} The uuid value decoded from the binary string
 */
Deserializers.decodeTimeUUID = function(val){
  if(val === null || val === undefined){
    return null;
  }

  return TimeUUID.fromBinary(val);
};

Deserializers.decodeMap = function(val, types)
{
    var map = {},
        offset = 2,
        value = new Buffer(val, 'binary');

    for (;;)
    {
        var keyLength = value.readUInt16BE(offset),
            key = module.exports[types[0]](value.slice(offset += 2, offset += keyLength));

        var valueLength = value.readUInt16BE(offset),
            memberValue = module.exports[types[1]](value.slice(offset += 2, offset += valueLength));

        map[key] = memberValue;

        if (offset >= value.length)
            break;
    }

    return map;
};

Deserializers.decodeList = function(val, type)
{
    var list = [],
        offset = 2,
        value = new Buffer(val, 'binary');

    for (;;)
    {
        var elementLength = value.readUInt16BE(offset),
            element = value.slice(offset += 2, offset += elementLength);

        list.push(this[type].de(element));

        if (offset >= value.length)
            break;
    }

    return list;
};

function NOOP() {}

module.exports =
{
    'BytesType': Deserializers.decodeBinary,
    'LongType': Deserializers.decodeLong,
    'IntegerType': Deserializers.decodeVarInt,
    'Int32Type': Deserializers.decodeInt32,
    'UTF8Type': Deserializers.decodeUTF8,
    'AsciiType': Deserializers.decodeAscii,
    'LexicalUUIDType': Deserializers.decodeUUID,
    'TimeUUIDType': Deserializers.decodeTimeUUID,
    'CounterColumnType': Deserializers.decodeLong,
    'FloatType': Deserializers.decodeFloat,
    'DoubleType': Deserializers.decodeDouble,
    'DecimalType': Deserializers.decodeDouble,
    'DateType': Deserializers.decodeDate,
    'BooleanType': Deserializers.decodeBoolean,
    'UUIDType': Deserializers.decodeUUID,
    'CompositeType': NOOP,
    'ReversedType': _.identity,
    'MapType': Deserializers.decodeMap,
    'SetType': Deserializers.decodeSet,
    'ListType': Deserializers.decodeList
};
