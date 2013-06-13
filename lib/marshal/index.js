var _ = require('lodash');

var Serializers = require('./serializers'),
    Deserializers = require('./deserializers');
/**
 * Given a string like org.apache.cassandra.db.marshal.UTF8Type
 * return a string of UTF8Type
 * @return {string} TypeName
 * @private
 * @memberOf Marshal
 */
function getType(str){
  var classes = str.split('.');
  return classes[classes.length - 1];
}

function getMapType(string)
{
    var mapTypes = string.slice(string.indexOf('(') + 1, -1).split(','),
        type = _.map(mapTypes, getType);

    return { 'name': 'MapType', 'type': type, 'isCollection': true };
}

function getSetOrList()
{
    return function getCollectionType(typeName, string)
    {
        var listType = string.slice(string.indexOf('(') + 1, -1),
            type = getType(listType);

        return { 'name': typeName, 'type': type, 'isCollection': true };
    };
}

var getSetType = _.partial(getSetOrList, 'SetType'),
    getListType = _.partial(getSetOrList, 'ListType');

/**
 * Returns the type string inside of the parentheses
 * @private
 * @memberOf Marshal
 */
function getInnerType(str){
  var index = str.indexOf('(');
  return index > 0 ? str.substring(index + 1, str.length - 1) : str;
}

/**
 * Returns an array of types for composite columns
 * @private
 * @memberOf Marshal
 */
function getCompositeTypes(str){
  var type = getInnerType(str);
  if (type === str) {
    return getType(str);
  }

  var types = type.split(','),
      i = 0, ret = [], typeLength = types.length;

  for(; i < typeLength; i += 1){
    ret.push( parseTypeString(types[i]) );
  }

  return ret;
}

/**
 * Parses the type string and decides what types to return
 * @private
 * @memberOf Marshal
 */
function parseTypeString(type){
  if (type.indexOf('CompositeType') > -1){
    return getCompositeTypes(type);
  } else if(type.indexOf('ReversedType') > -1){
    return getType(getInnerType(type));
  }
  else if(type.indexOf('org.apache.cassandra.db.marshal.SetType') > -1){
    return getSetType(type);
  }
  else if(type.indexOf('org.apache.cassandra.db.marshal.ListType') > -1){
    return getListType(type);
  }
  else if(type.indexOf('org.apache.cassandra.db.marshal.MapType') > -1){
    return getMapType(type);
  }
  else if(type === null || type === undefined) {
    return 'BytesType';
  } else {
    return getType(type);
  }
}

/**
 * Creates a serializer for composite types
 * @private
 * @memberOf Marshal
 */
function compositeSerializer(serializers){
  return function(vals, sliceStart){
    var i = 0, buffers = [], totalLength = 0,
        valLength = vals.length, val;

    if(!Array.isArray(vals)){
      vals = [vals];
      valLength = vals.length;
    }

    for(; i < valLength; i += 1){
      if (Array.isArray(vals[i])){
        val = [serializers[i](vals[i][0]), vals[i][1]];
        totalLength += val[0].length + 3;
      } else {
        val = serializers[i](vals[i]);
        totalLength += val.length + 3;
      }

      buffers.push(val);
    }

    var buf = new Buffer(totalLength),
        buffersLength = buffers.length,
        writtenLength = 0, eoc, inclusive;

    i = 0;
    for(; i < buffersLength; i += 1){
      val = buffers[i];
      eoc = new Buffer('00', 'hex');
      inclusive = true;

      if (Array.isArray(val)){
        inclusive = val[1];
        val = val[0];
        if(inclusive){
          if (sliceStart){
            eoc = new Buffer('ff', 'hex');
          } else if (sliceStart === false){
            eoc = new Buffer('01', 'hex');
          }
        } else {
          if (sliceStart){
            eoc = new Buffer('01', 'hex');
          } else if (sliceStart === false){
            eoc = new Buffer('ff', 'hex');
          }
        }
      } else if (i === buffersLength - 1){
      if (sliceStart){
          eoc = new Buffer('ff', 'hex');
        } else if (sliceStart === false){
          eoc = new Buffer('01', 'hex');
        }
      }

      buf.writeUInt16BE(val.length, writtenLength);
      writtenLength += 2;
      val.copy(buf, writtenLength, 0);
      writtenLength += val.length;
      eoc.copy(buf, writtenLength, 0);
      writtenLength += 1;
    }

    return buf;
  };
}

/**
 * Creates a deserializer for composite types
 * @private
 * @memberOf Marshal
 */
function compositeDeserializer(deserializers){
  return function(str){
    var buf = new Buffer(str, 'binary'),
        pos = 0, len, vals = [], i = 0;

     while( pos < buf.length){
       len = buf.readUInt16BE(pos);
       pos += 2;
       vals.push(deserializers[i](buf.slice(pos, len + pos)));
       i += 1;
       pos += len + 1;
     }

    return vals;
  };
}

/**
 * Gets the serializer(s) for a specific type
 * @private
 * @memberOf Marshal
 */
function getSerializer(type)
{
    if (Array.isArray(type))
        return compositeSerializer(_.map(type, getSerializer));

    return Serializers[type.replace(/^\s+|\s+$/g,'')];
}

/**
 * Gets the deserializer(s) for a specific type
 * @private
 * @memberOf Marshal
 */
function getDeserializer(type, subtype)
{
    if (Array.isArray(type))
        return compositeDeserializer(_.map(type, function(value) { return getDeserializer(value, subtype); }));

    return function deserializer(val)
    {
        return val !== null ? Deserializers[type](val, subtype) : null;
    };
}

/**
 * Creates a Serialization/Deserialization object for a column family
 * @constructor
 * @param {String} type The type to create the marshaller for (eg. 'BytesType' or 'org.apache...CompositeType(BytesType,UTF8Type))
 */
var Marshal = function Marshal(type)
{
    var parsedType = parseTypeString(type);

    if (parsedType.isCollection)
    {
        this.type = parsedType.type;
        parsedType = parsedType.name;
        this.isComposite = false;
    }
    else
    {
        this.type = parsedType;
        this.isComposite = Array.isArray(parsedType);
    }

    this.serialize = getSerializer(parsedType);
    this.deserialize = getDeserializer(parsedType, this.type);
};

module.exports = Marshal;
