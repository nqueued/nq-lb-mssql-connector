const sql = require('mssql'),
moment = require('moment'),
Connector = require('loopback-connector').SqlConnector,
ParameterizedSQL = require('loopback-connector').ParameterizedSQL;

module.exports.initialize = function initializeDataSource(dataSource, callback) {
  dataSource.connector = new CHSDMConnector(dataSource.settings);
  dataSource.dataSource = dataSource;
  if (callback) {
	  dataSource.connector.connect(callback);
  }
};

function CHSDMConnector(settings) { 
  Connector.call(this, settings.name, settings);
  this.name = settings.name;
  this.version = settings.version;
  this.verbose = settings.verbose || false;
  this.conn = new sql.Connection(settings);
}

require('util').inherits(CHSDMConnector, Connector);

CHSDMConnector.prototype.connect = function(callback) {
  this.conn.connect(callback || function() {});
}

CHSDMConnector.prototype.escapeName = function(name) {
  return name;
};

CHSDMConnector.prototype.escapeValue = function(value) {
  return value;
};

CHSDMConnector.prototype.getColumn2ModelMapping = function(cols) {
  let m = {};
  for (let c in cols) {
    let obj = cols[c][this.name];
    if (obj && obj.columnName) {
      m[obj.columnName] = c;
    }
  }
  return m;  
};

CHSDMConnector.prototype.getModel2ColumnMapping = function(cols) {
  let m = {};
  for (let c in cols) {
    let obj = cols[c][this.name];
    if (obj && obj.columnName) {
      m[c] = obj.columnName;
    }
  }
  return m;
};

CHSDMConnector.prototype.getModelId = function(cols) {
  for (let c in cols) {
    if (cols[c] && cols[c].id) {
      return { name: c, columnName: cols[c][this.name].columnName};
    }
  }
  return null;
};

CHSDMConnector.prototype.mapColumns = function(result, map) {
  let res = {};
  for (let r in result) {
    res[map[r] || r] = result[r];
  }
  return res;
};


CHSDMConnector.prototype.alls = function (model, filter, callback) {
  let map = this.getColumn2ModelMapping(this.dataSource.definitions[model].properties);
  let query = `select * from ${this.table(model)}`;
  let id = this.idName(model);
	if (filter && filter.where && id && filter.where[id]) { 
    query += ` where ${this.idColumn(model)} = '${filter.where[id]}'`;
	}
  this.executeSQL(query, null, null, (err, recordset) => {
    if (err) {
      return callback(err);
    }
    recordset = recordset.map((rs) => {
      return this.mapColumns(rs, map);
    });
    callback(err, recordset);
  });  
};

CHSDMConnector.prototype.create = function(model, data, options, callback) {
  var self = this;
  var stmt = this.buildInsert(model, data, options);
  options = options || {};
  options.isCreate = true;
  this.execute(stmt.sql, stmt.params, options, function(err, info) {
    if (err) {
      callback(err);
    } else {
      var insertedId = self.getInsertedId(model, info[0]);
      callback(err, insertedId);
    }
  });
};

CHSDMConnector.prototype.buildParams = function(model, data, excludeIds) {
  var keys = Object.keys(data);
  return this._buildParamsForKeys(model, data, keys, excludeIds);
}

CHSDMConnector.prototype._buildParamsForKeys = function(model, data, keys, excludeIds) {
  const props = this.getModelDefinition(model).properties;
  let params = [];

  for (var i = 0, n = keys.length; i < n; i++) {
    const key = keys[i];
    const p = props[key];
    if (p == null) {
      // Unknown property, ignore it
      //debug('Unknown property %s is skipped for model %s', key, model);
      continue;
    }

    if (excludeIds && p.id) {
      continue;
    }
    let k = this.columnEscaped(model, key);
    let v = this.toColumnValue(p, data[key]);
    if (v !== undefined) {
      params.push({value: v, property: p[this.name]});
    }
  }
  return params;
};

CHSDMConnector.prototype.getFieldDataType = function(prop) {
  prop = prop || {dataType: 'varchar'};
  switch(prop.dataType) {
    case 'int':
      return sql.Int;
    case 'smallint':
      return sql.SmallInt;
    case 'tinyint':
      return sql.TinyInt;
    case 'bigint':
      return sql.BigInt;
    case 'decimal':
      return sql.Decimal(prop.dataPrecision || 18, prop.dataScale || 0);
    case 'varchar':
      return sql.VarChar(prop.dataLength || 10000);
    case 'datetime':
      return sql.DateTime;
    default:
      return sql.Text;
  }
}

function getQueryType(query) {
  return query.split(' ').shift().toLowerCase();
}

CHSDMConnector.prototype.executeSQL = function (query, params, options, callback) {
  let queryType = getQueryType(query);
  if (queryType === 'insert') {
    query = query + ';select @@IDENTITY AS \'insertedId\'';
  }
  let trans = new sql.Transaction(this.conn);
  trans.begin().then(() => {
    let req = new sql.Request(trans);
    req.verbose = this.verbose;
    if (params) {
      params.forEach((p, i) => {
        if (p && p.value) {
          req.input(`${i+1}`, this.getFieldDataType(p.property), p.value);
        } else {
          req.input(`${i+1}`, sql.VarChar, p);
        }
      });
    }
    return req.query(query).then((recordset) => {
      trans.commit().then(() => {
        if (options.map && recordset) {
          let res = recordset.map((m) => { 
            let obj = {};
            Object.keys(m).forEach((k) => {
              obj[options.map[k] || k.toLowerCase()] = m[k];
            });
            return obj;
          });
          callback(null, res);
        } else {
          callback(null, recordset || {affectedRows: req.rowsAffected});
        }
      }); 
    });
  }).catch((err) => {
    trans.rollback();
    callback(err);
  });
};

CHSDMConnector.prototype.executeProc = function (proc, params, map, callback) {
  let req = new sql.Request(this.conn);
  req.verbose = this.verbose;
  if (typeof map === 'function') {
    callback = map;
    map = null;
  }
  if (params) {
    params.forEach((p) => {
      if (p.type && p.type === 'out') {
        req.output(`${p.id}`, this.getFieldDataType(p.property));
        //console.log('\t <- %s', p.id);
      } else {
        req.input(`${p.id}`, this.getFieldDataType(p.property), p.value);
        //console.log('\t -> %s, %s', p.id, p.value);          
      }
    });
  }
  req.execute(proc).then((recordset) => {
    //console.log('Exec Proc Returned: %j, %s', recordset, req.rowsAffected);
    if (map && recordset) {
      let res = recordset.map((r) => { return r.map((m) => {
        let obj = {};
        Object.keys(m).forEach((k) => {
          obj[map[k] || k.toLowerCase()] = m[k];
        });
        return obj;
      });
      });
      callback(null, res);
    } else {
      callback(null, recordset || {affectedRows: req.rowsAffected});
    }
  }).catch((err) => {
    callback(err);
  });
};

CHSDMConnector.prototype.ping = function (callback) {
  callback(null);
};

CHSDMConnector.prototype.toColumnValue = function(prop, val) {
  if (val === null || val === undefined) {
    if (prop.autoIncrement || prop.id) {
      return new ParameterizedSQL('DEFAULT');
    }
    return null;
  }
  if (!prop) {
    return val;
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      return val;
    }
    return val;
  }
  if (prop.type === Date) {
    val = new moment.utc(val).format('Y-MM-DD HH:mm:ss.SSS');
    return val;
  }
  if (prop.type === Boolean) {
    return !!val ? 1 : 0;
  }
  if (prop.type === Object) {
    return this._serializeObject(val);
  }
  if (typeof prop.type === 'function') {
    return this._serializeObject(val);
  }
  return this._serializeObject(val);
};

CHSDMConnector.prototype.fromColumnValue = function(prop, val) {
  if (val === null || val === undefined) {
    return val;
  }
  if (prop) {
    switch (prop.type.name) {
      case 'Number':
        val = Number(val);
        break;
      case 'String':
        val = String(val);
        break;
      case 'Date':
        if (val === '0000-00-00 00:00:00') {
          val = null;
        } else {
          val = new Date(val.toString());
        }
        break;
      case 'Boolean':
        val = Boolean(val);
        break;
      case 'GeoPoint':
      case 'Point':
        val = {
          lat: val.x,
          lng: val.y,
        };
        break;
      case 'List':
      case 'Array':
      case 'Object':
      case 'JSON':
        if (typeof val === 'string') {
          val = JSON.parse(val);
        }
        break;
      default:
        if (!Array.isArray(prop.type) && !prop.type.modelName) {
          // Do not convert array and model types
          val = prop.type(val);
        }
        break;
    }
  }
  return val;
};
CHSDMConnector.prototype._buildLimit = function(model, stmt, limit, offset) {
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  let sqlParts = stmt.sql.split(' ');
  sqlParts[1] += `,ROW_NUMBER() OVER (ORDER BY ${this.idColumn(model)}) as row`;
  // remove the order by to the end
  let joined = [], orderBy = '';
  for (let i = 0; i < sqlParts.length; ++i) {
    if (sqlParts[i].length === 5 && sqlParts[i].toUpperCase() === 'ORDER') {
      orderBy = `${sqlParts[i]} ${sqlParts[i+1]} ${sqlParts[i+2]}`;
      i += 2;
    } else if (sqlParts[i].toUpperCase() === 'DESC' || sqlParts[i].toUpperCase() === 'ASC') {
      orderBy += ` ${sqlParts[i]}`;
    } else {
      joined.push(sqlParts[i]);
    }
  }
  return `SELECT * FROM (${joined.join(' ')}) a WHERE a.row > ${offset} and a.row <= ${offset+limit} ${orderBy}`;
};

CHSDMConnector.prototype.applyPagination = function(model, stmt, filter) {
  let limitClause = this._buildLimit(model, stmt, filter.limit, filter.offset || filter.skip);
  // implement the row offset
  if (limitClause) {
    stmt.sql = limitClause;
    return stmt;
  }
  return stmt.merge(limitClause);
};

CHSDMConnector.prototype.getPlaceholderForIdentifier = function(key) {
  return `@${key}`;
};

CHSDMConnector.prototype.getPlaceholderForValue = function(key) {
  return `@${key}`;
};

CHSDMConnector.prototype._serializeObject = function(obj) {
  let val;
  if (obj && typeof obj.toJSON === 'function') {
    obj = obj.toJSON();
  }
  if (typeof obj !== 'string') {
    val = JSON.stringify(obj);
  } else {
    val = obj;
  }
  return val;
};

CHSDMConnector.prototype.discoverModelProperties = function (objectName, options, callback) {
  // not yet implemented
  callback(null, []);
};


CHSDMConnector.prototype.discoverSchemas = function (objectName, options, callback) {
  this.discoverModelProperties(objectName, options, (error, response) => {
    if (error) {
      return callback && callback(error);
    }
    let schema = {
      name: objectName,
      options: {
        idInjection: true, // false - to remove id property
        sObjectName: objectName
      },
      properties: {}
    };
 
    if (response || response.length !== 0) {
      response.forEach(function (field) {
        let fieldProperties = {};
        Object.keys(field).forEach(function (fieldProperty) {
          fieldProperties[fieldProperty] = field[fieldProperty];
        });
        schema.properties[field["name"]] = fieldProperties;
      });
    }
    options.visited = options.visited || {};
    if (!options.visited.hasOwnProperty(objectName)) {
      options.visited[objectName] = schema;
    }
    return callback && callback(null, options.visited);
  });
}

CHSDMConnector.prototype.getInsertedId = function(model, info) {
  let insertedId;
  if (Array.isArray(info)) {
    insertedId = info.reduce((p, n) => {
      return n.insertedId || p;
    }, undefined);
  } else {
  insertedId = info && typeof info.insertedId === 'number' ?
    info.insertedId : undefined;
  }
  return insertedId;
};

CHSDMConnector.prototype.getCountForAffectedRows = function(model, info) {
  return info && typeof info.affectedRows === 'number' ?
    info.affectedRows : 0;
};