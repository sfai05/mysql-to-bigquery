"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _mysql = require('mysql');

var _mysql2 = _interopRequireDefault(_mysql);

var _gcloud = require('gcloud');

var _moment = require('moment');

var _moment2 = _interopRequireDefault(_moment);

var _eventStream = require('event-stream');

var _lodash = require('lodash');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var MySQLtoBigQuery = function () {
  function MySQLtoBigQuery() {
    var config = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};

    _classCallCheck(this, MySQLtoBigQuery);

    var mySqlConfig = config.mySqlConfig || {};
    var bigQueryConfig = config.bigQueryConfig || {};

    // init MySQL
    this.connection = _mysql2.default.createConnection(mySqlConfig);

    this.connection.connect();

    // init BigQuery
    this.bq = (0, _gcloud.bigquery)(bigQueryConfig);

    this.dataset = this.bq.dataset(bigQueryConfig.dataset || process.env.BQ_DATASET);
  }

  _createClass(MySQLtoBigQuery, [{
    key: 'exec',
    value: function exec(tableNames) {
      var _this = this;

      return Promise.all(tableNames.map(function (t) {
        return _this._syncTable(t);
      })).then(function () {
        _this.connection.end();
      });
    }
  }, {
    key: '_syncTable',
    value: function _syncTable(tableName) {
      var _this2 = this;

      return new Promise(function (resolve, reject) {
        _this2._createTable(tableName).then(function (_) {
          return _this2._insertRecords(tableName);
        }).then(function (_) {
          resolve();
        }).catch(reject);
      });
    }
  }, {
    key: '_createTable',
    value: function _createTable(tableName) {
      var _this3 = this;

      return new Promise(function (resolve, reject) {
        _this3.connection.query('DESC ' + tableName + ';', function (error, rows, fields) {
          if (error) return reject(error);

          var json = rows.map(function (row) {
            return Object({ name: row.Field, type: _this3._convertToBqColumnType(row.Type) });
          });

          // MEMO 一旦削除
          _this3.dataset.table(tableName).delete(function (error) {
            if (error && error.code !== 404) {
              console.error(error);
              return reject(error);
            }

            var options = {
              schema: {
                fields: json
              }
            };

            _this3.dataset.createTable(tableName, options, function (error) {
              if (error) return reject(error);
              resolve();
            });
          });
        });
      });
    }
  }, {
    key: '_insertRecords',
    value: function _insertRecords(tableName) {
      var _this4 = this;

      return new Promise(function (resolve, reject) {
        var table = _this4.dataset.table(tableName);

        _this4.connection.query('SELECT * FROM ' + tableName + ';', function (error, results) {
          if (error) return reject(error);

          (0, _eventStream.readArray)(results.map(function (data) {
            return _this4._toJSON(data);
          })).pipe(table.createWriteStream('json')).on('error', function (error) {
            console.error(error);
            reject();
          }).on('complete', resolve);
        });
      });
    }
  }, {
    key: '_toJSON',
    value: function _toJSON(data) {
      return JSON.stringify(data) + '\n';
    }
  }, {
    key: '_escape',
    value: function _escape(val) {
      if (val === undefined || val === null) {
        return '""';
      }
      if (typeof val === 'string') {
        return '"' + val.replace(/"/g, '&quot').replace(/\n/g, '\\n') + '"';
      } else if (val instanceof Date) {
        return '"' + (0, _moment2.default)(val).format('YYYY-MM-DD HH:mm:ss') + '"';
      }

      return val;
    }
  }, {
    key: '_convertToBqColumnType',
    value: function _convertToBqColumnType(columnType) {
      var type = columnType.toLowerCase();
      if (type === 'tinyint(1)') {
        return 'BOOLEAN';
      } else if (type.match(/^int\([0-9]+\)$/)) {
        return 'INTEGER';
      } else if (type === 'datetime') {
        return 'TIMESTAMP';
      } else if (type === 'float' || type === 'double') {
        return 'FLOAT';
      }

      return 'STRING';
    }
  }]);

  return MySQLtoBigQuery;
}();

exports.default = MySQLtoBigQuery;