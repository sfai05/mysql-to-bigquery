"use strict";

import mysql from 'mysql';
import {bigquery} from 'gcloud'

import moment from 'moment';
import { map as esMap, readArray} from 'event-stream';
import { trimLeft } from 'lodash';

export default class MySQLtoBigQuery {

  constructor(config = {}) {
    const mySqlConfig = config.mySqlConfig || {};
    const bigQueryConfig = config.bigQueryConfig || {};

    // init MySQL
    this.connection = mysql.createConnection(mySqlConfig);

    this.connection.connect();

    // init BigQuery
    this.bq = bigquery(bigQueryConfig);

    this.dataset = this.bq.dataset(bigQueryConfig.dataset || process.env.BQ_DATASET);
  }

  exec(tableNames) {
    return Promise.all(tableNames.map(t => this._syncTable(t))).then(() => {
      this.connection.end()
    })
  }

  _syncTable(tableName) {
    return new Promise((resolve, reject) => {
      this._createTable(tableName).then(_ => {
        return this._insertRecords(tableName);
      }).then(_ => {
        resolve();
      }).catch(reject);
    });
  }

  _createTable(tableName) {
    return new Promise((resolve, reject) => {
      this.connection.query(`DESC ${tableName};`, (error, rows, fields) => {
        if (error) return reject(error);

        const json = rows.map(row => Object({name: row.Field, type: this._convertToBqColumnType(row.Type)}));

        // MEMO 一旦削除
        this.dataset.table(tableName).delete((error) => {
          if (error && error.code !== 404) {
            console.error(error);
            return reject(error);
          }

          const options = {
            schema: {
              fields: json
            }
          };

          this.dataset.createTable(tableName, options, (error) => {
            if (error) return reject(error);
            resolve();
          });
        });
      });
    });
  }

  _insertRecords(tableName) {
    return new Promise((resolve, reject) => {
      const table = this.dataset.table(tableName);

      this.connection.query(`SELECT * FROM ${tableName};`, (error, results) => {
        if (error) return reject(error);

        readArray(results.map(data => this._toJSON(data)))
        .pipe(table.createWriteStream('json'))
        .on('error', function(error){
          console.error(error)
          reject();
        })
        .on('complete', resolve);
      });
    });
  }

  _toJSON(data) {
    return JSON.stringify(data) + '\n';
  }

  _escape(val) {
    if (val === undefined || val === null) {
      return '""';
    }
    if (typeof val === 'string') {
      return `"${val.replace(/"/g, '&quot').replace(/\n/g, '\\n')}"`;
    }
    else if (val instanceof Date) {
      return `"${moment(val).format('YYYY-MM-DD HH:mm:ss')}"`;
    }

    return val;
  }

  _convertToBqColumnType(columnType) {
    const type = columnType.toLowerCase();
    if (type === 'tinyint(1)') {
      return 'BOOLEAN'
    }
    else if (type.match(/^int\([0-9]+\)$/)) {
      return 'INTEGER'
    }
    else if (type === 'datetime') {
      return 'TIMESTAMP'
    }
    else if (type === 'float' || type === 'double') {
      return 'FLOAT';
    }

    return 'STRING';
  }
}
