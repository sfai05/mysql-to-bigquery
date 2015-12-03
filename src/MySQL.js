"use strict";

import mysql from 'mysql';
import Promise from 'bluebird';
import moment from 'moment';
import fs from 'fs';
import {mapSync} from 'event-stream';
import {reduce, trimLeft} from 'lodash';

export default class MySQL {

  constructor(options = {}) {
    this.connection = mysql.createConnection({
      host: options.host || process.env.RDS_HOST,
      user: options.user || process.env.RDS_USER,
      password: options.password || process.env.RDS_PASSWORD,
      database: options.database || process.env.RDS_DATABASE
    });

    this.connection.connect();
  }

  generateFilesForBq(tableNames) {
    const promises = tableNames.map(n => this._dump(n));

    return Promise.all(promises);
  }

  _dump(tableName) {
    return new Promise((resolve, reject) => {
      this._dumpTable(tableName).then(_ => {
        return this._dumpSchema(tableName);
      }).then(resolve, reject);
    });
  }

  _dumpTable(tableName) {
    return new Promise((resolve, reject) => {
      const fileStream = fs.createWriteStream(`./out/dump_${tableName}.tsv`);

      this.connection.query(`SELECT * FROM ${tableName};`).stream()
        .pipe(mapSync(data => this._toTSV(data)))
        .pipe(fileStream);

      fileStream.on('error', reject).on('close', resolve);
    });
  }

  _toTSV(data) {
    return trimLeft(reduce(data, (result, val) => `${result}\t${this._escape(val)}`, '') + '\n');
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

  _dumpSchema(tableName) {
    return new Promise((resolve, reject) => {
      this.connection.query(`DESC ${tableName};`, (error, rows, fields) => {
        if (error) return reject(error);

        const json = rows.map(row => Object({name: row.Field, type: this._convertToBqColumnType(row.Type)}));

        fs.writeFile(`./out/schema_${tableName}.json`, JSON.stringify(json), (error) => {
          if (error) return reject(error);

          resolve();
        });
      });
    });
  }

  _convertToBqColumnType(columnType) {
    const type = columnType.toLowerCase();
    if (type === 'tinyint(1)') {
      return 'BOOLEAN'
    }
    else if (type.indexOf('int') !== -1) {
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
