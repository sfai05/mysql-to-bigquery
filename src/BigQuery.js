"use strict";

import {bigquery} from 'gcloud'
import fs from 'fs';
import Promsie from 'bluebird';

export default class BigQuery {

  constructor(options = {}) {

    this.bq = bigquery({
      projectId: options.projectId || process.env.BQ_PROJECT_ID,
      keyFilename: options.keyFilename || process.env.BQ_KEY_FILENAME
    });

    this.dataset = this.bq.dataset(options.dataset || process.env.BQ_DATASET);
  }

  insert(tableNames) {
    const promises = tableNames.map(n => this._import(n));

    return Promise.all(promises);
  }

  _import(tableName) {
    return new Promise((resolve, reject) => {
      this._createTable(tableName).then(_ => {
        return this._importFromFile(tableName);
      }).then(resolve, reject);
    });
  }

  _createTable(tableName) {
    return new Promise((resolve, reject) => {

      // TODO 追記が面倒なので一旦DELETE
      this.dataset.table(tableName).delete((error) => {
        if (error && error.code !== 404) {
          console.error(error);
          return reject(error);
        }

        fs.readFile(`./out/schema_${tableName}.json`, (error, text) => {
          const options = {
            schema: {
              fields: JSON.parse(text)
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

  _importFromFile(tableName) {
    return new Promise((resolve, reject) => {

      const table = this.dataset.table(tableName);
      const options = {
        fieldDelimiter: '\t',
        allowQuotedNewlines: true
      };

      fs.createReadStream(`./out/dump_${tableName}.tsv`)
        .pipe(table.createWriteStream(options))
        .on('error', reject)
        .on('complete', resolve);
    });
  }
}
