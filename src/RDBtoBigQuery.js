"use strict";

import MySQL from './MySQL';
import BigQuery from './BigQuery';

export default class RDBtoBigQuery {

  constructor(sqlConfig, bqConfig) {
    this.mySQL = new MySQL(sqlConfig);
    this.bq = new BigQuery(bqConfig);
  }

  exec(tableNames) {
    return new Promise((resolve, reject) => {
      this.mySQL.generateFilesForBq(tableNames).then(_ => {
        return this.bq.insert(tableNames);
      }).then(_ => {
        resolve();
      }).catch(reject);
    });
  }
}
