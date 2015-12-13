# mysql-to-bigquery
Data Transfer from MySQL to BigQuery

## Install

```
$ npm i mysql-to-bigquery --save
```

## Usage

```
var MySqlToBigQuery = require('mysql-to-bigquery');
var sqlToBq = new MySqlToBigQuery({
  mySqlConfig: {
    host: "localhost",
    user: "root",
    password: "password",
    database: "yourdatabase"
  },
  bigQueryConfig: {
    projectId: "xxxxxx",
    keyFilename: "/path/to/keyfile",
    dataset: "yourdataset"
  }
});

sqlToBq.exec(['users'], function(error) {

});
```

OR set following value instead of `mySqlConfig` and `bigQueryConfig`

```
export RDS_HOST=xxxxxxxx
export RDS_USER=xxxxxxxx
export RDS_PASSWORD=xxxxxxxx
export RDS_DATABASE=xxxxxxxx
export BQ_PROJECT_ID=xxxxxxxx
export BQ_KEY_FILENAME=xxxxxxxx
export BQ_DATASET=xxxxxxxx
```

## Tests
ToDo

## License
MIT
