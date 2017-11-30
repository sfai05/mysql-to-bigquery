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

## Tests
ToDo

## License
MIT
