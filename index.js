var RDBtoBigQuery = require('./lib/RDBtoBigQuery');

module.exports = function(options) {

  this.mySqlConfig = options.mySqlConfig || {};
  this.bigQueryConfig = options.bigQueryConfig || {};

  var toBQ = new RDBtoBigQuery(sqlConfig, bqConfig);

  this.exec = function(tables, callback) {
    toBQ.exec(tables).then(function(result) {
      callback(null, 'ok');
    }).catch(function(error) {
      callback(error);
    });
  };
};
