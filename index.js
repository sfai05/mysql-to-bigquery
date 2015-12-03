var RDBtoBigQuery = require('./lib/RDBtoBigQuery');

module.exports = function(options) {

  var toBQ = new RDBtoBigQuery(options);

  this.exec = function(tables, callback) {
    toBQ.exec(tables).then(function(result) {
      callback(null, 'ok');
    }).catch(function(error) {
      callback(error);
    });
  };
};
