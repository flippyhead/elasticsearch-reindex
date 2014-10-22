var async         = require('async'),
    _             = require('underscore'),
    EventEmitter  = require('events').EventEmitter,
    inherits      = require('util').inherits;

function Indexer () {
  EventEmitter.call(this);
};

inherits(Indexer, EventEmitter);

Indexer.prototype.index = function(docs, options, cb) {
  if (!docs) {
    return;
  }
  var self = this;

  options.indexer = options.indexer || function(item, options) {
    return [
      {index:{_index:options.index || item._index, _type:options.type || item._type, _id: item._id}},
      item._source
    ];
  };
  var chunks = _.toArray(_.groupBy(docs, function(item, index){ return Math.floor(index/options.bulk); }));
  async.eachLimit(chunks, options.concurrency, function(chunk, cb) {
    var bulk_data = [];
    chunk.forEach(function(item) {
      bulk_data = bulk_data.concat(options.indexer(item, options));
    });
    options.client.bulk({
      body: bulk_data
    }, function(err, res) {
      if (err) {
        self.emit('error', err);
      }
      if (res.errors) {
        res.items.forEach(function(item) {
          if (_.indexOf([200, 201], item.index.status) != -1) {
            self.emit('batch-complete', 1);
          } else {
            self.emit('warning', item);
          }
        });
      } else {
        self.emit('batch-complete', chunk.length);
      }
      cb(err);
    });
  }, function(err) {
    cb(err);
  });
};

module.exports = Indexer;
