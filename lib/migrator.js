var elasticsearch = require('elasticsearch')
    async         = require('async'),
    _             = require('underscore'),
    fs            = require('fs'),
    Indexer       = require('../lib/indexer'),
    URI           = require('URIjs'),
    os            = require('os');

function Migrator (options, logger, progress) {
  this.options = options;
  this.logger = logger || console;
  this.progress = progress;
};

Migrator.prototype.migrate = function() {
  var self = this;

  var from_uri      = new URI(self.options.from),
      to_uri     = new URI(self.options.to),
      from_client   = new elasticsearch.Client({host:from_uri.host(), requestTimeout:self.options.request_timeout}),
      to_client  = new elasticsearch.Client({host:to_uri.host(), requestTimeout:self.options.request_timeout}),
      from_path     = (function() { var tmp = from_uri.path().split('/'); return { index:tmp[1], type:tmp[2]}})(),
      to_path    = (function() { var tmp = to_uri.path().split('/'); return { index:tmp[1], type:tmp[2]}})(),
      total        = 0,  processed_total = 0,
      custom_indexer = self.options.custom_indexer ? require(fs.realpathSync(self.options.custom_indexer)) : null;
      scan_options = {
        index       : from_path.index,
        type        : from_path.type,
        search_type : 'scan',
        scroll      : self.options.scroll,
        size        : self.options.bulk
      };

  if (custom_indexer && custom_indexer.query) {
    scan_self.options.body = custom_indexer.query;
  }

  var reindexer = new Indexer();

  reindexer.on('warning', function(warning) {
    self.logger.warn(warning);
  });

  reindexer.on('error', function(error) {
    self.logger.error(error);
  });

  // reindexer.on('batch-complete', function(num_of_success) {
  //   processed_total += num_of_success;
  //   pace.op(processed_total);
  // });

  from_client.search(scan_options, function scroll_fetch(err, res) {
    if (err) {
      self.logger.fatal(err);
      return console.log("Scroll error:" + err);
    }
    var max_total = self.options.max_docs == -1 ? res.hits.total : self.options.max_docs;
    total += res.hits.hits.length;
    reindexer.index(res.hits.hits, {
      concurrency : self.options.concurrency,
      bulk        : self.options.bulk,
      client      : to_client,
      indexer     : custom_indexer ? custom_indexer.index : null,
      index       : to_path.index,
      type        : to_path.type
    }, function(err) {
      if (err) {
        self.logger.fatal(err);
        console.error("Reindex error: " + err);
        return
      }
      if (max_total !== total) {
        from_client.scroll({
          scrollId : res._scroll_id,
          scroll : self.options.scroll
        }, scroll_fetch);
      } else {
        console.log("Total " + total + " documents have been reindexed!");
        process.exit();
      }
    });
  });
};

module.exports = Migrator;