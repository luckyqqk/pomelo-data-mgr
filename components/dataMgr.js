var format = require('util').format;
var GameDataMgr = require('game-data-mgr');

module.exports = function(app, opts) {
    var dataMgr = new pomeloDataMgr(app, opts);
    app.set('dataMgr', dataMgr.dataMgr, true);
    return dataMgr;
};

var pomeloDataMgr = function(app, opts) {
    this.mysql = app.get('mysql');
    if (!this.mysql) {
        console.error(`${app.getServerId()} has no mysqlClient`);
        return;
    }
    this.redis = app.get('redis');
    if (!this.redis) {
        console.error(`${app.getServerId()} has no redisClient`);
        return;
    }
    this.tableReference = require(app.getBase() + opts.tableReference);
    this.databaseSign = format('%s:%s', opts.dbHost.substring(opts.dbHost.lastIndexOf('.') + 1), opts.database);
    this.dataMgr = new GameDataMgr(this.mysql, this.redis, this.tableReference, this.databaseSign, opts.database);
};

pomeloDataMgr.prototype.start = function(cb) {
    this.dataMgr.loadTableStructure((err)=>{
        if (!!err)
            console.error(err);
        cb();
    });
};

pomeloDataMgr.prototype.stop = function(cb) {
    this.dataMgr.destroy();
    if (!!this.mysql)
        this.mysql.end();
    if (!!this.redis)
        this.redis.end();
    cb();
};
