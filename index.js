const sqlite = require('sqlite');
const CronJob = require('cron').CronJob;
const timeZone = 'Asia/Shanghai';

var kvs = function () {
  this.db = null;
};
// open database (instantiate db object)
kvs.prototype.open = async function (dbpath) {
  var db = new kvs();
  await db._open(dbpath);
  return db;
};

kvs.prototype.close = function () {
  if (this.db) this.db.close();
};

kvs.prototype._open = async function (dbpath) {
  var self = this;
  var db = self.db = await sqlite.open(dbpath);
  await db.run(
    'CREATE TABLE IF NOT EXISTS items(' +
    ' key   TEXT PRIMARY KEY,' +
    ' value TEXT,' +
    ' ctime INTEGER,' +
    ' expire INTEGER)');
  self.stmt_get = await db.prepare(
    'SELECT * FROM items WHERE key=? LIMIT 1'
  );
  self.stmt_insert = await db.prepare(
    'INSERT INTO items (key,value,ctime,expire) VALUES (?,?,?,?)'
  );
  self.stmt_delete = await db.prepare(
    'DELETE FROM items WHERE key = ?'
  );
  self.stmt_delete_expired = await db.prepare(
    'DELETE FROM items WHERE expire > 0 AND expire < ?'
  );
};

kvs.prototype.get = async function (key) {
  var self = this;
  const result = await self.stmt_get.get([key]);
  if (result) {
    var t = (new Date()).getTime();
    if (result.expire > 0) {
      if (t > result.expire) {
        // expired;
        await self.delete(key);
        return null;
      }
    }
    return result.value;
  }
};

kvs.prototype.put = async function (key, value, expireSeconds = 0) {
  var t = (new Date()).getTime();
  await this.delete(key);
  const expireTime = expireSeconds ? (expireSeconds * 1000 + t) : 0;
  await this.stmt_insert.run([key, value, t, expireTime]);
};

kvs.prototype.delete = async function (key) {
  await this.stmt_delete.run([key]);
};

kvs.prototype.deleteExpired = async function () {
  var t = (new Date()).getTime();
  return await this.stmt_delete_expired.run([t]);
};

kvs.prototype.vacuum = async function () {
  return await this.db.run('vacuum');
};

// export
const KV = function () { };

KV.prototype.init = async function (path) {
  // empty path, temp disk database, or ':memory:'
  this.db = await new kvs().open(path);
  this.path = path;
  const result = await this.db.deleteExpired();
  await this.db.vacuum();
  console.log(`Removed ${result.stmt.changes} expired items for ${path}`);
  this.setupCron();
  return this;
};

KV.prototype.setupCron = async function () {
  // empty path, temp disk database, or ':memory:'
  const self = this;
  const every30mins = '0 */30 * * * *';
  new CronJob({
    cronTime: every30mins,
    onTick: async () => {
      const result = await self.db.deleteExpired();
      console.log(`Removed ${result.stmt.changes} expired items for ${self.path}`);
    },
    start: true,
    timeZone
  });
};

KV.prototype.get = async function (key) {
  return await this.db.get(key);
};

KV.prototype.set = KV.prototype.put = async function (key, value, expire) {
  return await this.db.put(key, value, expire);
};


module.exports = KV;
