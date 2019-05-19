var multifeed = require('multifeed')
var kcore = require('kappa-core')
var randombytes = require('randombytes')
var pump = require('pump')
var split = require('split2')
var through = require('through2')
var EventEmitter = require('events').EventEmitter
var duplexify = require('duplexify')
var collect = require('collect-stream')

var JOB = 'j!'
var OUTPUT = 'o!'
var STDOUT = '1!'
var STDERR = '2!'
var EXIT = '3!'

var types = { 1: 'stdout', 2: 'stderr' }

module.exports = Swarmhead

function Swarmhead (opts) {
  var self = this
  if (!(this instanceof Swarmhead)) return new Swarmhead(opts)
  this.hypercore = opts.hypercore
  this.swarm = opts.swarm
  this.storage = opts.storage
  this.exec = opts.exec
  this.multi = multifeed(this.hypercore, this.storage,
    { valueEncoding: 'json' })
  this.kcore = kcore(this.storage, { multifeed: this.multi })
  this.db = opts.db
  this.kcore.use('logs', 1, {
    api: {
      list: function (core, id) {
        var d = duplexify.obj()
        core.ready(function () {
          var out = through.obj(function (row, enc, next) {
            var parts = row.key.toString().split('!')
            var type = types[parts[3]]
            if (type === 'stdout' || type === 'stderr') {
              next(null, {
                type: type,
                worker: parts[2],
                time: parts[4],
                data: row.value
              })
            } else if (type === 'exit') {
              next(null, {
                type: type,
                code: Number(row.value.toString())
              })
            } else next()
          })
          d.setReadable(out)
          pump(self.db.createReadStream({
            gt: OUTPUT + id + '!',
            lt: OUTPUT + id + '!\uffff',
            keyEncoding: 'utf8',
            valueEncoding: 'json'
          }), out)
        })
        return d
      }
    },
    map: function (msgs, next) {
      var batch = []
      msgs.forEach(function (msg) {
        if (msg.value && msg.value.type === 'stderr') {
          batch.push({
            type: 'put',
            key: OUTPUT + msg.value.job + '!' + msg.value.worker
              + '!' + STDERR + msg.value.time,
            value: msg.value.data
          })
        } else if (msg.value && msg.value.type === 'stdout') {
          batch.push({
            type: 'put',
            key: OUTPUT + msg.value.job + '!' + msg.value.worker
              + '!' + STDOUT + msg.value.time,
            value: msg.value.data
          })
        } else if (msg.value && msg.value.type === 'exit') {
          batch.push({
            type: 'put',
            key: OUTPUT + msg.value.job + '!' + msg.value.worker
              + '!' + EXIT + msg.value.time,
            value: msg.value.code
          })
        }
      })
      self.db.batch(batch, { valueEncoding: 'json' }, next)
    }
  })
  self.kcore.use('jobs', 1, {
    api: {
      list: function () {
        var d = duplexify.obj()
        this.ready(function () {
          var out = through.obj(function (row, enc, next) {
            next(null, row.key.toString().slice(JOB.length))
          })
          pump(self.db.createReadStream({
            gt: JOB,
            lt: JOB + '\uffff',
            keyEncoding: 'utf8',
            valueEncoding: 'json'
          }), out)
          d.setReadable(out)
        })
        return d
      }
    },
    map: function (msgs, next) {
      var batch = []
      msgs.forEach(function (msg) {
        if (msg.value && msg.value.type === 'deploy') {
          batch.push({
            type: 'put',
            key: JOB + msg.value.id,
            value: 0
          })
        }
      })
      self.db.batch(batch, { valueEncoding: 'json' }, next)
    }
  })
}

Swarmhead.prototype.create = function (cb) {
  var self = this
  if (!cb) cb = noop
  self.kcore.feed('node', function (err, feed) {
    if (err) return cb(err)
    cb(null, feed.key)
  })
}

Swarmhead.prototype.deploy = function (cmd, cb) {
  if (!cb) cb = noop
  this.kcore.feed('jobs', function (err, feed) {
    if (err) return cb(err)
    var id = randombytes(8).toString('hex')
    feed.append({
      type: 'deploy',
      command: cmd,
      id
    }, onappend)
    function onappend (err, seq) {
      if (err) cb(err)
      else cb(null, id)
    }
  })
}

Swarmhead.prototype.join = function (id, cb) {
  if (!cb) cb = noop
  var self = this
  self.kcore.feed('node', function (err, feed) {
    if (err) return cb(err)
    var sw = self.swarm({ id: feed.key })
    sw.listen()
    sw.join(id ? toBuf(id) : feed.key)
    sw.on('connection', function (stream, info) {
      var r = self.multi.replicate({ live: true })
      pump(r, stream, r)
    })
    cb(null, sw)
  })
}

Swarmhead.prototype.work = function (id, cb) {
  var self = this
  var w = new EventEmitter
  self.join(id, function (err, sw) {
    if (err) return cb(err)
    w.swarm = sw
    self.kcore.use('work', 1, {
      api: {},
      map: map
    })
    cb(null, w)
  })
  function map (msgs, next) {
    ;(function advance (i) {
      if (i >= msgs.length) return next()
      var msg = msgs[i]
      //if (msg.key !== id || !msg.value
      if (!msg || !msg.value
      || msg.value.type !== 'deploy'
      || !msg.value.id
      || typeof msg.value.command !== 'string'
      ) return advance(i+1)
      run(msg, function (err) {
        if (err) next(err)
        else advance(i+1)
      })
    })(0)
  }
  function run (msg, cb) {
    var jobId = msg.value.id
    self.kcore.feed('result/' + jobId, function (err, feed) {
      if (err) return cb(err)
      var ps = self.exec(msg.value.command)
      var job = new EventEmitter
      job.id = jobId
      job.process = ps
      job.swarm = w.swarm
      w.emit('job', job)
      ps.on('error', function (err) {
        job.emit('error', err)
        cb(err)
      })
      var pending = 3
      ps.on('exit', function (code) {
        feed.append({
          type: 'exit',
          job: jobId,
          worker: feed.key.toString('hex'),
          time: Date.now(),
          code: code
        })
        job.emit('exit', code)
        done()
      })
      ps.stderr.pipe(split()).pipe(through(function (buf, enc, next) {
        feed.append({
          type: 'stderr',
          job: jobId,
          worker: feed.key.toString('hex'),
          time: Date.now(),
          data: buf.toString()
        })
        next()
      }, done))
      ps.stdout.pipe(split()).pipe(through(function (buf, enc, next) {
        feed.append({
          type: 'stdout',
          job: jobId,
          worker: feed.key.toString('hex'),
          time: Date.now(),
          data: buf.toString()
        })
        next()
      }, done))
      function done () {
        if (--pending === 0) cb()
      }
    })
  }
}

Swarmhead.prototype.jobs = function (cb) {
  if (!cb) cb = noop
  var self = this
  var d = duplexify.obj()
  self.kcore.ready('jobs', function () {
    d.setReadable(self.kcore.api.jobs.list())
  })
  if (cb) collect(d, cb)
  return d
}

Swarmhead.prototype.logs = function (id, cb) {
  if (!cb) cb = noop
  var self = this
  var d = duplexify.obj()
  self.kcore.ready('logs', function () {
    d.setReadable(self.kcore.api.logs.list(id))
  })
  if (cb) collect(d, cb)
  return d
}

function noop () {}

function toBuf (x) {
  return typeof x === 'string' ? Buffer.from(x, 'hex') : x
}
