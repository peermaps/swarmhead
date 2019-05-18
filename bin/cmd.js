#!/usr/bin/env node
var minimist = require('minimist')
var argv = minimist(process.argv.slice(2), {
  alias: { d: 'datadir', s: 'swarm' }
})

function open () {
  var swarmhead = require('../')
  var levelup = require('levelup')
  var leveldown = require('leveldown')
  var path = require('path')
  var mkdirp = require('mkdirp')
  mkdirp.sync(path.join(argv.datadir,'db'))
  return swarmhead({
    hypercore: require('hypercore'),
    swarm: require('discovery-swarm'),
    exec: require('child_process').exec,
    storage: path.join(argv.datadir,'store'),
    db: levelup(leveldown(path.join(argv.datadir,'db')))
  })
}

if (argv._[0] === 'create') {
  var sh = open()
  sh.create(function (err, key) {
    console.log(key.toString('hex'))
  })
} else if (argv._[0] === 'deploy') {
  var sh = open()
  var cmd = argv._.slice(1).join(' ')
  sh.deploy(cmd, function (err, job) {
    if (err) return error(err)
    else console.log(job)
  })
} else if (argv._[0] === 'join') {
  var sh = open()
  sh.join(argv._[1] && String(argv._[1]), function (err, sw) {
    if (err) return error(err)
    sw.on('connection', function (stream, info) {
      console.log('connection',
        `${info.host}:${info.port} ${info.id.toString('hex')}`)
    })
  })
} else if (argv._[0] === 'work') {
  var sh = open()
  sh.work(argv._[1], function (err, w) {
    if (err) return error(err)
    w.on('job', function (job) {
      console.error(`# job started: ${job.id}`)
      job.process.stdout.pipe(process.stdout, { end: false })
      job.process.stderr.pipe(process.stderr, { end: false })
      job.process.on('exit', function (code) {
        console.error(`# job exited: ${job.id} code: ${code}`)
      })
      job.swarm.on('connection', function (stream, info) {
        console.log('connection',
          `${info.host}:${info.port} ${info.id.toString('hex')}`)
      })
    })
  })
} else if (argv._[0] === 'jobs') {
  var sh = open()
  sh.jobs(function (err, jobs) {
    if (err) return error(err)
    console.log(jobs)
  })
}

function error (err) {
  console.error(err)
  process.exit(1)
}
