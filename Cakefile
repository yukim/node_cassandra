color = (str, color) ->
  "\033[0;#{color}m#{str}\033[0m"

run = (a0, a1=[], options={}) ->
  {spawn} = require 'child_process'

  console.log color("$ #{a0} #{a1.join(' ')}", 32)

  proc = spawn a0, a1
  proc.stdout.on 'data', (d) -> console.log "#{d}"
  proc.stderr.on 'data', (d) -> console.warn "#{d}"
  proc.on        'exit', (status) ->
    if status > 0
      options.onerror status  if typeof options.onerror is 'function'
      process.exit(status)

    options.onsuccess()  if typeof options.onsuccess is 'function'

task 'doc:build', 'Build documentation using jsdoc', build = (callback) ->
  path = process.env['JSDOC_PATH']

  unless path?
    console.warn color("Error:", 31), "You need jsdoc-toolkit."
    console.warn
    console.warn "Download it from http://code.google.com/p/jsdoc-toolkit/, then install"
    console.warn "it to a path of your choice. Then run:"
    console.warn ""
    console.warn "$ JSDOC_PATH=/path/to/jsdoc-toolkit cake doc:build"
    console.warn ""
    process.exit 256

  run 'java', [
    '-jar', "#{path}/jsrun.jar"
    "#{path}/app/run.js"
    "--template=#{path}/templates/jsdoc"
    '--directory=doc'
    "lib/cassandra.js"
  ], onsuccess: callback

task 'doc:open', 'Opens documentation in your browser', ->
  build ->
    run 'open', ['doc/index.html']

task 'doc:deploy', 'Deploys documentation to gh-pages', ->
  build ->
    repo = process.env['GITHUB_REPO'] || 'yukim/node_cassandra'

    run 'git',
      "update-ghpages #{repo} -i doc --branch gh-pages".split(' '),
      onerror: (status) ->
        console.warn color("Error:", 31), "You need git-update-ghpages."
        console.warn "See: http://github.com/rstacruz/git-update-ghpages"
