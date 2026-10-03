#!/bin/sh
# Assemble the single-file site that the service serves from app/static/index.html
cd "$(dirname "$0")"
cat parts/*.js > /tmp/mcp-app.js
node --check /tmp/mcp-app.js || exit 1
{
  echo '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">'
  echo '<style>:root{color-scheme:light}body{margin:0}img{max-width:100%}[hidden]{display:none!important}</style>'
  cat parts/01-style.html
  echo '</head><body>'
  cat parts/02-shell.html
  echo '<script>'; cat /tmp/mcp-app.js; echo '</script></body></html>'
} > ../app/static/index.html
wc -c ../app/static/index.html
