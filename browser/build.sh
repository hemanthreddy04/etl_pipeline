#!/bin/sh
# Assemble the browser edition: the site plus the service ported to JavaScript, running on SQLite in the page.
#   docs/index.html + docs/sql-asm.js   a static site (GitHub Pages can serve the docs/ folder as it is)
#   browser/out/                        the same page without the document wrapper, for publishing as a Claude artifact
cd "$(dirname "$0")" || exit 1
mkdir -p out ../docs
cat parts/*.js > out/app.js
node --check out/app.js || exit 1
{ cat parts/01-style.html parts/02-shell.html; echo '<script src="sql-asm.js"></script>'; echo '<script>'; cat out/app.js; echo '</script>'; } > out/medallion-control-plane-live.html
cp vendor/sql-asm.js out/sql-asm.js
{
  echo '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">'
  echo '<style>:root{color-scheme:light}body{margin:0}img{max-width:100%}[hidden]{display:none!important}</style></head><body>'
  cat out/medallion-control-plane-live.html
  echo '</body></html>'
} > ../docs/index.html
cp vendor/sql-asm.js ../docs/sql-asm.js
wc -c ../docs/index.html ../docs/sql-asm.js
