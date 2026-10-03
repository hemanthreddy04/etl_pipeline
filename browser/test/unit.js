const fs=require('fs'),path=require('path'),crypto=require('crypto');
const src=fs.readdirSync(path.join(__dirname,'../parts')).filter(f=>/^2\d-be-.*\.js$/.test(f)).sort().map(f=>fs.readFileSync(path.join(__dirname,'../parts',f),'utf8')).join('\n');
const m={exports:{}};new Function('module',src)(m);const BE=m.exports,{sha256,try_cast,parseCsv}=BE._;
let bad=0,n=0;
for(let i=0;i<400;i++){let s='';const len=i<200?i:Math.floor(Math.random()*5000);for(let j=0;j<len;j++)s+=String.fromCodePoint([97+j%26,0xe9,0x4e2d,0x1f600][Math.floor(Math.random()*4)]);
  n++;if(sha256(s)!==crypto.createHash('sha256').update(s,'utf8').digest('hex')){bad++;if(bad<4)console.log('sha mismatch at length',len)}}
console.log('sha256:',n-bad,'of',n,'match the reference implementation');
const vec=JSON.parse(fs.readFileSync(path.join(__dirname,'vectors.json'),'utf8').replace(/\bInfinity\b/g,'1e999'));let tb=0;
for(const [v,t,want] of vec){let got=try_cast(v,t);if(got===undefined)got=null;
  const same=want===got||(typeof want==='number'&&typeof got==='number'&&(Math.abs(want-got)<=1e-9*Math.abs(want)||(!Number.isFinite(want)&&!Number.isFinite(got))));
  if(!same){tb++;if(tb<25)console.log('  try_cast',JSON.stringify(v),t,'python',JSON.stringify(want),'js',JSON.stringify(got))}}
console.log('try_cast:',vec.length-tb,'of',vec.length,'match Python');
console.log(JSON.stringify(parseCsv('a,b\r\n"x, ""y""",2\n\n"multi\nline",3\nlast,')));
