import { build } from '../../sdks/typescript/node_modules/esbuild/lib/main.js';
import { readFile, writeFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
const bundled = await build({entryPoints:['src/browser.ts'],bundle:true,write:false,format:'esm',platform:'browser',target:'es2022'});
await writeFile('src/assets.ts',`export const page = ${JSON.stringify(await readFile('src/index.html','utf8'))};\nexport const browser = ${JSON.stringify(bundled.outputFiles[0].text)};\n`);
const args = process.argv.slice(2), suffix = args.includes('starlingmonkey') ? '-starlingmonkey' : '';
for(const name of ['front','room','pusher','edge','relay','directory']) execFileSync('node',['../../sdks/typescript/bin/wasmkeep-ts.mjs','build',`src/${name}.ts`,'-o',`${name}${suffix}.wasm`,...(name === 'front' ? ['--world','front']:[]),...args],{stdio:'inherit'});
