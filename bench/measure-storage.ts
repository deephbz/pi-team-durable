// Optional, process-local instrumentation for the disposable scale benchmark.
// node --import ./bench/measure-storage.ts bench/scale.ts --tasks 500 --alerts 1000
// PTB_STORAGE_MEASURE_OUTPUT selects the JSON report. The live store is unchanged.
import { SqliteStorage } from '@earendil-works/pi-durable/storage/sqlite';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync,rmSync,statSync,writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
const bytes=(value:unknown)=>Buffer.byteLength(JSON.stringify(value));
const kinds=new Map<number,string>(),samples:Array<{command:string;bytes:number;headBytes:number;documents:number;resultBytes?:number}>=[];
const originalCommit=SqliteStorage.prototype.commit,originalClose=SqliteStorage.prototype.close;
SqliteStorage.prototype.commit=async function(writes,context){
 let command='other',headBytes=0,documents=0,resultBytes:number|undefined;
 for(const write of writes){
  if(write.type==='document.create')kinds.set(write.record.id,write.record.kind);
  if(write.type!=='document.create'&&write.type!=='document.change')continue;
  documents++;const kind=kinds.get(write.type==='document.create'?write.record.id:write.id),content=write.content;
  if(kind==='ptb.head')headBytes+=bytes(content);
  if(kind==='ptb.call.direct'&&content.kind==='base'&&typeof content.value.fingerprint==='string'){
   command=JSON.parse(content.value.fingerprint).name;
   if(typeof content.value.result==='string')resultBytes=Buffer.byteLength(content.value.result);
  }
 }
 samples.push({command,bytes:bytes(writes),headBytes,documents,...(resultBytes===undefined?{}:{resultBytes})});
 return originalCommit.call(this,writes,context);
};
const q=(values:number[],fraction:number)=>values.sort((a,b)=>a-b)[Math.floor((values.length-1)*fraction)]??0;
let measured=false;
SqliteStorage.prototype.close=async function(context){
 if(!measured){
  measured=true;
  // The adapter exposes its owned database facade. Query only this benchmark store.
  const {db,schema} = this as any,t=schema.tables;
  const retained=await db.all(`SELECT d.kind,COUNT(DISTINCT d.id) AS documents,SUM(length(CAST(r.content AS BLOB))) AS contentBytes FROM ${t.documents} d JOIN ${t.documentRevisions} r ON r.document_id=d.id GROUP BY d.kind`);
  const row=await db.get(`SELECT r.content FROM ${t.documents} d JOIN ${t.documentRevisions} r ON r.document_id=d.id WHERE d.kind='"ptb.head"' ORDER BY r.seq DESC LIMIT 1`);
  const head=JSON.parse(JSON.parse(row.content).value);
  const byCommand=Object.fromEntries([...new Set(samples.map(s=>s.command))].map(command=>{
   const xs=samples.filter(s=>s.command===command);
   return [command,{commits:xs.length,writeBytesP50:q(xs.map(s=>s.bytes),.5),writeBytesP99:q(xs.map(s=>s.bytes),.99),headWriteBytesP50:q(xs.map(s=>s.headBytes),.5),documentsP50:q(xs.map(s=>s.documents),.5),documentsP99:q(xs.map(s=>s.documents),.99),maxResultBytes:Math.max(0,...xs.map(s=>s.resultBytes??0))}];
  }));
  // Measure the physical footprint with and without the permanent direct-call
  // fallback in two vacuumed copies. Never delete receipts from the live store.
  const dir=mkdtempSync(join(tmpdir(),'ptb-byte-accounting-')),file=join(dir,'copy.sqlite');
  let compactBytes:number,withoutDirectCalls:number;
  try{
   await db.run('VACUUM INTO ?',file);compactBytes=statSync(file).size;
   const copy=new DatabaseSync(file);
   try{
    const ids=`SELECT id FROM ${t.documents} WHERE kind='"ptb.call.direct"'`;
    copy.exec(`DELETE FROM ${t.documentRevisions} WHERE document_id IN (${ids});DELETE FROM ${t.recordIds} WHERE id IN (${ids});DELETE FROM ${t.documents} WHERE kind='"ptb.call.direct"';VACUUM;`);
   }finally{copy.close();}
   withoutDirectCalls=statSync(file).size;
  }finally{rmSync(dir,{recursive:true,force:true});}
  const report={headBytes:bytes(head),headFields:Object.fromEntries(Object.entries(head).map(([key,value])=>[key,bytes(value)])),retained:retained.map((r:any)=>({...r,kind:JSON.parse(r.kind)})),compactBytes,withoutDirectCalls,directCallFootprint:compactBytes-withoutDirectCalls,byCommand};
  writeFileSync(process.env.PTB_STORAGE_MEASURE_OUTPUT??join(tmpdir(),'ptb-storage-measure.json'),JSON.stringify(report,null,2));
 }
 return originalClose.call(this,context);
};
