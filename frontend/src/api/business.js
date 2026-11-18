import {api,post} from './client.js';
const qs=o=>{let p=new URLSearchParams(Object.entries(o).filter(([,v])=>v!==undefined&&v!==null&&v!==''));let s=p.toString();return s?'?'+s:'';};
export const workloads={list:()=>api('/workloads'),save:(d,id)=>post('/workloads'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const candidates={list:()=>api('/candidates'),save:(d,id)=>post('/candidates'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const experiments={list:()=>api('/experiments'),run:d=>post('/experiments',d),detail:id=>api('/experiments/'+id)};
export const telemetry={list:(f={})=>api('/telemetry'+qs(f)),backfill:d=>post('/telemetry',d),correct:(id,d)=>post('/telemetry/'+id,d,'PUT')};
export const archive={status:()=>api('/archive'),start:d=>post('/archive/jobs',d),resume:id=>post('/archive/jobs/'+id+'/resume',{})};
