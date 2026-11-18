import {workloads,candidates,experiments,telemetry,archive} from '../api/business.js';
export const store={workloads:[],candidates:[],experiments:[],telemetry:[],archive:null,user:null,telemetryFilter:{}};
export async function refresh(){[store.workloads,store.candidates,store.experiments,store.telemetry,store.archive]=await Promise.all([workloads.list(),candidates.list(),experiments.list(),telemetry.list(store.telemetryFilter),archive.status()]);}
