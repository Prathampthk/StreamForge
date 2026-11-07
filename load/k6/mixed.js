import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
const acknowledged = new Counter('kafka_acknowledged_events');
const unconfirmed = new Counter('unconfirmed_or_rejected_events');
const batch = Number(__ENV.BATCH || 100);
const seed = Number(__ENV.SEED || 42);
export const options = { scenarios: { mixed: { executor:'constant-arrival-rate', rate:Number(__ENV.EVENTS_PER_SECOND || 1000)/batch, timeUnit:'1s', duration:__ENV.DURATION || '15m', preAllocatedVUs:32, maxVUs:256, gracefulStop:'10s' } }, thresholds: { checks:['rate>0.99'] } };
function hash(n) { n=(n^seed)>>>0; n=Math.imul(n^(n>>>16),0x45d9f3b); n=Math.imul(n^(n>>>16),0x45d9f3b); return (n^(n>>>16))>>>0; }
export default function() {
 const rows=[]; const index=exec.scenario.iterationInTest*batch;
 for(let j=0;j<batch;j++) { const i=index+j,h=hash(i),ms=Date.now().toString(16).padStart(12,'0');
  const id=`${ms.slice(0,8)}-${ms.slice(8)}-7${exec.vu.idInTest.toString(16).padStart(3,'0')}-8${(h&4095).toString(16).padStart(3,'0')}-${i.toString(16).padStart(12,'0')}`;
  const e={schema_version:1,event_id:id,tenant_id:__ENV.TENANT || 'benchmark',user_id:`user_${h%100000}`,event_type:h%100<60?'page_view':h%100<80?'add_to_cart':h%100<90?'purchase':'api_request',occurred_at:new Date().toISOString(),attributes:{source:'k6'}};
  if(e.event_type==='purchase')Object.assign(e,{order_id:`order_${i}`,amount_cents:h%100000+1,currency:h%2?'USD':'EUR'});
  if(e.event_type==='api_request')e.status_code=hash(h)%100<10?503:200;
  rows.push(e);
 }
 const response=http.post((__ENV.INGEST_URL || 'http://bench-ingest:8080')+'/v1/events/batch',JSON.stringify(rows),{headers:{'Content-Type':'application/json',Authorization:'Bearer '+__ENV.INGEST_API_KEY},timeout:'10s',tags:{name:'ingest.batch'}});
 let body={};try{body=response.json();}catch(_){}
 const ok=response.status===202 && body.accepted===batch && body.acknowledgments && body.acknowledgments.length===batch;
 check(response,{'Kafka acknowledged every event':()=>ok});
 if(ok)acknowledged.add(batch);else unconfirmed.add(batch);
}
