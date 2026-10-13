(() => {
  'use strict';
  let data = null, selected = '7d', busy = false;
  const el = id => document.getElementById(id);
  const make = (tag, cls, text) => { const n=document.createElement(tag); if(cls)n.className=cls; if(text!==undefined)n.textContent=text; return n; };
  const pct = n => typeof n==='number' && Number.isFinite(n) ? n.toFixed(3)+'%' : '—';
  const num = n => n.toLocaleString('en-US');
  const date = v => new Intl.DateTimeFormat('en-GB',{timeZone:'Asia/Tokyo',month:'short',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false}).format(new Date(v))+' JST';
  const stale = () => !data || !Number.isFinite(Date.parse(data.generated_at)) || Date.now()-Date.parse(data.generated_at)>180000 || Date.parse(data.generated_at)>Date.now();
  function render(){
    const grid=el('sliGrid');grid.replaceChildren();el('sliTimeline').replaceChildren();
    document.querySelectorAll('[data-sli-window]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.sliWindow===selected)));
    el('sliWindowNote').textContent=selected==='7d'?'7 days · primary assessment window':'24 hours · early feedback';
    if(stale()){
      el('sliCollection').textContent='Unavailable';
      el('sliCollectionMeta').textContent='Fresh measurements are required. SLO assessment is unknown.';
      grid.append(make('p','sli-empty','Fresh arena measurements are required. Missing observations are never counted as successful delivery.'));
      el('sliStamp').textContent=data?TokyoReliability.stamp(data.generated_at)+' · stale':'measurement unavailable';return;
    }
    const window=data.windows.find(w=>w.id===selected);
    el('sliCollection').textContent=window.full_history?'History available':'Collecting history';
    el('sliCollectionMeta').textContent='Observations began '+date(data.history_started_at)+'. An assessment requires a complete window and 99% observation coverage.';
    for(const m of window.metrics){
      const assessment=m.assessment==='COLLECTING'?'Collecting':m.assessment==='NO_DUE'?'No renewal due':m.assessment.charAt(0)+m.assessment.slice(1).toLowerCase();
      const facts=[{label:'Reference',value:pct(m.target_percent)},{label:'Assessment',value:assessment}];
      const fact=(label,value)=>facts.push({label,value});
      if(m.id==='daily_renewal'){
        fact('Verified / failed',m.good_events+' / '+m.bad_events);
        fact('Unknown / pending',m.unknown_events+' / '+m.pending_events);
        fact('Expected renewals',String(m.expected_events));
        fact('Deadline','00:00 JST + 600 s');
      }else{
        fact('Known observations',num(m.good_minutes+m.bad_minutes)+' min');
        fact('Coverage',pct(m.coverage_percent));
        fact('Conservative SLI',pct(m.conservative_percent));
        fact('Confirmed downtime',num(m.bad_minutes)+' min');
        fact('Unknown / missing',num(m.unknown_minutes+m.missing_minutes)+' min');
        fact('Error budget',m.error_budget_minutes.toFixed(1)+' min');
        fact('Confirmed budget used',m.confirmed_budget_used_percent.toFixed(1)+'%');
      }
      const basis=m.id==='api_measurement'?'Successful observations / all expected minutes.':m.id==='daily_renewal'?'Verified successes / known renewal outcomes.':'Observed SLI / known minutes only.';
      const card=TokyoReliability.card({id:m.id,label:m.label,window:'rolling '+selected,value:pct(m.value_percent),facts,note:basis+' '+m.definition});
      card.dataset.sli=m.id;grid.append(card);
    }
    for(const b of data.timeline){
      const bar=make('div','sli-hour');bar.title=date(b.start_at)+' · LIVE '+b.good+' min · OFFLINE '+b.bad+' min · unknown '+(b.unknown+b.missing)+' min';
      bar.setAttribute('role','img');bar.setAttribute('aria-label',bar.title);
      for(const [cls,count] of [['sli-good',b.good],['sli-bad',b.bad],['sli-unknown',b.unknown+b.missing]]){const part=make('i',cls);part.style.height=(count/60*100)+'%';bar.append(part);}
      el('sliTimeline').append(bar);
    }
    el('sliStamp').textContent=TokyoReliability.stamp(data.generated_at);
  }
  async function refresh(){
    if(busy)return;busy=true;
    try{
      const r=await fetch('./twitch-sli.json?t='+Date.now(),{cache:'no-store',signal:AbortSignal.timeout(10000)});
      if(!r.ok)throw new Error('Unavailable');const v=await r.json();
      if(v.schema!=='stream.twitch_sli_public.v1'||!Array.isArray(v.windows)||!Array.isArray(v.timeline))throw new Error('Schema');
      data=v;
    }catch(_){data=null;}
    finally{busy=false;render();}
  }
  document.querySelectorAll('[data-sli-window]').forEach(b=>b.addEventListener('click',()=>{selected=b.dataset.sliWindow;render();}));
  el('refresh').addEventListener('click',refresh);setInterval(refresh,60000);refresh();
})();
