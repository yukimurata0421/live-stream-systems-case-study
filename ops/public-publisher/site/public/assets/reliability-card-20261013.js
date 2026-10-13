/* Shared presentation only. Provider measurements and assessment stay at source. */
(() => {
  'use strict';
  const node=(tag,cls,text)=>{const n=document.createElement(tag);if(cls)n.className=cls;if(text!==undefined)n.textContent=text;return n;};
  function card({id,label,window,value,facts,note}){
    const box=node('article','reliability-card');if(id)box.dataset.metric=id;
    const head=node('div','reliability-head');head.append(node('h3','',label),node('span','',window));
    const rows=node('dl','reliability-facts');
    for(const fact of facts||[])rows.append(node('dt','',fact.label),node('dd','',fact.value));
    box.append(head,node('strong','',value),rows,node('p','reliability-note',note));return box;
  }
  function stamp(value){
    const time=typeof value==='number'?value*1000:Date.parse(value);
    if(!Number.isFinite(time))return 'measurement unavailable';
    const age=Math.max(0,Math.floor((Date.now()-time)/1000));
    const ageText=age<60?age+'s':age<3600?Math.floor(age/60)+'m':Math.floor(age/3600)+'h';
    return 'measured '+new Date(time).toLocaleString('ja-JP',{timeZone:'Asia/Tokyo',hour12:false})+' JST / '+ageText+' old';
  }
  window.TokyoReliability=Object.freeze({card,stamp});
})();
