(() => {
  'use strict';
  const providers=['youtube','twitch'];
  const tabs=[...document.querySelectorAll('[data-provider]')];
  function select(provider,focus=false){
    if(!providers.includes(provider))return;
    for(const tab of tabs){
      const active=tab.dataset.provider===provider;
      tab.setAttribute('aria-selected',String(active));tab.tabIndex=active?0:-1;
      document.getElementById(tab.getAttribute('aria-controls')).hidden=!active;
      if(active&&focus)tab.focus();
    }
  }
  for(const tab of tabs){
    tab.addEventListener('click',()=>select(tab.dataset.provider));
    tab.addEventListener('keydown',event=>{
      let next;
      if(event.key==='ArrowRight'||event.key==='ArrowLeft')next=providers[1-providers.indexOf(tab.dataset.provider)];
      else if(event.key==='Home')next=providers[0];
      else if(event.key==='End')next=providers[1];
      else return;
      event.preventDefault();select(next,true);
    });
  }
  function fromHash(){
    if(['#twitchSli','#sliTitle','#twitchPanel'].includes(location.hash))select('twitch');
    else if(['#reliabilityTitle','#youtubePanel'].includes(location.hash))select('youtube');
  }
  window.addEventListener('hashchange',fromHash);fromHash();
})();
