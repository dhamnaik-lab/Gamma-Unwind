/* GAMMA R10-C2A: broker-independent selections INSIDE existing Watchlists page.
   Storage API from R10-B. Does not activate scanner filtering, index signals, orders,
   market data, scheduler, generated-contract lists, or broker switch. */
(() => {
  'use strict';
  const ID = 'gu-unified-watchlist';
  const API = '/api/user-symbols';
  const LIMIT = 75;
  const validSymbol = /^[A-Z][A-Z0-9&._-]{0,24}$/;
  let view = null;
  let pendingReconcile = false;

  const el = (tag, className, content) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (content !== undefined) node.textContent = content;
    return node;
  };
  function watchlistsRoute() {
    return /^#\/watchlists(?:[/?]|$)/.test(window.location.hash || '');
  }
  function mainContainer() {
    return document.querySelector('#root main');
  }
  function validDoc(d) {
    return !!(d && typeof d === 'object' && d.schema === 1 &&
      Number.isSafeInteger(d.revision) && d.revision >= 0 &&
      Array.isArray(d.stocks) && d.stocks.length <= LIMIT &&
      d.stocks.every(x => typeof x === 'string' && validSymbol.test(x) &&
        x !== 'NIFTY' && x !== 'BANKNIFTY') &&
      new Set(d.stocks).size === d.stocks.length &&
      Array.isArray(d.indices) && d.indices.length <= 2 &&
      d.indices.every(x => x === 'NIFTY' || x === 'BANKNIFTY') &&
      new Set(d.indices).size === d.indices.length && d.active_for_scanning === false);
  }
  async function request(method, data) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 12000);
    try {
      const response = await fetch(API, {
        method, credentials:'same-origin', cache:'no-store', redirect:'error',
        headers: method === 'GET' ? {'Accept':'application/json'} :
          {'Accept':'application/json', 'Content-Type':'application/json','X-Gamma-Control':'1'},
        body:method === 'GET' ? undefined : JSON.stringify(data),
        signal:controller.signal
      });
      if (response.status === 409) {
        const e = new Error('Selections changed elsewhere. Refresh before saving.');
        e.conflict = true; throw e;
      }
      let result;
      try { result = await response.json(); }
      catch (_) { throw new Error('Invalid response from the local selection service'); }
      if (!response.ok) throw new Error('Selection API declined request (HTTP ' + response.status + ')');
      if (!validDoc(result)) throw new Error('Unexpected selection response; no changes applied');
      return result;
    } finally { clearTimeout(timer); }
  }
  function createCard() {
    const host = el('section','gu-unified-card'); host.id = ID;
    host.setAttribute('aria-labelledby','gu-unified-heading');
    const header = el('div','gu-unified-header');
    const labels = el('div','gu-unified-labels');
    const title = el('h2','gu-unified-heading','My Watchlist'); title.id = 'gu-unified-heading';
    const subtitle = el('p','gu-unified-subtitle','User-selected F&O stocks and index underlyings · shared across brokers');
    labels.append(title,subtitle);
    const toggle = el('button','gu-unified-toggle','Manage symbols');
    toggle.type='button'; toggle.setAttribute('aria-controls','gu-unified-editor');
    toggle.setAttribute('aria-expanded','false');
    header.append(labels,toggle);host.append(header);
    const summary = el('p','gu-unified-summary','Loading your saved selections…');
    summary.setAttribute('role','status');host.append(summary);
    const editor=el('div','gu-unified-editor');editor.id='gu-unified-editor';editor.hidden=true;
    editor.append(el('p','gu-unified-warning',
      'Selections are saved, but do NOT yet control GAMMA scanning or create option contracts. Existing generated lists and paper positions are unchanged.'));
    const columns=el('div','gu-unified-columns');
    const stocksCol=el('div','gu-unified-col');
    const stocksTitle=el('h3','','F&O stock underlyings');
    const stocksHint=el('p','gu-unified-hint','Uppercase symbols, one per line; maximum 75. F&O membership is not verified in this phase.');
    const textarea=el('textarea','gu-unified-textarea');textarea.rows=5;
    textarea.setAttribute('aria-label','Selected F&O stock underlyings');
    textarea.placeholder='SBIN\nRELIANCE\nICICIBANK';textarea.spellcheck=false;
    stocksCol.append(stocksTitle,stocksHint,textarea);
    const indexCol=el('div','gu-unified-col');
    indexCol.append(el('h3','','Index underlyings'));
    const checkboxes={};
    for (const name of ['NIFTY','BANKNIFTY']) {
      const label=el('label','gu-unified-check');
      const input=el('input');input.type='checkbox';input.value=name;
      input.setAttribute('aria-label','Select '+name);
      checkboxes[name]=input;
      label.append(input,el('span','',name));indexCol.append(label);
    }
    indexCol.append(el('p','gu-unified-hint','Index selections are stored only. Index strategy signals remain disabled pending validation.'));
    columns.append(stocksCol,indexCol);editor.append(columns);
    const feedback=el('p','gu-unified-feedback','');feedback.setAttribute('role','status');
    feedback.setAttribute('aria-live','polite');editor.append(feedback);
    const foot=el('div','gu-unified-footer');
    const revision=el('span','gu-unified-revision','Not loaded');
    const refresh=el('button','gu-unified-action','Refresh');refresh.type='button';
    const save=el('button','gu-unified-action gu-unified-primary','Save selections');save.type='button';
    foot.append(revision,refresh,save);editor.append(foot);host.append(editor);
    let snapshot=null, busy=false;
    function note(s,ok) {feedback.textContent=s;feedback.dataset.result=ok?'ok':'error';}
    function enabled(isEnabled) {for(const node of [textarea,...Object.values(checkboxes),refresh,save])node.disabled=!isEnabled;}
    function render(d) {
      if(!host.isConnected)return;
      snapshot=d;textarea.value=d.stocks.join('\n');
      for(const name of ['NIFTY','BANKNIFTY']) checkboxes[name].checked=d.indices.includes(name);
      revision.textContent='Saved revision '+d.revision;
      const stockText=d.stocks.length?d.stocks.join(' · '):'No stock selections';
      const indexText=d.indices.length?d.indices.join(' · '):'No index selections';
      summary.textContent='Saved draft (scanner filtering inactive):  F&O: '+stockText+'  |  Indices: '+indexText;
      enabled(true);
    }
    async function refreshData() {
      if(busy)return;
      busy=true;enabled(false);note('Loading saved selections…',true);
      try { const d=await request('GET');render(d);note('Selections loaded. Scanner filtering remains inactive.',true); }
      catch (_) { snapshot=null;note('Unable to load selections from GAMMA local API. Please retry.',false);refresh.disabled=false; }
      finally{busy=false;}
    }
    function draft() {
      if(!snapshot)throw new Error('Selections have not loaded.');
      const raw=textarea.value.trim();
      const stocks=raw?raw.split(/[\s,;]+/):[];
      if(stocks.length>LIMIT)throw new Error('Maximum 75 stock symbols.');
      if(stocks.some(x=>!validSymbol.test(x)||x==='NIFTY'||x==='BANKNIFTY'))
        throw new Error('Enter valid uppercase F&O stock symbols; use separate index checkboxes.');
      if(new Set(stocks).size!==stocks.length)throw new Error('Remove duplicate stock symbols.');
      const indices=['NIFTY','BANKNIFTY'].filter(x=>checkboxes[x].checked);
      return {revision:snapshot.revision,stocks,indices};
    }
    async function persist() {
      if(busy||!snapshot)return;
      let data;
      try{data=draft();}catch(e){note(e.message,false);return;}
      busy=true;enabled(false);note('Saving selections…',true);
      try {const d=await request('POST',data);render(d);note('Saved. Scanner filtering is not yet active.',true);}
      catch(e) {
        note(e.conflict?'Another edit was saved. Refresh to resolve the revision conflict.':
          'Save failed. '+e.message,false);
        if(e.conflict)refresh.disabled=false;else enabled(true);
      } finally{busy=false;}
    }
    toggle.addEventListener('click',()=>{
      editor.hidden=!editor.hidden;
      toggle.setAttribute('aria-expanded',String(!editor.hidden));
      toggle.textContent=editor.hidden?'Manage symbols':'Hide editor';
      if(!editor.hidden){if(!snapshot)refreshData();textarea.focus();}
    });
    refresh.addEventListener('click',()=>{
      if(snapshot&&!window.confirm('Discard unsaved changes and reload saved selections?'))return;
      refreshData();
    });
    save.addEventListener('click',persist);
    // Read-only GET only after the Watchlists page exists; no brokerage requests.
    refreshData();
    return host;
  }
  function reconcile() {
    pendingReconcile=false;
    const onPage=watchlistsRoute();const main=mainContainer();
    if(!onPage||!main){
      if(view){view.remove();view=null;}return;
    }
    if(view&&view.isConnected&&view.parentElement===main)return;
    if(view){view.remove();view=null;}
    view=createCard();main.insertBefore(view,main.firstChild);
  }
  function scheduleReconcile(){
    if(pendingReconcile)return;
    pendingReconcile=true;queueMicrotask(reconcile);
  }
  function start(){
    window.addEventListener('hashchange',scheduleReconcile);
    new MutationObserver(scheduleReconcile).observe(document.getElementById('root') || document.documentElement,
      {subtree:true,childList:true});
    scheduleReconcile();
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',start,{once:true});
  else start();
})();
