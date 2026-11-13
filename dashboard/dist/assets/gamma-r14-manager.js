/* GAMMA-UNWIND R1.4: connection management INSIDE existing native token tab.
   Never stores secrets in browser storage; old unsafe switch UI is disabled.
   All calls remain same-origin, using existing GAMMA control header. */
(()=>{
  'use strict';
  const tag='gu-r14-panel';
  const endpoint='/api/broker-manager';
  let snapshot=null,choice='',pending=false,drawnChoice=null;
  const $=(sel,root=document)=>root.querySelector(sel);
  const el=(type,cls,txt)=>{const n=document.createElement(type); if(cls)n.className=cls;if(txt!==undefined)n.textContent=txt;return n};
  const note=(message,ok=false)=>{const n=$('#gu-r14-notice');if(n){n.textContent=message;n.className=ok?'gu-ok':'gu-note'}};
  async function call(path,method='GET',data){
    const controller=new AbortController();
    const limit=setTimeout(()=>controller.abort(),20000);
    try{
      const response=await fetch(endpoint+path,{method,credentials:'same-origin',cache:'no-store',
        headers:method==='GET'?{'Accept':'application/json'}:{'Content-Type':'application/json','X-Gamma-Control':'1'},
        body:method==='GET'?undefined:JSON.stringify(data),signal:controller.signal});
      const body=await response.json();
      if(!response.ok)throw new Error((body&&body.error)||'Broker operation failed');
      return body;
    }finally{clearTimeout(limit)}
  }
  function txt(parent,type,cls,value){const n=el(type,cls,value);parent.append(n);return n}
  function wipeSecretInputs(root){root.querySelectorAll('input[type="password"], input[data-gu-secret]').forEach(n=>{n.value=''})}
  function makeInput(parent,key,type){
    const label=el('label','gu-field');txt(label,'span','',key);
    const input=el('input');input.type=type;input.autocomplete='off';input.spellcheck=false;
    input.dataset.field=key;if(type==='password')input.dataset.guSecret='1';
    input.placeholder='Enter new value (not displayed later)';label.append(input);parent.append(label);return input;
  }
  async function saveProfile(row,container){
    const vals={};container.querySelectorAll('input[data-field]').forEach(n=>{if(n.value.trim())vals[n.dataset.field]=n.value.trim()});
    if(!Object.keys(vals).length){note('Enter at least one field to save.');return}
    await call('/profile/'+encodeURIComponent(row.id),'POST',{fields:vals});
    wipeSecretInputs(container);await refresh(true);note('Encrypted broker configuration saved. Active scanner was not changed.',true);
  }
  async function login(row,container){
    const vals={};container.querySelectorAll('input[data-login-field]').forEach(n=>{vals[n.dataset.loginField]=n.value.trim()});
    await call('/login/'+encodeURIComponent(row.id),'POST',{fields:vals});
    wipeSecretInputs(container);await refresh(true);note('Broker code exchanged. Session stored locally; market feed NOT verified. Active scanner unchanged.',true);
  }
  async function stage(row,container){
    const confirm=$('[data-confirm]',container)?.value||'';
    const ack=!!$('[data-ack]',container)?.checked;
    const result=await call('/switch/'+encodeURIComponent(row.id),'POST',{
      expected_active:snapshot.active,confirmation:confirm,acknowledge:ack});
    await refresh(true);
    note('Scanner staged: '+result.staged+'. '+(result.running?result.running.toUpperCase():'NONE')+' remains running until you restart. Backup: '+result.backup_name,true);
  }
  function button(label,callback,disabled){
    const b=el('button','gu-action',label);b.type='button';b.disabled=!!disabled;
    b.addEventListener('click',async()=>{if(pending)return;pending=true;b.disabled=true;
      try{await callback()}catch(e){note('Operation declined: '+(e.message||'unknown error'))}
      finally{pending=false;b.disabled=!!disabled}});return b;
  }
  function render(){
    const host=$('#'+tag);if(!host||!snapshot)return;
    host.replaceChildren();
    txt(host,'h2','gu-title','Broker connection manager · R1.4');
    txt(host,'p','gu-subtitle','Connections stay independent of the active scanner. Credentials are encrypted under your Windows user; active scanner credentials remain in legacy .env after a confirmed switch.');
    const summary=el('div','gu-summary');
    txt(summary,'strong','','Running: '+(snapshot.active?snapshot.active.toUpperCase():'NONE (select a broker)'));
    txt(summary,'span','',snapshot.restart_pending?' · Restart pending for '+(snapshot.staged?snapshot.staged.toUpperCase():'NONE'):' · No scanner change pending');
    host.append(summary);
    const picker=el('div','gu-pickers');
    for(const row of snapshot.brokers){const b=button(row.label,()=>{choice=row.id;render()},false);b.classList.add('gu-picker');if(row.id===choice)b.classList.add('gu-selected');picker.append(b)}
    host.append(picker);
    const row=snapshot.brokers.find(x=>x.id===choice);
    if(!row){
      txt(host,'p','gu-muted','No broker selected. Choose a broker above to configure credentials, log in, or stage it as your active scanner.');
      const n=txt(host,'p','gu-note','');n.id='gu-r14-notice';
      return;
    }
    const body=el('div','gu-body');host.append(body);
    txt(body,'h3','',''+row.label+' · '+row.state.replaceAll('_',' '));
    txt(body,'p','gu-muted','Adapter: '+row.adapter_status+'. Market-data feed: '+(row.feed_verified?'verified (read-only quote OK)':'unverified')+'. Configuration presence alone is not authentication.');
    const status=el('div','gu-status');
    for(const field of row.fields){const item=el('span',field.present?'gu-present':'gu-missing',field.key+': '+(field.present?'present':'missing'));status.append(item)}
    body.append(status);
    if(!row.active){
    const editor=el('section','gu-box');txt(editor,'h4','','Configure broker');
    txt(editor,'p','gu-muted','Only enter fields you want to add or change. Saved values will not be shown again.');
    for(const field of row.fields)makeInput(editor,field.key,'password');
    editor.append(button('Save configuration (scanner unchanged)',()=>saveProfile(row,editor),false));
    body.append(editor);
    }
    if(!row.active){
      const loginSection=el('section','gu-box');txt(loginSection,'h4','','Independent broker login');
      if(row.login_method==='redirect_code'){
        txt(loginSection,'p','gu-muted','Complete the broker-controlled login manually. Paste its single-use redirect URL/code (or required PIN/TOTP) into the fields below. No order permissions are requested by GAMMA.');
        const open=button('Open broker login page',async()=>{
          const d=await call('/login-url/'+encodeURIComponent(row.id));
          if(!d.url){note('Set required API credentials first; no trusted broker login URL is available.');return}
          window.open(d.url,'_blank','noopener,noreferrer');
        },false);loginSection.append(open);
        for(const field of row.login_fields){const inp=makeInput(loginSection,field,'password');inp.dataset.loginField=field;inp.removeAttribute('data-field')}
        loginSection.append(button('Exchange code for '+row.label+' (scanner unchanged)',()=>login(row,loginSection),false));
      }else{
        txt(loginSection,'p','gu-muted','This adapter does not provide an independent client-edition login exchange. Obtain its access token from the broker and save it in the configuration above; token presence will remain UNVERIFIED.');
      }
      body.append(loginSection);
      const disconnect=el('section','gu-box');txt(disconnect,'h4','','Disconnect saved inactive profile');
      txt(disconnect,'p','gu-muted','Remove this broker’s encrypted profile from GAMMA. Does not change the running scanner or remove legacy .env credentials.');
      const dc=makeInput(disconnect,'Type DISCONNECT '+row.id.toUpperCase(),'text');dc.dataset.disconnect='1';dc.removeAttribute('data-field');
      disconnect.append(button('Disconnect broker profile',async()=>{
        const confirmation=$('[data-disconnect]',disconnect)?.value||'';
        await call('/disconnect/'+encodeURIComponent(row.id),'POST',{confirmation});
        await refresh(true);note('Encrypted inactive broker profile cleared. Active scanner unchanged.',true);
      },false));body.append(disconnect);
      const sw=el('section','gu-box');txt(sw,'h4','','Switch scanner to '+row.label);
      txt(sw,'p','gu-muted','Switching writes the verified broker keys to legacy .env (with automatic backup in data/backups/) and requires a manual app restart. Never switch mid-trade.');
      if(row.can_stage&&!snapshot.restart_pending){
        const c=makeInput(sw,'Type SWITCH TO '+row.id.toUpperCase(),'text');c.dataset.confirm='1';c.removeAttribute('data-field');
        const l=el('label','gu-field');const a=el('input');a.type='checkbox';a.dataset.ack='1';l.append(a);l.append(document.createTextNode(' I understand that a manual app restart is required for the switch to take effect.'));sw.append(l);
        sw.append(button('Stage scanner switch (no restart)',()=>stage(row,sw),false));
      }else{
        txt(sw,'p','gu-muted',snapshot.restart_pending?'Restart pending; do not stage another broker.':(row.switch_reason||'Not eligible: required configuration/token is missing or adapter is unverified.'));
        if(!snapshot.restart_pending&&row.token_present&&row.adapter_status!=='beta-unverified'){
          sw.append(button('Verify session & feed (R1.5)',async()=>{
            const res=await call('/r15/verify-auth/'+encodeURIComponent(row.id),'POST',{});
            await refresh(true);
            note(row.label+': '+String(res.status||'checked').replaceAll('_',' ')+(res.feed_verified?' — switch unlocked above.':'.'),!!res.feed_verified);
          },false));
        }
      }
      body.append(sw);
    }else txt(body,'p','gu-ok','This is the active scanner. Use the original daily login form above; this panel does not reroute it.');
    const n=txt(host,'p','gu-note','');n.id='gu-r14-notice';
  }
  async function refresh(rebuild=true){
    try{
      snapshot=await call('/status');
      if(!snapshot||!Array.isArray(snapshot.brokers))throw new Error('Invalid broker readiness result');
      if(!snapshot.brokers.some(x=>x.id===choice))choice=snapshot.active||'';
      if(rebuild)render();
    }catch(e){const host=$('#'+tag);if(host)host.textContent='Broker manager status unavailable. Original login remains unchanged.'}
  }
  // Prevent historical R1 "choose for next restart" controls from bypassing readiness checks.
  document.addEventListener('click',ev=>{
    const b=ev.target.closest?.('button');
    if(b&&b.closest('[data-native-broker-catalog]')&&b.textContent.trim()==='Choose for next restart'){
      ev.preventDefault();ev.stopPropagation();ev.stopImmediatePropagation();note('Use the R1.4 validated scanner switch below.');
    }
  },true);
  function mount(){
    const host=$('#'+tag);
    if(!host||!location.hash.includes('/token'))return;
    if(host.dataset.guStarted!=='1'){
      host.dataset.guStarted='1';choice=snapshot?.active||'';refresh(true);
    }
    // Old R1 stage action must not be interactable, even by keyboard.
    const catalog=$('[data-native-broker-catalog]');
    if(catalog)catalog.querySelectorAll('button').forEach(b=>{
      if(b.textContent.trim()==='Choose for next restart'){b.disabled=true;b.title='Use R1.4 validated scanner switch below';}
    });
  }
  window.addEventListener('gu:r14-refresh',()=>{if($('#'+tag))refresh(true)});
  const observer=new MutationObserver(mount);
  observer.observe(document.documentElement,{subtree:true,childList:true});
  mount();
})();
