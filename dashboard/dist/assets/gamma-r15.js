/* GAMMA R1.5: collapsed, opt-in read-only account + market-feed qualification.
 * No login token, identity, or credential is displayed or placed in client JS.
 */
(()=>{'use strict';
 const names=['Dhan','Zerodha','Angel','Fyers','Upstox','Nuvama','Kotak','Groww','Shoonya','Flattrade','Definedge'];
 const betaUnverified=new Set(['nuvama','shoonya','flattrade','definedge']);
 const short=(s)=>String(s||'not_verified').replace(/_/g,' ');
 function mount(){
   const place=document.getElementById('gu-r14-panel');
   if(!place || document.getElementById('gu-r15-qualification'))return;
   const panel=document.createElement('details'); panel.id='gu-r15-qualification';
   panel.style.cssText='margin:12px 0 20px;padding:15px 18px;border:1px solid #405064;border-radius:9px;color:#e5eaf5;background:#111923;font:14px/1.55 system-ui,sans-serif';
   const summary=document.createElement('summary');summary.style.cssText='cursor:pointer;font-weight:600';
   summary.textContent='R1.5 · Verify broker session & market feed (read-only)';panel.appendChild(summary);
   const help=document.createElement('p');help.textContent='Checks broker authentication and one lightweight market-data quote (never places orders). Verified brokers unlock validated scanner switching in R1.4 above.';
   help.style.cssText='color:#b4c0d2;margin:12px 0';panel.appendChild(help);
   const list=document.createElement('div');list.style.cssText='display:flex;flex-wrap:wrap;gap:9px';panel.appendChild(list);
   const message=document.createElement('p');message.setAttribute('aria-live','polite');message.textContent='No account verification requested.';message.style.cssText='color:#c7d3ea;margin:12px 0 0';panel.appendChild(message);
   names.forEach((label)=>{
     const broker=label.toLowerCase();const button=document.createElement('button');
     button.type='button';button.style.cssText='background:#233248;border:1px solid #52647b;border-radius:6px;color:#fff;padding:8px 12px;cursor:pointer';
     button.textContent=label+(betaUnverified.has(broker)?' · check (beta unverified)':' · check profile & feed');
     button.addEventListener('click',async()=>{
       button.disabled=true;message.textContent='Read-only profile + market-feed check requested for '+label+'…';
       try{
         const response=await fetch('/api/broker-manager/r15/verify-auth/'+broker,{method:'POST',headers:{'X-Gamma-Control':'1','Content-Type':'application/json'},body:'{}',cache:'no-store',credentials:'same-origin'});
         const data=await response.json();
         if(data.feed_verified&&data.switch_eligible){
           message.textContent=label+': verified (session + market feed OK). Switch unlocked in R1.4 above.';
         }else if(data.feed_verified){
           message.textContent=label+': verified (session + market feed OK). Active scanner unchanged.';
         }else{
           message.textContent=label+': '+short(data.status||data.error)+'. Active scanner unchanged; feed not verified.';
         }
         window.dispatchEvent(new CustomEvent('gu:r14-refresh'));
       }catch(e){message.textContent='Local verification request unavailable. Scanner unchanged.';}
       button.disabled=false;
     });list.appendChild(button);
   });
   place.insertAdjacentElement('afterend',panel);
 }
 const observer=new MutationObserver(mount);
 observer.observe(document.documentElement,{childList:true,subtree:true});
 mount();
})();
