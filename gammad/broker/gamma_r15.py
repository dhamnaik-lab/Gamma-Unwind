"""GAMMA R1.5: isolated, opt-in READ-ONLY authentication proof.

This module cannot stage a scanner, connect a price feed, send orders, or
change profiles. Proofs are ephemeral and token-bound. No evidence persists.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Mapping

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse

_NAMES=('dhan','zerodha','angel','fyers','upstox','nuvama','kotak','groww','shoonya','flattrade')
_SUPPORTED=('dhan','zerodha','upstox','groww')
_MAX_JSON=32768
_KEY=os.urandom(32)
_LOCK=threading.Lock()
_PROOFS={}
_LAST_TRY={}
_SESSION_SECONDS=120  # A fresh broker profile read is necessary before later actions.
_COOLDOWN_SECONDS=45
_IST=timezone(timedelta(hours=5,minutes=30))


def _bound(name: str, creds: Mapping[str,str]) -> str:
    fields={'dhan':('DHAN_CLIENT_ID','DHAN_ACCESS_TOKEN'),
            'zerodha':('KITE_API_KEY','KITE_ACCESS_TOKEN'),
            'upstox':('UPSTOX_ACCESS_TOKEN',),
            'groww':('GROWW_ACCESS_TOKEN',)}[name]
    parts=[name,*[creds.get(k,'') for k in fields]]
    return hmac.new(_KEY,'\0'.join(parts).encode(),hashlib.sha256).hexdigest()


def _setup(name: str, creds: Mapping[str,str]):
    if name=='dhan':
        keys=('DHAN_CLIENT_ID','DHAN_ACCESS_TOKEN')
        headers={'access-token':creds.get('DHAN_ACCESS_TOKEN','')}
        url='https://api.dhan.co/v2/profile'
    elif name=='zerodha':
        keys=('KITE_API_KEY','KITE_ACCESS_TOKEN')
        headers={'X-Kite-Version':'3','Authorization':'token '+creds.get('KITE_API_KEY','')+':'+creds.get('KITE_ACCESS_TOKEN','')}
        url='https://api.kite.trade/user/profile'
    elif name=='upstox':
        keys=('UPSTOX_ACCESS_TOKEN',)
        headers={'Authorization':'Bearer '+creds.get('UPSTOX_ACCESS_TOKEN','')}
        url='https://api.upstox.com/v2/user/profile'
    elif name=='groww':
        keys=('GROWW_ACCESS_TOKEN',)
        headers={'Authorization':'Bearer '+creds.get('GROWW_ACCESS_TOKEN',''),'X-API-VERSION':'1.0'}
        url='https://api.groww.in/v1/user/detail'
    else:
        return None, (), {}, None
    missing=[k for k in keys if not isinstance(creds.get(k),str) or not creds[k].strip()]
    return url, missing, headers, keys


def _validate(name: str, body: object, creds: Mapping[str,str], now: datetime) -> bool:
    """Validate known successful broker schema and session-specific details, never return identity."""
    if not isinstance(body,dict):return False
    if name=='dhan':
        if str(body.get('dhanClientId') or '')!=creds.get('DHAN_CLIENT_ID'):return False
        try:
            expires=datetime.strptime(body['tokenValidity'],'%d/%m/%Y %H:%M').replace(tzinfo=_IST)
            return expires>now.astimezone(_IST)
        except (ValueError,TypeError,KeyError):return False
    if name=='zerodha':
        d=body.get('data')
        # Zerodha's GET /user/profile intentionally does not return api_key.
        # The request authenticates (api_key, access_token) over allowlisted HTTPS.
        # _bound() additionally ties short-lived proof to those exact credentials.
        return (body.get('status')=='success' and isinstance(d,dict) and
                bool(d.get('user_id')) and d.get('broker')=='ZERODHA')
    if name=='upstox':
        d=body.get('data')
        return (body.get('status')=='success' and isinstance(d,dict) and
                bool(d.get('user_id')) and d.get('is_active') is True and
                str(d.get('broker','')).upper()=='UPSTOX')
    if name=='groww':
        d=body.get('payload')
        return body.get('status')=='SUCCESS' and isinstance(d,dict) and bool(d.get('ucc'))
    return False


def _read_profile(name: str, creds: Mapping[str,str], transport=None) -> bool:
    url, missing, headers, _ = _setup(name,creds)
    if not url or missing:return False
    try:
        # Explicit allowlisted HTTPS URL only. No redirects, no ambient proxy variables.
        with httpx.Client(timeout=5,follow_redirects=False,trust_env=False,transport=transport) as client:
            with client.stream('GET',url,headers={**headers,'Accept':'application/json'}) as response:
                if response.status_code!=200:return False
                size=0
                chunks=[]
                for chunk in response.iter_bytes():
                    size+=len(chunk)
                    if size>_MAX_JSON:return False
                    chunks.append(chunk)
                body=json.loads(b''.join(chunks))
        return _validate(name,body,creds,datetime.now(timezone.utc))
    except (httpx.HTTPError,ValueError,UnicodeDecodeError,TypeError,KeyError):
        return False


def _vault_creds(name: str) -> dict[str,str]:
    # Import is deliberately inside the operation: never load credentials for
    # GET status, and never load the active FYERS legacy .env.
    from gammad.broker import gamma_r14
    profiles=gamma_r14._load_vault()
    record=profiles.get(name,{}) if isinstance(profiles,dict) else {}
    fields=record.get('keys',{}) if isinstance(record,dict) else {}
    return dict(fields) if isinstance(fields,dict) else {}


def _recent(name: str, creds: Mapping[str,str], *, at=None):
    at=time.monotonic() if at is None else at
    with _LOCK:
        proof=_PROOFS.get(name)
        if proof and proof['expires_mono']>at and hmac.compare_digest(proof['binding'],_bound(name,creds)):
            return True
    return False


def _mark(name: str, creds: Mapping[str,str], *, at=None):
    at=time.monotonic() if at is None else at
    with _LOCK:
        _PROOFS[name]={'binding':_bound(name,creds),'expires_mono':at+_SESSION_SECONDS}


def _forget(name: str):
    with _LOCK:_PROOFS.pop(name,None)


def auth_verify(name: str, *, transport=None) -> dict:
    """Opt-in broker GET profile. Never called on page load; never emits credentials."""
    if name not in _SUPPORTED:return {'broker':name,'status':'unsupported'}
    if name=='fyers':return {'broker':name,'status':'active_scanner_not_touched'}
    now=time.monotonic()
    with _LOCK:
        if now-_LAST_TRY.get(name,-1e9)<_COOLDOWN_SECONDS:
            return {'broker':name,'status':'cooldown'}
        _LAST_TRY[name]=now
        _PROOFS.pop(name,None)
    try:
        creds=_vault_creds(name)
        _,missing,_,_=_setup(name,creds)
        if missing:return {'broker':name,'status':'configuration_incomplete'}
        ok=_read_profile(name,creds,transport=transport)
        if ok:_mark(name,creds)
        return {'broker':name,'status':'authenticated_read_only' if ok else 'profile_check_failed',
                'feed_verified':False,'switch_eligible':False}
    except Exception:
        # Never serialize exception text because HTTP clients can echo URLs or tokens.
        _forget(name)
        return {'broker':name,'status':'profile_check_failed',
                'feed_verified':False,'switch_eligible':False}


def status() -> dict:
    # Security principle: do NOT call _load_vault here (no read of credentials on GET).
    # Recent proof is only an observation; another broker login may rotate the token.
    with _LOCK:
        recent={name:(proof['expires_mono']>time.monotonic()) for name,proof in _PROOFS.items()}
    return {'release':'R1.5','scanner_activation':'disabled_until_independent_feed_qualification',
            'session_proof_lifetime_seconds':_SESSION_SECONDS,
            'brokers':[{'name':name,'profile_probe':name in _SUPPORTED,
                        'auth_observation':'recent_profile_success' if recent.get(name,False) else 'not_verified',
                        'feed_verified':False,'switch_eligible':False} for name in _NAMES]}


def install_routes(app, runtime):
    # All paths share R1.4.1's trusted-host/peer middleware, plus local controls.
    def allowed(req: Request) -> bool:
        if (req.headers.get('x-gamma-control') or '')!='1':return False
        origin=req.headers.get('origin')
        if origin and origin.lower() not in ('http://127.0.0.1:8760','http://localhost:8760','http://[::1]:8760'):
            return False
        host=(req.headers.get('host') or '').lower()
        peer=req.client.host if req.client else ''
        return host in ('127.0.0.1:8760','localhost:8760','[::1]:8760') and peer in ('127.0.0.1','::1','::ffff:127.0.0.1')

    @app.get('/api/broker-manager/r15/status')
    def r15_status():
        return status()

    @app.post('/api/broker-manager/r15/verify-auth/{broker}')
    def r15_verify(broker: str, request: Request):
        if not allowed(request):return JSONResponse({'error':'forbidden'},status_code=403)
        if broker not in _NAMES:return JSONResponse({'error':'unknown broker'},status_code=404)
        if broker=='fyers':return JSONResponse({'error':'active scanner unchanged'},status_code=409)
        return auth_verify(broker)

    @app.post('/api/broker-manager/r15/activate/{broker}')
    def r15_activate(broker: str, request: Request):
        # Deliberately incapable of modifying .env or invoking R1.4 staging.
        return JSONResponse({'error':'scanner activation requires verified real-time feed and controlled restart'},status_code=423)
