"""GAMMA v0.8.9 client: independent broker profiles and guarded scanner handoff.

Security invariants: no secret in a GET response/log; no inactive profile changes
runtime adapter; DPAPI under current Windows user; broker exchange results remain in
DPAPI until an explicitly confirmed and restart-bound switch stages them in .env.
"""
from __future__ import annotations

import ctypes
from ctypes import wintypes
import json
import os
from pathlib import Path
import re
import secrets
import tempfile
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import Request
from fastapi.responses import JSONResponse

_LOCK = threading.RLock()
_IDS = ('dhan','zerodha','angel','fyers','upstox','nuvama','kotak','groww','shoonya','flattrade')
_CTRL = 'X-Gamma-Control'
_CONFIRM = re.compile(r'^SWITCH TO ([A-Z][A-Z0-9_-]{1,24})$')
_SAFE_VALUE = re.compile(r'^[^\r\n\x00]{1,8192}$')

class BrokerManagerFailure(Exception):
    pass


def _protect(data: bytes) -> bytes:
    if os.name != 'nt':
        raise BrokerManagerFailure('Windows DPAPI required; no plaintext fallback permitted')
    class BLOB(ctypes.Structure):
        _fields_=[('size', wintypes.DWORD),('data',ctypes.POINTER(ctypes.c_byte))]
    crypt=ctypes.WinDLL('crypt32',use_last_error=True)
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    f=crypt.CryptProtectData
    f.argtypes=[ctypes.POINTER(BLOB),wintypes.LPCWSTR,ctypes.POINTER(BLOB),ctypes.c_void_p,ctypes.c_void_p,wintypes.DWORD,ctypes.POINTER(BLOB)]
    f.restype=wintypes.BOOL
    kernel.LocalFree.argtypes=[ctypes.c_void_p]
    kernel.LocalFree.restype=ctypes.c_void_p
    raw=(ctypes.c_byte*len(data)).from_buffer_copy(data)
    src=BLOB(len(data),ctypes.cast(raw,ctypes.POINTER(ctypes.c_byte)))
    out=BLOB()
    if not f(ctypes.byref(src),'GAMMA R1.4 broker profiles',None,None,None,0x1,ctypes.byref(out)):
        raise BrokerManagerFailure('Windows user encryption unavailable')
    try:
        return ctypes.string_at(out.data,out.size)
    finally:
        kernel.LocalFree(out.data)


def _unprotect(data: bytes) -> bytes:
    if os.name != 'nt':
        raise BrokerManagerFailure('Windows DPAPI required; no plaintext fallback permitted')
    class BLOB(ctypes.Structure):
        _fields_=[('size',wintypes.DWORD),('data',ctypes.POINTER(ctypes.c_byte))]
    crypt=ctypes.WinDLL('crypt32',use_last_error=True)
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    f=crypt.CryptUnprotectData
    f.argtypes=[ctypes.POINTER(BLOB),ctypes.POINTER(wintypes.LPWSTR),ctypes.POINTER(BLOB),ctypes.c_void_p,ctypes.c_void_p,wintypes.DWORD,ctypes.POINTER(BLOB)]
    f.restype=wintypes.BOOL
    kernel.LocalFree.argtypes=[ctypes.c_void_p]
    kernel.LocalFree.restype=ctypes.c_void_p
    raw=(ctypes.c_byte*len(data)).from_buffer_copy(data)
    src=BLOB(len(data),ctypes.cast(raw,ctypes.POINTER(ctypes.c_byte)))
    out=BLOB()
    if not f(ctypes.byref(src),None,None,None,None,0,ctypes.byref(out)):
        raise BrokerManagerFailure('Broker profile cannot be decrypted for this Windows user')
    try:
        return ctypes.string_at(out.data,out.size)
    finally:
        kernel.LocalFree(out.data)


def _vault_path() -> Path:
    home=os.environ.get('LOCALAPPDATA','')
    if not home:
        raise BrokerManagerFailure('LOCALAPPDATA unavailable; broker profiles not persisted')
    return Path(home)/'GAMMA-UNWIND'/'broker-profiles-r14.dpapi'


def _atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    fd,name=tempfile.mkstemp(prefix='.gamma-r14-',dir=path.parent)
    try:
        with os.fdopen(fd,'wb') as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name): os.unlink(name)


def _load_vault() -> dict:
    p=_vault_path()
    if not p.exists(): return {}
    if p.is_symlink(): raise BrokerManagerFailure('Invalid broker-profile path')
    raw=json.loads(_unprotect(p.read_bytes()).decode('utf-8'))
    if type(raw) is not dict or any(k not in _IDS or type(v) is not dict for k,v in raw.items()):
        raise BrokerManagerFailure('Invalid broker profile schema')
    return raw


def _save_vault(data: dict) -> None:
    _atomic(_vault_path(),_protect(json.dumps(data,separators=(',',':')).encode()))


def _catalog():
    from gammad.broker import registry
    return registry


def _dotenv(root: Path) -> dict[str,str]:
    from dotenv import dotenv_values
    f=root/'.env'
    if not f.is_file() or f.is_symlink():
        raise BrokerManagerFailure('Expected root .env missing')
    return {str(k):(v or '') for k,v in dotenv_values(f).items() if k is not None}


def _merged(name: str, base: dict, vault: dict) -> dict[str,str]:
    reg=_catalog(); entry=reg.get(name)
    result={key:(base.get(key) or '') for key in entry.key_names}
    result.update({k:v for k,v in vault.get(name,{}).get('keys',{}).items() if k in entry.key_names})
    return result


def _profile_status(name: str,active: str,base:dict,vault:dict,login_state:object):
    reg=_catalog(); entry=reg.get(name)
    creds=_merged(name,base,vault)
    login=set(reg.login_keys(entry))
    checks=[]
    for key in entry.env_keys:
        checks.append({'key':key.name,'present':bool((creds.get(key.name) or '').strip()),
                       'category':('token' if key.name==entry.token_key else 'login' if key.name in login else 'required' if key.name in reg.required_keys(entry) else 'optional')})
    needed={k for k in reg.required_keys(entry) if k!=entry.token_key}
    missing=sorted(k for k in needed if not (creds.get(k) or '').strip())
    token_present=bool((creds.get(entry.token_key) or '').strip())
    state='setup_incomplete' if missing else 'token_missing' if not token_present else 'configured_not_verified'
    if name==active:
        state='login_ok' if login_state is True else 'login_required' if login_state is False else 'active_login_unverified'
    elif vault.get(name,{}).get('exchange_completed') and token_present:
        state='exchange_completed_not_feed_verified'
    method='redirect_code' if entry.client_login is not None else 'manual_token_only'
    return {'id':name,'label':entry.label,'active':name==active,'adapter_status':entry.caps.status,
            'state':state,'fields':checks,'missing_setup':missing,'token_present':token_present,
            'login_method':method,'login_fields':list(entry.client_login.fields) if entry.client_login is not None else [],
            'feed_verified':False,'can_stage':False,'switch_reason':'Scanner switching requires independently validated authentication and market-data feed',
            'last_exchange_utc':vault.get(name,{}).get('exchange_utc') if vault.get(name,{}).get('exchange_completed') else None}


def _active(runtime,base):
    # Active adapter is in memory until process restart; .env BROKER is a staged target.
    entry=getattr(runtime,'broker_entry',None)
    return entry.name if entry is not None else str(base.get('BROKER') or '').lower()


def snapshot(runtime,root:Path):
    with _LOCK:
        base=_dotenv(root); vault=_load_vault(); active=_active(runtime,base)
        try:
            s=runtime.token_status(); valid=s.get('valid') if type(s) is dict else None
            if type(valid) is not bool: valid=None
        except Exception: valid=None
        return {'active':active,'staged':str(base.get('BROKER') or '').lower(),
                'restart_pending':active != str(base.get('BROKER') or '').lower(),
                'brokers':[_profile_status(i,active,base,vault,valid) for i in _IDS],
                'notice':'Credential presence is not authentication or feed verification. An exchange is not proof of live feed.'}


def save_profile(name:str,fields:dict,root:Path) -> dict:
    with _LOCK:
        if name not in _IDS or type(fields) is not dict or not fields or len(fields)>24:
            raise BrokerManagerFailure('Invalid broker or field set')
        reg=_catalog(); entry=reg.get(name)
        keys=set(entry.key_names)
        if any(k not in keys or type(v) is not str or not _SAFE_VALUE.fullmatch(v) for k,v in fields.items()):
            raise BrokerManagerFailure('Invalid broker credential field')
        data=_load_vault(); p=data.setdefault(name,{'keys':{}})
        p.setdefault('keys',{}).update(fields)
        p.pop('exchange_completed',None);p.pop('exchange_utc',None)
        _save_vault(data)
        return {'ok':True,'broker':name,'saved_field_names':sorted(fields)}


def disconnect_profile(name:str,runtime,root:Path)->dict:
    with _LOCK:
        if name not in _IDS or name==_active(runtime,_dotenv(root)):
            raise BrokerManagerFailure('Active scanner cannot be disconnected from this panel')
        data=_load_vault()
        if name in data:
            data.pop(name)
            _save_vault(data)
        return {'ok':True,'broker':name,'profile_cleared':True,
                'warning':'An older token in legacy .env, if present, is not deleted automatically.'}


def _login_url(name:str,root:Path):
    with _LOCK:
        reg=_catalog();entry=reg.get(name)
        if not entry.client_login: return None
        creds=_merged(name,_dotenv(root),_load_vault())
        try:
            url=entry.client_login.login_url(creds)
        except Exception: return None
        if not isinstance(url,str):return None
        u=urlparse(url)
        if u.scheme!='https' or not u.hostname or u.username or u.password:return None
        # Strict domain allowlist, only broker-hosted authorization pages.
        allow={'fyers':('fyers.in',),'zerodha':('zerodha.com','kite.trade'),
               'upstox':('upstox.com',),'angel':('angelone.in',),'nuvama':('nuvamawealth.com',),
               'kotak':('kotaksecurities.com',),'groww':('groww.in',)}
        domains=allow.get(name,())
        if not any(u.hostname==d or u.hostname.endswith('.'+d) for d in domains):return None
        return url


def exchange_login(name:str,fields:dict,runtime,root:Path) -> dict:
    with _LOCK:
        reg=_catalog()
        if name not in _IDS or name==_active(runtime,_dotenv(root)):
            raise BrokerManagerFailure('Use original daily-login form for active broker')
        entry=reg.get(name)
        if not entry.client_login:raise BrokerManagerFailure('This broker has manual token setup only')
        expected=set(entry.client_login.fields)
        if type(fields) is not dict or set(fields)!=expected or any(type(v) is not str or not _SAFE_VALUE.fullmatch(v) for v in fields.values()):
            raise BrokerManagerFailure('Expected broker login field(s) missing')
        data=_load_vault();creds=_merged(name,_dotenv(root),data)
        needed=reg.login_keys(entry)
        if any(not (creds.get(k) or '').strip() for k in needed):
            raise BrokerManagerFailure('Complete broker app configuration before login')
        # Existing per-broker client edition handler, not owner-only auto-login.
        try:
            result=entry.client_login.login(creds,fields)
        except Exception:
            # Keep user redirects and secrets out of errors and logs.
            raise BrokerManagerFailure('Broker login exchange refused or unavailable') from None
        if type(result) is not dict or not result.get(entry.token_key):
            raise BrokerManagerFailure('Broker exchange returned no usable access token')
        changes={k:v for k,v in result.items() if k in entry.key_names and isinstance(v,str) and _SAFE_VALUE.fullmatch(v)}
        if entry.token_key not in changes:raise BrokerManagerFailure('Broker access token not available')
        profile=data.setdefault(name,{'keys':{}})
        profile.setdefault('keys',{}).update(changes)
        profile['exchange_completed']=True
        profile['exchange_utc']=datetime.now(timezone.utc).isoformat(timespec='seconds')
        _save_vault(data)
        return {'ok':True,'broker':name,'state':'exchange_completed_not_feed_verified'}


def _env_stage_text(original: bytes,selected:str,updates:dict)->bytes:
    # Existing legacy client needs active broker's credentials in .env on restart.
    # Preserve encoding and line endings; reject ambiguity, multiline injection, and duplicate keys.
    try: text=original.decode('utf-8-sig')
    except UnicodeDecodeError: raise BrokerManagerFailure('Unsupported .env encoding') from None
    enc='utf-8-sig' if original.startswith(b'\xef\xbb\xbf') else 'utf-8'
    sep='\r\n' if '\r\n' in text else '\n'
    lines=text.splitlines(keepends=True)
    def setline(key,val):
        if type(val) is not str or not _SAFE_VALUE.fullmatch(val) or val.strip()!=val:
            raise BrokerManagerFailure('Invalid staged configuration value')
        pat=re.compile(r'^\s*'+re.escape(key)+r'\s*=')
        idx=[i for i,line in enumerate(lines) if pat.match(line)]
        if len(idx)>1:raise BrokerManagerFailure('Ambiguous duplicate .env field')
        line=key+'='+json.dumps(val,ensure_ascii=False)+sep
        if idx: lines[idx[0]]=line
        else:
            if lines and not lines[-1].endswith(('\n','\r')):lines.append(sep)
            lines.append(line)
    setline('BROKER',selected)
    for k,v in sorted(updates.items()):setline(k,v)
    return ''.join(lines).encode(enc)


def _scanner_activation_verified(name: str) -> bool:
    # Disabled by design until an adapter can prove both authentication and feed.
    # Do not infer readiness from token presence or a successful OAuth exchange.
    return False

def switch(name:str,expected:str,confirmation:str,ack:bool,runtime,root:Path)->dict:
    with _LOCK:
        base=_dotenv(root); active=_active(runtime,base)
        if name not in _IDS or expected!=active or base.get('BROKER','').strip().lower()!=active:
            raise BrokerManagerFailure('Active broker changed or restart already pending')
        if name==active:raise BrokerManagerFailure('Requested broker is already active')
        if confirmation != 'SWITCH TO '+name.upper():
            raise BrokerManagerFailure('Type exact broker-switch confirmation')
        if ack is not True:raise BrokerManagerFailure('Acknowledgement of restart and unverified live feed is required')
        reg=_catalog();entry=reg.get(name)
        if entry.caps.status=='beta-unverified':
            raise BrokerManagerFailure('Unverified broker adapter cannot be selected for scanning')
        # R1.4.1: fail closed until an independent authenticated/feed check exists.
        if not _scanner_activation_verified(name):
            raise BrokerManagerFailure('Scanner switching disabled until broker authentication and data-feed checks are implemented')
        data=_load_vault();creds=_merged(name,base,data)
        need=reg.required_keys(entry)
        missing=[k for k in need if not (creds.get(k) or '').strip()]
        if missing:raise BrokerManagerFailure('Target broker configuration incomplete: '+', '.join(missing))
        p=root/'.env'
        if p.is_symlink():raise BrokerManagerFailure('Unsafe .env path')
        before=p.read_bytes()
        after=_env_stage_text(before,name,{k:creds[k] for k in entry.key_names if creds.get(k)})
        # Snapshot of sensitive .env is stored in the same private directory (legacy compatibility).
        # Every switch requires explicit confirmation; no app restart occurs in this endpoint.
        stamp=datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        backup=root/('.env.before-r14-'+stamp+'.bak')
        if backup.exists():raise BrokerManagerFailure('Backup name conflict')
        _atomic(backup,before)
        try: _atomic(p,after)
        except Exception:
            _atomic(p,before)
            raise BrokerManagerFailure('Could not stage broker configuration') from None
        return {'ok':True,'staged':name,'running':active,'restart_required':True,'backup_name':backup.name,
                'warning':'Legacy .env holds active credentials. Backup and .env must remain private.'}


def install_routes(app,runtime):
    """Called before original SPA catch-all. Keeps old API routes available except unsafe stage."""
    from gammad import REPO_ROOT
    root=Path(REPO_ROOT)
    async def guarded(req:Request):
        if req.headers.get(_CTRL)!='1':
            return JSONResponse({'error':'control header required'},status_code=403)
        if req.headers.get('content-type','').split(';')[0].strip().lower()!='application/json':
            return JSONResponse({'error':'JSON required'},status_code=415)
        if req.headers.get('origin'):
            host=req.headers.get('host','')
            if req.headers['origin'] not in ('http://'+host,'https://'+host):
                return JSONResponse({'error':'same origin required'},status_code=403)
        return None
    async def safe_body(req):
        raw=await req.body()
        if len(raw)>24000:raise BrokerManagerFailure('Request too large')
        try: body=json.loads(raw)
        except ValueError:raise BrokerManagerFailure('Invalid JSON') from None
        if type(body) is not dict:raise BrokerManagerFailure('Expected JSON object')
        return body
    async def execute(req,fn):
        rejection=await guarded(req)
        if rejection:return rejection
        try: return await fn(await safe_body(req))
        except BrokerManagerFailure as err:
            # Err strings authored above contain names, not secret values.
            return JSONResponse({'error':str(err)},status_code=409)
        except Exception:
            return JSONResponse({'error':'Broker manager operation failed (no credentials included)'},status_code=500)
    @app.middleware('http')
    async def block_legacy_stage(req:Request,call_next):
        # R1.4.1: local-only manager. Reject DNS rebinding and direct LAN access.
        if req.url.path.startswith('/api/broker-manager/'):
            supplied_host=(req.headers.get('host') or '').lower()
            peer=req.client.host if req.client else ''
            if (supplied_host not in ('127.0.0.1:8760','localhost:8760','[::1]:8760')
                or peer not in ('127.0.0.1','::1','::ffff:127.0.0.1')):
                return JSONResponse({'error':'local broker manager only'},status_code=403)
        if req.url.path.rstrip('/')=='/api/brokers/stage' and req.method.upper()=='POST':
            return JSONResponse({'error':'Legacy unvalidated switch disabled by R1.4; use Broker Manager'},status_code=409)
        return await call_next(req)
    @app.get('/api/broker-manager/status')
    def get_status():
        try: return JSONResponse(snapshot(runtime,root),headers={'Cache-Control':'no-store'})
        except Exception:return JSONResponse({'error':'Broker readiness unavailable'},status_code=503)
    @app.get('/api/broker-manager/login-url/{name}')
    def get_login_url(name:str):
        if name not in _IDS:return JSONResponse({'error':'Unknown broker'},status_code=404)
        return JSONResponse({'url':_login_url(name,root)},headers={'Cache-Control':'no-store'})
    @app.post('/api/broker-manager/profile/{name}')
    async def put_profile(name:str,req:Request):
        if name not in _IDS:return JSONResponse({'error':'Unknown broker'},status_code=404)
        async def run(data):
            if name==_active(runtime,_dotenv(root)):
                raise BrokerManagerFailure('Use the original daily login for the active scanner broker')
            return save_profile(name,data.get('fields'),root)
        return await execute(req,run)
    @app.post('/api/broker-manager/disconnect/{name}')
    async def post_disconnect(name:str,req:Request):
        if name not in _IDS:return JSONResponse({'error':'Unknown broker'},status_code=404)
        async def run(data):
            if data.get('confirmation')!='DISCONNECT '+name.upper():
                raise BrokerManagerFailure('Type exact disconnect confirmation')
            return disconnect_profile(name,runtime,root)
        return await execute(req,run)
    @app.post('/api/broker-manager/login/{name}')
    async def post_login(name:str,req:Request):
        if name not in _IDS:return JSONResponse({'error':'Unknown broker'},status_code=404)
        from starlette.concurrency import run_in_threadpool
        async def run(data):return await run_in_threadpool(exchange_login,name,data.get('fields'),runtime,root)
        return await execute(req,run)
    @app.post('/api/broker-manager/switch/{name}')
    async def post_switch(name:str,req:Request):
        if name not in _IDS:return JSONResponse({'error':'Unknown broker'},status_code=404)
        async def run(data):return switch(name,data.get('expected_active'),data.get('confirmation'),data.get('acknowledge') is True,runtime,root)
        return await execute(req,run)


# GAMMA R1.5: additive read-only broker account qualification; existing route installation preserved.
_R15_original_install_routes = install_routes
def install_routes(app, runtime):
    _R15_original_install_routes(app, runtime)
    from gammad.broker.gamma_r15 import install_routes as _r15_install
    _r15_install(app, runtime)
