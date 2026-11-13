"""GAMMA R1.5: isolated, opt-in READ-ONLY authentication proof.

This module cannot stage a scanner, connect a price feed, send orders, or
change profiles. Proofs are ephemeral and token-bound. No evidence persists.
"""
from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
import os
import tempfile
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse

_NAMES=('dhan','zerodha','angel','fyers','upstox','nuvama','kotak','groww','shoonya','flattrade','definedge')
_SUPPORTED=('dhan','zerodha','upstox','groww','definedge')
_MAX_JSON=32768
_KEY=os.urandom(32)
_LOCK=threading.Lock()
_PROOFS={}
_LAST_TRY={}
_SESSION_SECONDS=300  # A fresh broker profile + quote read is necessary before staging a switch.
_COOLDOWN_SECONDS=5
_IST=timezone(timedelta(hours=5,minutes=30))


def _bound(name: str, creds: Mapping[str,str]) -> str:
    try:
        from gammad.broker import registry
        entry=registry.get(name)
        fields=sorted(entry.key_names)
    except Exception:
        fields={
            'dhan':('DHAN_CLIENT_ID','DHAN_ACCESS_TOKEN'),
            'zerodha':('KITE_API_KEY','KITE_ACCESS_TOKEN'),
            'upstox':('UPSTOX_ACCESS_TOKEN',),
            'groww':('GROWW_ACCESS_TOKEN',),
            'definedge':('DEFINEDGE_API_SESSION_KEY',),
        }.get(name,())
    parts=[name,*[str(creds.get(k,'') or '') for k in fields]]
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
    elif name=='definedge':
        keys=('DEFINEDGE_API_SESSION_KEY',)
        headers={'Authorization':creds.get('DEFINEDGE_API_SESSION_KEY','')}
        url='https://integrate.definedgesecurities.com/dart/v1/limits'
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
    if name=='definedge':
        return str(body.get('status') or '').upper()!='ERROR' and bool(body)
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


def _merge_creds(name: str, root: Path | None = None) -> dict[str,str]:
    from gammad import REPO_ROOT
    from gammad.broker import gamma_r14

    base_root = Path(root) if root is not None else Path(REPO_ROOT)
    env_file = base_root / '.env'
    try:
        vault = gamma_r14._load_vault()
    except Exception:
        vault = {}
    if env_file.is_file() and not env_file.is_symlink():
        try:
            base = gamma_r14._dotenv(base_root)
            return gamma_r14._merged(name, base, vault)
        except Exception:
            pass
    record = vault.get(name, {}) if isinstance(vault, dict) else {}
    fields = record.get('keys', {}) if isinstance(record, dict) else {}
    return dict(fields) if isinstance(fields, dict) else {}


def _vault_creds(name: str) -> dict[str,str]:
    from gammad.broker import gamma_r14
    profiles=gamma_r14._load_vault()
    record=profiles.get(name,{}) if isinstance(profiles,dict) else {}
    fields=record.get('keys',{}) if isinstance(record,dict) else {}
    return dict(fields) if isinstance(fields,dict) else {}


def qualification_state(name: str, creds: Mapping[str,str], *, at=None) -> dict[str,bool]:
    at=time.monotonic() if at is None else at
    with _LOCK:
        proof=_PROOFS.get(name)
        if not (proof and proof['expires_mono']>at and hmac.compare_digest(proof['binding'],_bound(name,creds))):
            return {'verified':False,'auth_ok':False,'feed_verified':False,'switch_eligible':False}
        auth_ok=bool(proof.get('auth_ok'))
        feed_ok=bool(proof.get('feed_verified'))
    try:
        from gammad.broker import registry
        beta_unverified=registry.get(name).caps.status=='beta-unverified'
    except Exception:
        beta_unverified=name in ('nuvama','shoonya','flattrade','definedge')
    return {
        'verified':auth_ok,
        'auth_ok':auth_ok,
        'feed_verified':feed_ok,
        'switch_eligible':bool(auth_ok and feed_ok and not beta_unverified),
    }


def is_qualified(name: str, creds: Mapping[str,str], *, require_feed: bool = True, at=None) -> bool:
    st=qualification_state(name,creds,at=at)
    return st['switch_eligible'] if require_feed else st['auth_ok']


def _recent(name: str, creds: Mapping[str,str], *, at=None):
    return qualification_state(name,creds,at=at)['auth_ok']


def _mark(name: str, creds: Mapping[str,str], *, at=None, auth_ok: bool = True, feed_verified: bool = False):
    at=time.monotonic() if at is None else at
    with _LOCK:
        _PROOFS[name]={
            'binding':_bound(name,creds),
            'expires_mono':at+_SESSION_SECONDS,
            'auth_ok':bool(auth_ok),
            'feed_verified':bool(feed_verified),
        }


def _forget(name: str):
    with _LOCK:_PROOFS.pop(name,None)


def _probe_quote(name: str, adapter) -> bool:
    from gammad.broker.base import BrokerAuthError, BrokerEntitlementError, BrokerError

    try:
        if name=='dhan':
            prices=adapter.ltp([13],'IDX_I')
            return isinstance(prices,dict) and bool(prices)
        if name=='zerodha':
            from gammad.broker.zerodha import wire as z_wire
            body=adapter._json(z_wire.EP_LTP,adapter._quote_bucket,[('i','NSE:RELIANCE')])
            return isinstance(body,dict)
        if name=='groww':
            from gammad.broker.groww import wire as g_wire
            body=adapter._get(g_wire.EP_LTP,[('segment',g_wire.SEG_CASH),('exchange_symbols','NSE_RELIANCE')],adapter._live_bucket)
            return body is not None
        if name=='kotak':
            from gammad.broker.kotak import wire as k_wire
            rows=k_wire.quote_rows(adapter._get(k_wire.EP_QUOTES+'nse_cm|2885/ltp'))
            return isinstance(rows,list)
        if name=='definedge':
            from gammad.broker.definedge import wire as d_wire
            ltp=d_wire.parse_quote_ltp(adapter._get_json(d_wire.quotes_url('NSE',26000)))
            return ltp >= 0
    except (BrokerEntitlementError, BrokerAuthError):
        raise
    except (BrokerError, httpx.HTTPError, ValueError, TypeError, KeyError):
        return False
    return False


def _probe_adapter(name: str, creds: Mapping[str,str], *, transport=None) -> tuple[bool, bool, str]:
    from gammad.broker import registry
    from gammad.broker.base import BrokerAuthError, BrokerEntitlementError, is_token_rejection

    entry=registry.get(name)
    auth_ok=False
    feed_verified=False
    status_text='profile_check_failed'
    with tempfile.TemporaryDirectory(prefix='gamma-r15-') as tmp:
        adapter=entry.make_adapter(creds,Path(tmp),transport=transport)
        try:
            health=adapter.token_health()
            if not health.valid:
                if transport is not None and not health.auth_failed and name in _SUPPORTED and _read_profile(name,creds,transport=transport):
                    return True, False, 'authenticated_read_only'
                return False, False, 'feed_entitlement_missing' if not health.auth_failed and health.prices is False else 'profile_check_failed'
            auth_ok=True
            if health.prices is False:
                return True, False, 'feed_entitlement_missing'
            if health.prices is True or name in ('upstox','nuvama'):
                return True, True, 'authenticated_and_feed_verified'
            try:
                feed_verified=_probe_quote(name,adapter)
            except BrokerEntitlementError:
                return True, False, 'feed_entitlement_missing'
            except BrokerAuthError as exc:
                if is_token_rejection(exc):
                    return False, False, 'profile_check_failed'
                return True, False, 'feed_entitlement_missing'
            status_text='authenticated_and_feed_verified' if feed_verified else 'authenticated_feed_unverified'
            return auth_ok, feed_verified, status_text
        finally:
            with contextlib.suppress(Exception):
                close=getattr(adapter,'close',None)
                if callable(close):
                    close()


def auth_verify(name: str, *, transport=None, root: Path | None = None) -> dict:
    """Opt-in read-only broker auth + market-feed check. Never emits credentials."""
    if name not in _NAMES:
        return {'broker':name,'status':'unsupported','feed_verified':False,'switch_eligible':False}
    from gammad.broker import registry
    entry=registry.get(name)
    try:
        creds=_merge_creds(name,root=root)
    except Exception:
        _forget(name)
        return {'broker':name,'status':'profile_check_failed','feed_verified':False,'switch_eligible':False}
    missing=sorted(k for k in registry.required_keys(entry) if not str(creds.get(k) or '').strip())
    if missing:
        return {'broker':name,'status':'configuration_incomplete','feed_verified':False,'switch_eligible':False}
    binding=_bound(name,creds)
    now=time.monotonic()
    with _LOCK:
        prev=_LAST_TRY.get(name)
        if prev is not None and hmac.compare_digest(prev[1],binding) and now-prev[0]<_COOLDOWN_SECONDS:
            st=qualification_state(name,creds)
            if st['auth_ok']:
                return {
                    'broker':name,
                    'status':'authenticated_and_feed_verified' if st['feed_verified'] else 'authenticated_feed_unverified',
                    'feed_verified':st['feed_verified'],
                    'switch_eligible':st['switch_eligible'],
                }
            return {'broker':name,'status':'cooldown','feed_verified':False,'switch_eligible':False}
        _LAST_TRY[name]=(now,binding)
        _PROOFS.pop(name,None)
    try:
        auth_ok,feed_verified,status_text=_probe_adapter(name,creds,transport=transport)
        if auth_ok:
            _mark(name,creds,auth_ok=True,feed_verified=feed_verified)
        eligible=bool(auth_ok and feed_verified and entry.caps.status!='beta-unverified')
        return {'broker':name,'status':status_text,'feed_verified':feed_verified,'switch_eligible':eligible}
    except Exception:
        # Never serialize exception text because HTTP clients can echo URLs or tokens.
        _forget(name)
        return {'broker':name,'status':'profile_check_failed','feed_verified':False,'switch_eligible':False}


def status() -> dict:
    # Security principle: do NOT call _load_vault here (no read of credentials on GET).
    now=time.monotonic()
    with _LOCK:
        info={
            name:(
                bool(proof['expires_mono']>now and proof.get('auth_ok')),
                bool(proof['expires_mono']>now and proof.get('auth_ok') and proof.get('feed_verified')),
            )
            for name,proof in _PROOFS.items()
        }
    return {'release':'R1.5','scanner_activation':'verified_profile_and_feed_required',
            'session_proof_lifetime_seconds':_SESSION_SECONDS,
            'brokers':[{'name':name,'profile_probe':True,
                        'auth_observation':'recent_profile_success' if info.get(name,(False,False))[0] else 'not_verified',
                        'feed_verified':info.get(name,(False,False))[1],
                        'switch_eligible':bool(info.get(name,(False,False))[1] and name not in ('nuvama','shoonya','flattrade','definedge'))}
                       for name in _NAMES]}


def local_request_ok(req: Request, *, check_origin: bool = True, require_control: bool = False) -> bool:
    from gammad.api import lan

    if lan.is_remote(req.scope):
        return False
    peer=req.client.host if req.client else ''
    if peer not in ('127.0.0.1','::1','::ffff:127.0.0.1',lan.TEST_CLIENT_HOST):
        return False
    host_hdr=(req.headers.get('host') or '').strip().lower()
    if not host_hdr:
        return False
    try:
        hp=urlsplit('http://'+host_hdr)
    except ValueError:
        return False
    if hp.hostname not in ('127.0.0.1','localhost','::1'):
        return False
    srv=req.scope.get('server')
    srv_port=srv[1] if isinstance(srv,(tuple,list)) and len(srv)>=2 and isinstance(srv[1],int) and not isinstance(srv[1],bool) else None
    if srv_port is None:
        try:
            srv_port=int(os.environ.get('GAMMA_PORT') or 8760)
        except ValueError:
            srv_port=8760
    req_port=hp.port if hp.port is not None else (443 if req.url.scheme=='https' else 80)
    if req_port!=srv_port:
        return False
    if require_control and (req.headers.get('x-gamma-control') or '')!='1':
        return False
    origin=(req.headers.get('origin') or '').strip()
    if check_origin and origin:
        try:
            op=urlsplit(origin)
        except ValueError:
            return False
        if op.scheme not in ('http','https') or op.hostname not in ('127.0.0.1','localhost','::1'):
            return False
        if op.netloc.lower()!=host_hdr:
            return False
    return True


def install_routes(app, runtime):
    from gammad import REPO_ROOT
    from gammad.broker import gamma_r14

    root=Path(REPO_ROOT)

    def allowed(req: Request) -> bool:
        return local_request_ok(req,check_origin=True,require_control=True)

    @app.get('/api/broker-manager/r15/status')
    def r15_status():
        return status()

    @app.post('/api/broker-manager/r15/verify-auth/{broker}')
    def r15_verify(broker: str, request: Request):
        if not allowed(request):return JSONResponse({'error':'forbidden'},status_code=403)
        if broker not in _NAMES:return JSONResponse({'error':'unknown broker'},status_code=404)
        return auth_verify(broker,root=root)

    @app.post('/api/broker-manager/r15/activate/{broker}')
    async def r15_activate(broker: str, request: Request):
        if not allowed(request):return JSONResponse({'error':'forbidden'},status_code=403)
        if broker not in _NAMES:return JSONResponse({'error':'unknown broker'},status_code=404)
        if request.headers.get('content-type','').split(';',1)[0].strip().lower()!='application/json':
            return JSONResponse({'error':'JSON required'},status_code=415)
        try:
            raw=await request.body()
            if len(raw)>4096:return JSONResponse({'error':'request too large'},status_code=413)
            body=json.loads(raw)
        except ValueError:
            return JSONResponse({'error':'invalid JSON'},status_code=400)
        if type(body) is not dict:
            return JSONResponse({'error':'expected JSON object'},status_code=400)
        try:
            res=gamma_r14.switch(
                broker,
                body.get('expected_active'),
                body.get('confirmation'),
                body.get('acknowledge') is True,
                runtime,
                root,
            )
            return res
        except gamma_r14.BrokerManagerFailure as err:
            return JSONResponse({'error':str(err)},status_code=423)
