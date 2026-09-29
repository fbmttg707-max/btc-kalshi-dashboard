import asyncio, json, os, time
from collections import deque
from statistics import pstdev
import websockets
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

app=FastAPI(title="BTC/Kalshi Cloud Predictor")
SYMBOL=os.getenv("BINANCE_SYMBOL","btcusdt").lower()
state={"price":None,"buy_flow":50.0,"trades":0,"connected":False,"updated":0}
prices=deque(maxlen=1800)
clients=set()

def pct(a,b):
    return 0 if not a or not b else (a/b-1)*100

def make_signal():
    p=state["price"]; a=list(prices)
    if not p or len(a)<31:
        return {"direction":"WAIT","probability":50,"range_low":None,"range_high":None,"m10":0,"m30":0,"vol":0}
    m10=pct(p,a[-11]); m30=pct(p,a[-31])
    rs=[pct(a[i],a[i-1]) for i in range(1,len(a)) if a[i-1]]
    vol=pstdev(rs[-60:]) if len(rs)>10 else 0
    momentum=max(-1,min(1,(m10*.6+m30*.4)/.30))
    flow=(state["buy_flow"]-50)/50
    raw=(momentum*.65+flow*.35)*(1-min(.45,vol/.5)*.3)
    probability=max(50,min(92,50+42*raw))
    direction="UP" if raw>.12 else "DOWN" if raw<-.12 else "NO TRADE"
    move=max(.00035,vol*.018)
    return {"direction":direction,"probability":round(probability,1),
            "range_low":round(p*(1-move),2),"range_high":round(p*(1+move),2),
            "m10":round(m10,4),"m30":round(m30,4),"vol":round(vol,4)}

async def market_feed():
    url=f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade"
    while True:
        try:
            async with websockets.connect(url,ping_interval=20,ping_timeout=20) as ws:
                state["connected"]=True
                async for raw in ws:
                    d=json.loads(raw); p=float(d["p"])
                    state["price"]=p
                    state["trades"]+=1
                    state["updated"]=time.time()
                    state["buy_flow"]=state["buy_flow"]*.98+(100 if not d["m"] else 0)*.02
                    prices.append(p)
        except Exception:
            state["connected"]=False
            await asyncio.sleep(2)

async def broadcaster():
    while True:
        data={"state":state,"signal":make_signal(),"server_time":time.time()}
        for ws in list(clients):
            try: await ws.send_json(data)
            except Exception: clients.discard(ws)
        await asyncio.sleep(.5)

@app.get("/health")
async def health():
    return {"ok":True,"market_connected":state["connected"],"price":state["price"]}

@app.get("/")
async def home():
    return HTMLResponse(HTML)

@app.websocket("/ws")
async def socket(ws:WebSocket):
    await ws.accept(); clients.add(ws)
    try:
        while True: await ws.receive_text()
    except Exception: clients.discard(ws)

@app.on_event("startup")
async def startup():
    asyncio.create_task(market_feed())
    asyncio.create_task(broadcaster())

HTML=r"""<!doctype html>
<html><head><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>BTC Signal</title>
<style>
:root{--bg:#06080d;--card:#111722;--line:#222b39;--txt:#f6f8fb;--muted:#8995a7;--g:#35e58b;--r:#ff5871;--y:#ffd15c}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);font:15px system-ui,-apple-system,sans-serif}.app{max-width:620px;margin:auto;padding:env(safe-area-inset-top) 14px 24px}
header{display:flex;justify-content:space-between;padding:18px 2px 10px}.logo{font-size:20px;font-weight:900}.status{color:var(--muted);font-size:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:20px;padding:18px;margin:10px 0}.hero{text-align:center;padding:26px 16px}
.k{font-size:11px;color:var(--muted);letter-spacing:.13em;text-transform:uppercase}.price{font-size:42px;font-weight:900;margin:8px 0 18px}
.signal{font-size:52px;font-weight:1000;margin:2px}.up{color:var(--g)}.down{color:var(--r)}.wait{color:var(--y)}.prob{font-size:29px;font-weight:800}.bar{height:9px;background:#252d3a;border-radius:8px;overflow:hidden;margin:13px 0}.fill{height:100%;width:50%;background:var(--g);transition:.25s}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.tile{background:#0b1018;border-radius:14px;padding:13px}.tile span{display:block;color:var(--muted);font-size:11px;text-transform:uppercase}.tile b{font-size:19px;display:block;margin-top:4px}
.note{color:var(--muted);font-size:12px;line-height:1.5}.timer{font-size:34px;font-weight:900}.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--r);margin-right:5px}.ok{background:var(--g)}
</style></head><body><main class="app">
<header><div class="logo">₿ BTC SIGNAL</div><div class="status"><span id="dot" class="dot"></span><span id="feed">OFFLINE</span></div></header>
<section class="card hero"><div class="k">Live BTC / USDT</div><div id="price" class="price">—</div>
<div class="k">Short-horizon signal</div><div id="signal" class="signal wait">WAIT</div><div id="prob" class="prob">50%</div>
<div class="bar"><div id="fill" class="fill"></div></div><div class="k">Predicted short-term range</div><div id="range">—</div></section>
<section class="card"><div class="k">15-minute cycle timer</div><div id="timer" class="timer">--:--</div><p class="note">Cycle timer only. This is not an official Kalshi contract clock/reference feed.</p></section>
<section class="card"><div class="grid">
<div class="tile"><span>Buy flow</span><b id="buy">—</b></div><div class="tile"><span>10s momentum</span><b id="m10">—</b></div>
<div class="tile"><span>30s momentum</span><b id="m30">—</b></div><div class="tile"><span>Volatility</span><b id="vol">—</b></div>
<div class="tile"><span>Trades</span><b id="trades">—</b></div><div class="tile"><span>Server</span><b id="server">—</b></div>
</div></section>
<section class="card"><div class="k">Important</div><p class="note">Analytics/paper mode only. No orders are placed. The model is not guaranteed to predict future BTC prices. For contract-level testing, connect the official settlement/reference methodology and compare every signal with actual outcomes.</p></section>
</main>
<script>
const $=id=>document.getElementById(id);
function money(x){return x==null?'—':Number(x).toLocaleString(undefined,{maximumFractionDigits:2})}
function tick(){let d=new Date(),s=d.getMinutes()*60+d.getSeconds(),r=900-(s%900);$('timer').textContent=String(Math.floor(r/60)).padStart(2,'0')+':'+String(r%60).padStart(2,'0')}
setInterval(tick,250);tick();
function connect(){let w=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');
w.onopen=()=>{$('feed').textContent='LIVE';$('dot').className='dot ok';w.send('hello')};
w.onclose=()=>{ $('feed').textContent='RECONNECTING';$('dot').className='dot';setTimeout(connect,1500)};
w.onmessage=e=>{let d=JSON.parse(e.data),s=d.state,m=d.signal;
$('price').textContent=money(s.price);$('signal').textContent=m.direction;
$('signal').className='signal '+(m.direction==='UP'?'up':m.direction==='DOWN'?'down':'wait');
$('prob').textContent=m.probability+'%';$('fill').style.width=m.probability+'%';
$('fill').style.background=m.direction==='DOWN'?'var(--r)':m.direction==='NO TRADE'?'var(--y)':'var(--g)';
$('range').textContent=m.range_low==null?'—':money(m.range_low)+' — '+money(m.range_high);
$('buy').textContent=s.buy_flow.toFixed(1)+'%';$('m10').textContent=m.m10+'%';$('m30').textContent=m.m30+'%';$('vol').textContent=m.vol;
$('trades').textContent=s.trades.toLocaleString();$('server').textContent=s.connected?'OK':'OFFLINE'};
}connect();
</script></body></html>"""
