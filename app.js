
const brl = v => new Intl.NumberFormat('pt-BR',{style:'currency',currency:'BRL'}).format(v);
const api = async (url, opts={}) => {
  const r = await fetch(url,{headers:{'Content-Type':'application/json'},...opts});
  return await r.json();
};

async function refresh(){
  const s = await api('/api/status');
  document.querySelector('#balance').textContent = brl(s.balance);
  document.querySelector('#pnl').textContent = `${Number(s.pnl_percent).toFixed(2)}%`;
  document.querySelector('#entries').textContent = s.entries;
  document.querySelector('#status').textContent = s.status;
  const b=document.querySelector('#robotBadge'); b.textContent=s.running?'ATIVO':'PARADO'; b.className='badge '+(s.running?'on':'off');
  renderSignals(s.signals||[]);
  renderHistory(s.history||[]);
  if(s.last_scan) document.querySelector('#lastScan').textContent='Última análise: '+new Date(s.last_scan*1000).toLocaleTimeString('pt-BR');
}

function renderSignals(items){
  const el=document.querySelector('#signals');
  if(!items.length){el.innerHTML='<div class="empty">Nenhum sinal atingiu o score mínimo.</div>';return}
  el.innerHTML=items.map((x,i)=>`
    <div class="signal">
      <div>
        <div class="symbol">${i===0?'🎯 ':''}${x.name} <span class="${x.direction==='CALL'?'call':'put'}">${x.direction==='CALL'?'▲ CALL':'▼ PUT'}</span></div>
        <div class="meta">${x.symbol} • preço ${x.price} • suporte ${x.support.toFixed(5)} • resistência ${x.resistance.toFixed(5)}</div>
        <div class="reasons">${x.reasons.join(' • ')}</div>
      </div>
      <div class="score">${x.score}/12</div>
    </div>`).join('');
}

function renderHistory(items){
  const el=document.querySelector('#history');
  if(!items.length){el.innerHTML='<div class="empty">Nenhuma entrada registrada.</div>';return}
  el.innerHTML=items.map(x=>`<div class="history-row">
    <strong>${x.name}</strong><span class="${x.direction==='CALL'?'call':'put'}">${x.direction}</span>
    <span>Score ${x.score}</span><span>${brl(x.stake)}</span><span>${x.result}</span>
  </div>`).join('');
}

async function scan(){
  document.querySelector('#status').textContent='Analisando...';
  try{ await api('/api/scan',{method:'POST'}); await refresh(); }
  catch(e){ alert('Não foi possível concluir o scanner. Verifique internet e terminal.');}
}
async function startRobot(){await api('/api/start',{method:'POST'});refresh()}
async function stopRobot(){await api('/api/stop',{method:'POST'});refresh()}
async function paperEntry(){
  const r=await api('/api/paper-entry',{method:'POST'});
  if(!r.ok) alert(r.message); refresh();
}

async function saveConfig(e){
  e.preventDefault();
  const body={
    banca_inicial:+banca.value, percentual_entrada:+percent.value,
    stop_gain:+sg.value, stop_loss:+sl.value, max_entradas:+maxe.value,
    min_score:+score.value, duracao_minutos:+dur.value, payout_demo:0.8
  };
  await api('/api/config',{method:'POST',body:JSON.stringify(body)});
  alert('Configurações salvas.'); refresh();
}

if('serviceWorker' in navigator) navigator.serviceWorker.register('/static/sw.js');
refresh();
setInterval(refresh,5000);
