const money=(v,c='USD')=>new Intl.NumberFormat('pt-BR',{style:'currency',currency:c||'USD'}).format(Number(v||0));
const api=async(url,opts={})=>{const r=await fetch(url,{headers:{'Content-Type':'application/json'},cache:'no-store',...opts});let d={};try{d=await r.json()}catch{d={detail:`HTTP ${r.status}`}}if(!r.ok)throw new Error(typeof d.detail==='string'?d.detail:JSON.stringify(d.detail||d));return d};

let accountCurrency='USD',accountType='',scanBusy=false,derivBusy=false,lastConfig=null;

const el=id=>document.getElementById(id);
const get=(...ids)=>ids.map(el).find(Boolean);

function setVal(ids,value){const x=get(...ids);if(x&&document.activeElement!==x)x.value=value}
function setChecked(ids,value){const x=get(...ids);if(x)x.checked=!!value}
function valueOf(ids,fallback=''){const x=get(...ids);return x?x.value:fallback}
function checkedOf(ids,fallback=false){const x=get(...ids);return x?!!x.checked:fallback}

function syncEntryUI(){
 const tipo=valueOf(['entradaTipo','entryType'],'percentual');
 const p=get('percent','percentualEntrada'),v=get('valorEntrada','entryValue');
 if(p)p.disabled=tipo==='valor';
 if(v)v.disabled=tipo!=='valor';
 const pl=get('percentWrap'),vl=get('valorEntradaWrap','entryValueWrap');
 if(pl)pl.style.display=tipo==='valor'?'none':'';
 if(vl)vl.style.display=tipo==='valor'?'':'none';
}

async function refresh(){
 try{
  const s=await api('/api/status');
  accountCurrency=s.currency||accountCurrency||'USD';
  balance.textContent=money(s.balance,accountCurrency);
  pnl.textContent=`${Number(s.pnl_percent||0).toFixed(2)}%`;
  entries.textContent=s.entries||0;
  status.textContent=s.status||'—';

  robotBadge.textContent=s.scanning?'ESCANEANDO':(s.open_trade?'OPERANDO':(s.running?'ATIVO':'PARADO'));
  robotBadge.className='badge '+((s.running||s.scanning||s.open_trade)?'on':'off');

  renderSignals(s.signals||[]);
  renderHistory(s.history||[]);
  renderOpen(s.open_trade);
  renderNews(s.news||[]);

  if(s.last_scan)lastScan.textContent='Última análise: '+new Date(s.last_scan*1000).toLocaleTimeString('pt-BR');

  const c=s.config||{};
  lastConfig=c;
  setVal(['banca'],Number(s.start_balance??c.banca_inicial??s.balance??0));
  setVal(['percent','percentualEntrada'],c.percentual_entrada??1);
  setVal(['entradaTipo','entryType'],c.entrada_tipo||'percentual');
  setVal(['valorEntrada','entryValue'],c.valor_entrada??1);
  setVal(['sg'],c.stop_gain??5);
  setVal(['sl'],c.stop_loss??5);
  setVal(['maxe'],c.max_entradas??5);
  setVal(['score'],c.min_score??7);
  setVal(['dur'],c.duracao_minutos??1);
  setChecked(['operacaoAutomatica','autoTrade'],c.operacao_automatica);
  syncEntryUI();

  const autoLabel=get('autoTradeStatus');
  if(autoLabel)autoLabel.textContent=c.operacao_automatica?'Operação automática: LIGADA':'Operação automática: DESLIGADA';
 }catch(e){status.textContent='Falha de atualização: '+e.message}
}

function renderSignals(items){
 if(!items.length){signals.innerHTML='<div class="empty">Nenhum sinal confirmado pela análise-base.</div>';return}
 signals.innerHTML=items.map((x,i)=>`<div class="signal"><div><div class="symbol">${i===0?'🎯 ':''}${x.name} <span class="${x.direction==='CALL'?'call':'put'}">${x.direction==='CALL'?'▲ COMPRA':'▼ VENDA'}</span></div><div class="meta">${x.symbol} • preço ${x.price} • 110 candles fechados em M1/M5/M15</div><div class="meta"><strong>${x.analysis||''}</strong> • Gatilho: ${x.confirmation||''}</div><div class="meta">Suporte forte ${Number(x.support||0).toFixed(5)} • Resistência forte ${Number(x.resistance||0).toFixed(5)}</div><div class="reasons">${(x.reasons||[]).join(' • ')}</div></div><div class="score">${x.score}/10</div></div>`).join('')
}

function renderOpen(x){
 if(!x){openTrade.className='empty';openTrade.innerHTML='Nenhuma operação aberta.';return}
 const elapsed=Math.max(0,Math.floor(Date.now()/1000-Number(x.time||0))),total=Number(x.duration_minutes||1)*60,left=Math.max(0,total-elapsed);
 openTrade.className='open-trade';
 openTrade.innerHTML=`<div><small>ATIVO</small><strong>${x.name}</strong><span>${x.symbol}</span></div><div><small>DIREÇÃO</small><strong class="${x.direction==='CALL'?'call':'put'}">${x.side||(x.direction==='CALL'?'COMPRA':'VENDA')}</strong><span>${x.mode||''}${x.automatic?' • AUTO':' • MANUAL'}</span></div><div><small>VALOR</small><strong>${money(x.stake,accountCurrency)}</strong><span>Score ${x.score}</span></div><div><small>P/L ATUAL</small><strong class="${Number(x.profit)>=0?'call':'put'}">${money(x.profit,accountCurrency)}</strong><span>${x.status||'open'}</span></div><div><small>TEMPO</small><strong>${Math.floor(left/60)}:${String(left%60).padStart(2,'0')}</strong><span>${x.confirmation||''}</span></div>`
}

function renderHistory(items){
 if(!items.length){history.innerHTML='<div class="empty">Nenhuma operação finalizada.</div>';return}
 history.innerHTML=items.map(x=>`<div class="history-row"><strong>${x.name}</strong><span class="${x.direction==='CALL'?'call':'put'}">${x.side||(x.direction==='CALL'?'COMPRA':'VENDA')}</span><span>${x.automatic?'AUTO':'MANUAL'} • ${x.mode||'—'}</span><span>${money(x.stake,accountCurrency)}</span><strong class="${x.result==='WIN'?'call':x.result==='LOSS'?'put':''}">${x.result||'—'}</strong><span class="${Number(x.profit)>=0?'call':'put'}">${money(x.profit,accountCurrency)}</span></div>`).join('')
}

function renderNews(items){news.innerHTML=items.length?items.map(n=>`<div class="news-item"><strong>${n.title||'Informação'}</strong><span>${n.time||''}</span><p>${n.summary||''}</p></div>`).join(''):'<div class="empty">Sem informação de notícias vinculada ao ativo no momento.</div>'}

async function scan(){
 if(scanBusy)return;scanBusy=true;status.textContent='Escaneando análise-base M1/M5/M15...';
 try{await api('/api/scan',{method:'POST'});await refresh()}catch(e){alert(e.message)}finally{scanBusy=false}
}
async function startRobot(){try{await saveConfig(null,false);await api('/api/start',{method:'POST'});await refresh()}catch(e){alert(e.message)}}
async function stopRobot(){try{await api('/api/stop',{method:'POST'});await refresh()}catch(e){alert(e.message)}}

async function refreshDeriv(){
 if(derivBusy)return;derivBusy=true;
 try{
  const d=await api('/api/deriv/accounts');
  derivDisconnected.style.display=d.connected?'none':'block';
  derivConnected.style.display=d.connected?'block':'none';
  if(!d.connected){balanceMode.textContent='Deriv desconectada';return}

  const sel=derivAccount,old=sel.value;
  sel.innerHTML='<option value="">Selecione DEMO ou REAL</option>'+(d.accounts||[]).map(a=>`<option value="${a.account_id}" data-type="${a.account_type}" data-currency="${a.currency||'USD'}" data-balance="${a.balance??0}" ${d.selected===a.account_id?'selected':''}>${String(a.account_type).toUpperCase()} • ${a.account_id} • ${a.currency||''} ${a.balance??''}</option>`).join('');
  if(old&&!d.selected)sel.value=old;
  const opt=sel.options[sel.selectedIndex];

  if(sel.value){
   accountType=String(opt.dataset.type||'');
   accountCurrency=opt.dataset.currency||'USD';
   const liveBalance=Number(opt.dataset.balance||0);
   balance.textContent=money(liveBalance,accountCurrency);
   balanceMode.textContent=`${accountType.toUpperCase()} • ${accountCurrency}`;
   accountMode.textContent=`Modo selecionado: ${accountType.toUpperCase()}${accountType==='real'?' — DINHEIRO REAL':''}`;
  }
 }catch(e){
  derivDisconnected.style.display='block';derivConnected.style.display='none';balanceMode.textContent='Deriv: '+e.message
 }finally{derivBusy=false}
}

async function selectDerivAccount(){
 const id=derivAccount.value;if(!id)return;
 const opt=derivAccount.options[derivAccount.selectedIndex];
 if(opt.dataset.type==='real'&&!confirm('ATENÇÃO: conta REAL. Ordens automáticas ou manuais usarão dinheiro real. Deseja selecionar?')){derivAccount.value='';return}
 try{
  const r=await api('/api/deriv/select',{method:'POST',body:JSON.stringify({account_id:id})});
  accountCurrency=r.currency||opt.dataset.currency||'USD';
  setVal(['banca'],r.banca_inicial??opt.dataset.balance);
  await refresh();await refreshDeriv()
 }catch(e){alert(e.message)}
}

async function logoutDeriv(){await api('/auth/logout',{method:'POST'});location.reload()}

async function derivTrade(){
 const opt=derivAccount.options[derivAccount.selectedIndex];
 if(!derivAccount.value){alert('Conecte a Deriv e selecione DEMO ou REAL.');return}
 if(opt.dataset.type==='real'&&!confirm('CONFIRMAR ORDEM REAL: esta operação usará dinheiro real. Continuar?'))return;
 try{
  const r=await api('/api/deriv/trade',{method:'POST'});
  alert(`Operação ${r.entry.side} aberta em ${String(opt.dataset.type).toUpperCase()}.`);
  await refresh()
 }catch(e){alert(e.message)}
}

async function saveConfig(e,showMessage=true){
 if(e&&e.preventDefault)e.preventDefault();
 try{
  const tipo=valueOf(['entradaTipo','entryType'],lastConfig?.entrada_tipo||'percentual');
  const auto=checkedOf(['operacaoAutomatica','autoTrade'],lastConfig?.operacao_automatica||false);
  const payload={
   banca_inicial:+valueOf(['banca'],lastConfig?.banca_inicial||0),
   percentual_entrada:+valueOf(['percent','percentualEntrada'],lastConfig?.percentual_entrada||1),
   entrada_tipo:tipo,
   valor_entrada:+valueOf(['valorEntrada','entryValue'],lastConfig?.valor_entrada||1),
   operacao_automatica:auto,
   stop_gain:+valueOf(['sg'],lastConfig?.stop_gain||5),
   stop_loss:+valueOf(['sl'],lastConfig?.stop_loss||5),
   max_entradas:+valueOf(['maxe'],lastConfig?.max_entradas||5),
   min_score:+valueOf(['score'],lastConfig?.min_score||7),
   duracao_minutos:+valueOf(['dur'],lastConfig?.duracao_minutos||1)
  };
  await api('/api/config',{method:'POST',body:JSON.stringify(payload)});
  if(showMessage)alert(`Configurações salvas.${auto?' Operação automática LIGADA.':''}`);
  await refresh()
 }catch(err){if(showMessage)alert(err.message);else throw err}
}

const entryTypeControl=get('entradaTipo','entryType');
if(entryTypeControl)entryTypeControl.addEventListener('change',syncEntryUI);

if('serviceWorker'in navigator){
 navigator.serviceWorker.getRegistrations().then(rs=>rs.forEach(r=>r.unregister())).catch(()=>{});
}
if('caches'in window)caches.keys().then(keys=>Promise.all(keys.map(k=>caches.delete(k)))).catch(()=>{});

refresh();
refreshDeriv();
setInterval(refresh,3000);
setInterval(refreshDeriv,12000);
