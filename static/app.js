let cfgLoaded=false;
async function api(u,o={}){let r=await fetch(u,{cache:"no-store",...o});let d={};try{d=await r.json()}catch(e){}if(!r.ok)throw new Error(d.detail||d.message||"HTTP "+r.status);return d}
const $=s=>document.querySelector(s);
const money=v=>v==null?"--":Number(v).toLocaleString("pt-BR",{style:"currency",currency:"BRL"});
function toast(t){$("#toast").textContent=t;$("#toast").style.display="block";setTimeout(()=>$("#toast").style.display="none",4500)}
function render(s){
 $("#balance").textContent=money(s.balance);$("#pnl").textContent=Number(s.pnl_percent||0).toFixed(2)+"%";$("#entries").textContent=s.entries||0;$("#status").textContent=s.status||"--";
 $("#progress").textContent=s.scan_progress||((s.symbols||[]).length?("Monitorando: "+s.symbols.join(" • ")):"");
 let a=s.analyses||[], sig=$("#signals");
 if(!a.length)sig.innerHTML="Nenhum sinal atingiu o score mínimo.";
 else sig.innerHTML=a.map(x=>`<div class="opp"><span class="tag">${x.result}</span> <b>${x.symbol}</b> • Score ${x.score} <small>(C ${x.buy_score} / V ${x.sell_score})</small><br><span class="muted">${(x.reasons||[]).join(" • ")}</span></div>`).join("");
 let best=(s.signals||[]).find(x=>x.result==="COMPRA");$("#buy").disabled=!best||!!s.open_trade;
 if(s.open_trade)$("#open").innerHTML=`<b>${s.open_trade.symbol} • COMPRA</b><br>Entrada: ${s.open_trade.entry_price}<br>Quantidade: ${s.open_trade.qty}<br>Fechamento em: <b>${s.countdown}s</b>`;
 else $("#open").textContent="Nenhuma operação aberta.";
 let h=s.history||[];$("#history").innerHTML=h.length?h.map(x=>`<div class="opp">${x.symbol} • ${x.result} • P/L aprox.: ${money(x.pnl_brl)}</div>`).join(""):"Sem operações nesta sessão.";
 if(s.last_error)$("#progress").innerHTML+=`<div class="danger">${s.last_error}</div>`;
 if(!cfgLoaded&&s.config){$("#pct").value=s.config.percentual_entrada;$("#sg").value=s.config.stop_gain;$("#sl").value=s.config.stop_loss;$("#maxe").value=s.config.max_entradas;$("#score").value=s.config.min_score;$("#maxa").value=s.config.max_ativos;$("#auto").checked=s.config.auto_trade;cfgLoaded=true}
}
async function status(){try{render(await api("/api/status"))}catch(e){$("#status").textContent="Erro de conexão"}}
window.addEventListener("DOMContentLoaded",()=>{
 $("#scan").onclick=async()=>{try{$("#status").textContent="Analisando...";await api("/api/scan",{method:"POST"});await status()}catch(e){toast("Erro: "+e.message);await status()}};
 $("#start").onclick=async()=>{await api("/api/start",{method:"POST"});toast("Scanner automático iniciado.");status()};
 $("#stop").onclick=async()=>{await api("/api/stop",{method:"POST"});toast("Scanner parado.");status()};
 $("#buy").onclick=async()=>{if(!confirm("Enviar COMPRA REAL usando o melhor sinal válido? A operação será fechada após 1 minuto."))return;try{let d=await api("/api/real-entry",{method:"POST"});toast(d.message||"Compra enviada.");status()}catch(e){toast("Erro: "+e.message)}};
 $("#save").onclick=async()=>{let body={percentual_entrada:+$("#pct").value,stop_gain:+$("#sg").value,stop_loss:+$("#sl").value,max_entradas:+$("#maxe").value,min_score:+$("#score").value,max_ativos:+$("#maxa").value,duracao_segundos:60,auto_trade:$("#auto").checked,max_movimento_m1_pct:1.0};try{await api("/api/config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});toast("Gerenciamento salvo.")}catch(e){toast("Erro: "+e.message)}};
 status();setInterval(status,3000);
});
