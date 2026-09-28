let cfgLoaded=false;
async function api(u,o={}){let r=await fetch(u,{cache:"no-store",...o});let d={};try{d=await r.json()}catch(e){}if(!r.ok)throw new Error(d.detail||d.message||"HTTP "+r.status);return d}
const $=s=>document.querySelector(s), money=v=>v==null?"--":Number(v).toLocaleString("pt-BR",{style:"currency",currency:"BRL"});
function toast(t){$("#toast").textContent=t;$("#toast").style.display="block";setTimeout(()=>$("#toast").style.display="none",4500)}
function render(s){
 $("#balance").textContent=money(s.balance);$("#pnl").textContent=Number(s.pnl_percent||0).toFixed(4)+"%";$("#entries").textContent=s.entries||0;$("#status").textContent=s.status||"--";
 $("#progress").textContent=s.scan_progress||((s.symbols||[]).length?("Monitorando: "+s.symbols.join(" • ")):"");
 let a=s.analyses||[],sig=$("#signals");
 sig.innerHTML=a.length?a.map(x=>`<div class="opp"><span class="tag">${x.result}</span> <b>${x.symbol}</b> • Score ${x.score} <small>(C ${x.buy_score} / V ${x.sell_score})</small><br><span class="muted">${(x.reasons||[]).join(" • ")}</span><br><small>Notícias: ${x.news_direction||"NEUTRA"} • força ${x.news_strength||0}</small></div>`).join(""):"Nenhum sinal atingiu o score mínimo.";
 let best=(s.signals||[]).find(x=>x.result==="COMPRA");$("#buy").disabled=!best||!!s.open_trade;
 if(s.open_trade){let t=s.open_trade;$("#open").innerHTML=`<b>${t.symbol} • COMPRA</b><br>Entrada: ${t.entry_price}<br>Preço atual: ${t.current_price||t.entry_price}<br>Variação: <b>${Number(t.current_pnl_percent||0).toFixed(4)}%</b><br>Quantidade: ${t.qty}<br>Tempo restante: <b>${s.countdown}s</b><br>Notícia durante operação: <b>${t.news_direction||"monitorando"}</b> • força ${t.news_strength||0}`}
 else $("#open").textContent="Nenhuma operação aberta.";
 let h=s.history||[];$("#history").innerHTML=h.length?h.map(x=>`<div class="opp"><b>${x.symbol}</b> • ${x.close_reason||x.result}<br>Entrada ${x.entry_price} → Saída ${x.exit_price}<br>Taxa compra: ${money(x.buy_fee||0)} • Taxa venda: ${money(x.sell_fee||0)}<br><b>Resultado líquido: ${money(x.pnl_brl)}</b></div>`).join(""):"Sem operações nesta sessão.";
 if(s.last_error)$("#progress").innerHTML+=`<div class="danger">${s.last_error}</div>`;
 if(!cfgLoaded&&s.config){let c=s.config;$("#pct").value=c.percentual_entrada;$("#sg").value=c.stop_gain;$("#sl").value=c.stop_loss;$("#maxe").value=c.max_entradas;$("#score").value=c.min_score;$("#maxa").value=c.max_ativos;$("#tp").value=c.take_profit_operacao;$("#opsl").value=c.stop_loss_operacao;$("#duration").value=c.duracao_segundos;$("#tf").value=c.timeframe;$("#news").checked=c.news_filter;$("#newsexit").checked=c.news_exit_enabled;$("#newsstrength").value=c.news_exit_min_strength;$("#newsconfirm").value=c.news_exit_confirm_pct;$("#newsseconds").value=c.news_check_seconds;$("#auto").checked=c.auto_trade;cfgLoaded=true}
}
async function status(){try{render(await api("/api/status"))}catch(e){$("#status").textContent="Erro de conexão"}}
window.addEventListener("DOMContentLoaded",()=>{
 $("#scan").onclick=async()=>{try{$("#status").textContent="Analisando...";await api("/api/scan",{method:"POST"});await status()}catch(e){toast("Erro: "+e.message);await status()}};
 $("#start").onclick=async()=>{await api("/api/start",{method:"POST"});toast("Scanner automático iniciado.");status()};
 $("#stop").onclick=async()=>{await api("/api/stop",{method:"POST"});toast("Scanner parado.");status()};
 $("#buy").onclick=async()=>{if(!confirm("Enviar COMPRA REAL usando o melhor sinal válido? TP, Stop Loss e tempo máximo controlarão a saída."))return;try{let d=await api("/api/real-entry",{method:"POST"});toast(d.message||"Compra enviada.");status()}catch(e){toast("Erro: "+e.message)}};
 $("#save").onclick=async()=>{let body={percentual_entrada:+$("#pct").value,stop_gain:+$("#sg").value,stop_loss:+$("#sl").value,max_entradas:+$("#maxe").value,min_score:+$("#score").value,max_ativos:+$("#maxa").value,duracao_segundos:+$("#duration").value,timeframe:$("#tf").value,take_profit_operacao:+$("#tp").value,stop_loss_operacao:+$("#opsl").value,auto_trade:$("#auto").checked,max_movimento_pct:1.0,news_filter:$("#news").checked,news_confirm_only:true,news_exit_enabled:$("#newsexit").checked,news_exit_min_strength:+$("#newsstrength").value,news_exit_confirm_pct:+$("#newsconfirm").value,news_check_seconds:+$("#newsseconds").value};try{await api("/api/config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});toast("Gerenciamento salvo.")}catch(e){toast("Erro: "+e.message)}};
 status();setInterval(status,3000);
});
