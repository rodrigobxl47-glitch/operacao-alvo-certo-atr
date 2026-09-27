async function req(url,opt={}){const r=await fetch(url,opt);return await r.json()}
function money(v){return v==null?"--":Number(v).toLocaleString("pt-BR",{style:"currency",currency:"BRL"})}
function render(s){
 document.querySelector("#balance").textContent=money(s.balance);
 document.querySelector("#pnl").textContent=Number(s.pnl_percent||0).toFixed(2)+"%";
 document.querySelector("#entries").textContent=s.entries||0;
 document.querySelector("#status").textContent=s.status||"--";
 document.querySelector("#symbols").textContent=(s.symbols||[]).join(" • ")||"--";
 const el=document.querySelector("#signals");
 if(!s.signals?.length){el.innerHTML="<p>Nenhum sinal atingiu o score mínimo.</p>";return}
 el.innerHTML=s.signals.map(x=>`<article><h3>${x.symbol} — ${x.direction}</h3><b>Score ${x.score}</b><p>Preço: ${x.price}</p><p>${(x.reasons||[]).join(" • ")}</p></article>`).join("");
}
async function status(){try{render(await req("/api/status"))}catch(e){}}
document.querySelector("#scan").onclick=async()=>{document.querySelector("#status").textContent="Analisando...";await req("/api/scan",{method:"POST"});status()}
document.querySelector("#start").onclick=async()=>{await req("/api/start",{method:"POST"});status()}
document.querySelector("#stop").onclick=async()=>{await req("/api/stop",{method:"POST"});status()}
status();setInterval(status,5000);
