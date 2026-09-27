Operação Alvo Certo (ATR) — Mercado Bitcoin FINAL
Arquivos prontos para GitHub/Render.
Variáveis do Render
MB_API_ID
MB_API_SECRET
MB_ACCOUNT_ID (opcional)
Build
pip install -r requirements.txt
Start
uvicorn main:app --host 0.0.0.0 --port $PORT
Estratégia preservada
M1 + confirmação M15; EMA 10/100; cruzamento EMA 3/13; engolfo; sequência de candles; suporte/resistência dos últimos 10 candles; tendência macro; score mínimo 7; resultado COMPRA / VENDA / AGUARDAR.
Operação
Compra real somente com sinal COMPRA válido.
Venda automática após 60 segundos apenas da quantidade comprada pelo próprio ATR.
Se o valor calculado por % estiver abaixo do min-cost do ativo, a ordem é bloqueada.
VENDA analítica não abre short nem vende saldo antigo.
