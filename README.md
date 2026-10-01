Operação Alvo Certo (ATR) — Deriv MTF v2
Aplicação Web/PWA em FastAPI para análise técnica de índices derivados/sintéticos disponíveis na Deriv.
Estratégia desta versão
Trabalha somente com ativos classificados como derivados/sintéticos pelo scanner.
Usa 110 candles em cada período: M1, M5 e M15.
Procura confluência da mesma direção nos três tempos gráficos.
Tendência de alta/baixa: terceiro toque da linha e estrutura de pernadas A/B/C.
Suporte/resistência: terceiro toque e topo/fundo duplo, com confirmação pelo corpo do candle, evitando validar apenas pavios.
Rompimento: exige rompimento na direção analisada e nova confirmação.
Confirmações de alta: Hammer, Inverted Hammer, Bullish Engulfing, Morning Star, Piercing Line, Bullish Harami, Three White Soldiers e Bullish Marubozu.
Confirmações de baixa: Shooting Star, Hanging Man, Bearish Engulfing, Evening Star, Dark Cloud Cover, Bearish Harami, Three Black Crows e Bearish Marubozu.
Interface
Saldo/banca da conta selecionada.
Seleção de conta Deriv DEMO ou REAL após autenticação.
Operação aberta com COMPRA/VENDA, valor, P/L e tempo aproximado restante.
Histórico com WIN, LOSS ou EMPATE.
Painel informativo de notícias separado do gatilho técnico.
Confirmação adicional no navegador antes de usar conta REAL.
Variáveis no Render
Configure em Environment:
DERIV_CLIENT_ID = Client ID/App ID da aplicação OAuth Deriv.
DERIV_REDIRECT_URI = https://SEU-SERVICO.onrender.com/auth/callback
SESSION_SECRET = chave aleatória longa e secreta.
Não coloque senha da Deriv, OTP ou access token no GitHub.
Render
Build Command: pip install -r requirements.txt
Start Command: uvicorn main:app --host 0.0.0.0 --port $PORT
Teste de saúde: /health deve responder com {"ok": true, ...}.
Local / Termux
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
Abra http://127.0.0.1:8000.
Segurança
Teste primeiro em DEMO. A opção REAL envia ordens com dinheiro real quando uma conta real estiver selecionada e a ordem for confirmada. Critérios técnicos e padrões de candles não garantem resultado positivo.
