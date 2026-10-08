ATR v5.2.1 FOREX • Pernadas — Operação Alvo Certo
====================================

OBJETIVO
--------
Versão do projeto ATR preparada para trabalhar com Forex na Deriv, substituindo o universo de índices sintéticos.

PARES ANALISADOS
-----------------
- EUR/USD
- GBP/USD
- USD/JPY
- USD/CAD
- AUD/USD
- USD/CHF
- NZD/USD

O sistema resolve os códigos dos pares a partir dos ativos disponíveis na Deriv, em vez de depender de um nome fixo como EUR/USD.

ESTRATÉGIA MANTIDA
------------------
- 110 candles fechados em M1, M5 e M15.
- M1 continua sendo o timeframe de entrada/confirmação desta versão.
- Combos independentes.
- Suporte forte no limite mais baixo do gráfico + 3 contatos + próxima vela de confirmação.
- Resistência forte no limite mais alto do gráfico + 3 contatos + próxima vela de confirmação.
- Tendência de alta + 1ª→5ª pernada, cada uma com sua correção + candle de confirmação após a correção da 5ª.
- Tendência de baixa + 1ª→5ª pernada, cada uma com sua correção + candle de confirmação após a correção da 5ª.
- Níveis de suporte/resistência também consideram regiões de rejeição/perda de força dos candles.
- Não exige que todos os critérios estejam presentes ao mesmo tempo.
- Score mínimo removido da decisão.

CONFIRMAÇÕES DE ALTA
--------------------
- Hammer
- Bullish Engulfing
- Morning Star
- Piercing Line
- Bullish Harami
- Three White Soldiers
- Bullish Marubozu

CONFIRMAÇÕES DE BAIXA
---------------------
- Shooting Star
- Bearish Engulfing
- Evening Star
- Dark Cloud Cover
- Bearish Harami
- Three Black Crows
- Bearish Marubozu

CORREÇÕES DA v5.2
-----------------
1. STATUS
   O status passa a refletir o estado real:
   - PARADO
   - ESCANEANDO
   - ATIVO
   - OPERAÇÃO ABERTA

2. HISTÓRICO
   Operações liquidadas são registradas com resultado WIN/LOSS e informações da operação.

3. INFORMAÇÕES/NOTÍCIAS
   A interface mantém o painel de informações/notícias para o contexto do Forex.
   Notícias não geram uma entrada automaticamente.

4. ANTI-429
   Mantida a arquitetura da v5.1 para reduzir excesso de conexões:
   - WebSocket público persistente.
   - Cache de ativos.
   - Cache de candles.
   - Requisições sequenciais para reduzir rajadas.
   - Backoff/retry em respostas 429.

RENDER
------
Build Command:
pip install -r requirements.txt

Start Command:
uvicorn main:app --host 0.0.0.0 --port $PORT

VARIÁVEIS DE AMBIENTE
---------------------
DERIV_CLIENT_ID
DERIV_REDIRECT_URI
SESSION_SECRET

DERIV_REDIRECT_URI deve ser exatamente:
https://operacao-alvo-certo-atr.onrender.com/auth/callback

IMPORTANTE
----------
- Não coloque tokens, senhas, OTPs ou SESSION_SECRET dentro do código.
- O modo REAL deve ser usado com cautela. O projeto não garante lucro.
- Forex possui horários de mercado e pode ficar sem candles fora do período disponível.
- O plano Free do Render pode suspender a aplicação após inatividade; portanto, ele não garante execução contínua 24 horas.
- Antes de operar com dinheiro real, teste primeiro o fluxo de login, saldo, análise, abertura e liquidação.

ARQUIVOS
--------
main.py
requirements.txt
static/index.html
static/app.js
static/style.css
README_ATR_v5_2_FOREX.txt

DEPLOY
------
1. Extraia o ZIP.
2. Substitua os arquivos correspondentes no GitHub.
3. Faça commit/push.
4. No Render, aguarde o novo deploy.
5. Abra a aplicação.
6. Faça login pela Deriv.
7. Confira se o saldo aparece corretamente.
8. Confira o status ATIVO/ESCANEANDO.
9. Confira o histórico depois de uma operação liquidada.

VERSÃO
-------
ATR v5.2.1 FOREX • Pernadas
