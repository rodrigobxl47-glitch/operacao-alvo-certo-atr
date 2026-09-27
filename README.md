Operação Alvo Certo (ATR) — aprimorado
Mantém a interface e a estratégia anterior e acrescenta:
Take Profit e Stop Loss por operação
tempo máximo configurável
seleção M1/M5, mantendo confirmação M15
resultado líquido e taxas registradas por ordem
confirmação opcional por notícias (GDELT), usada apenas como filtro de conflito
correção do cálculo do Stop Gain/Stop Loss da sessão para usar P/L realizado
fechamento vende somente a quantidade comprada pelo ATR
Variáveis Render: MB_API_ID, MB_API_SECRET e opcional MB_ACCOUNT_ID. Build: pip install -r requirements.txt Start: uvicorn main:app --host 0.0.0.0 --port $PORT
IMPORTANTE: ordens são reais. Teste primeiro scanner, saldo e controles. O fechamento por tempo é solicitado ao atingir o tempo configurado; execução real pode ocorrer alguns segundos depois por rede/API/mercado.
