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
V2:
histórico ampliado para 160 candles;
M5 agregado de M1;
regimes ALTA/BAIXA/LATERAL;
ABC, 3º toque, pullback/rompimento, engolfo, rejeição, sequência;
lateral somente nos extremos da faixa com gatilho;
score mínimo continua configurável.
V3 inteligente:
mantém todos os elementos anteriores;
combo contextual ALTA / BAIXA / LATERAL;
exige espaço até resistência/suporte antes de aceitar continuação;
monitor de notícia durante operação;
saída antecipada por notícia somente quando notícia é contrária, atinge força mínima e o preço confirma movimento adverso;
TP, SL e tempo máximo continuam como proteções prioritárias. Observação: notícia/tom não prevê mercado e pode conter ruído; por isso não dispara saída isoladamente.
V4 - volatilidade adaptativa:
mantém limite de volatilidade como proteção;
movimento acima do limite não é mais bloqueado automaticamente;
impulso comprador só passa com ALTA + M15 + ABC/pullback + gatilho + score forte e confluência;
impulso vendedor é reconhecido de forma simétrica, sem abrir short no mercado à vista;
movimento esticado ou conflitante continua em AGUARDAR.
