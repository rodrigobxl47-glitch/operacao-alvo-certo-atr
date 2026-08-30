# Operação Alvo Certo (ATR)

PWA Web + backend Python para análise multiativos na Deriv.

## O que esta versão faz
- Descobre automaticamente ativos CALL/PUT via `active_symbols`.
- Busca candles M1 e M15 via `ticks_history`.
- Calcula EMA 3, 10, 13 e 100.
- Analisa tendência, engolfo, sequência de candles, suporte/resistência e confirmação M15.
- Cria score de confluência.
- Mostra os melhores sinais em uma interface Web.
- Gerencia percentual da banca, stop gain/loss e limite de entradas.
- Registra entrada em modo DEMO/Paper Trading.

## Importante
Ela NÃO envia ordem de dinheiro real. A conta DEMO autenticada da API nova da Deriv usa OTP; isso pode ser integrado em uma próxima etapa.

## Rodar no Termux (Android)
1. Instale o Termux.
2. No terminal:
   pkg update
   pkg install python
3. Entre na pasta do projeto.
4. Instale:
   pip install -r requirements.txt
5. Rode:
   uvicorn main:app --host 0.0.0.0 --port 8000
6. Abra no navegador:
   http://127.0.0.1:8000

## No computador
Instale Python 3.11+ e rode:
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000

## Instalar como aplicativo
Abra o endereço no Chrome/Edge e escolha "Adicionar à tela inicial" / "Instalar aplicativo".

## Configuração inicial sugerida
- Banca: 1000
- Entrada: 1%
- Score mínimo: 7
- Stop gain: 5%
- Stop loss: 5%
- Máximo de entradas: 5
