# Runbook operacional

## Inicialização segura

O primeiro ciclo deve ser executado com uma conta Spot Testnet, `USE_TESTNET=true` e `DRY_RUN=true`. Após injetar as chaves, valide os logs de preflight: ping, clock delta, account, exchangeInfo, reconciliação e criação do listenKey. Nenhuma ordem deve aparecer em modo dry-run.

O endpoint de saúde deve responder com JSON em `GET /health`. O status esperado é `200 OK` depois da reconciliação inicial. Enquanto uma reconciliação estiver pendente, o endpoint responde `503 Service Unavailable`, sinalizando que novas entradas estão pausadas.

## Observabilidade mínima

Os logs são JSON e usam `RUST_LOG`, com nível padrão `info`. Eventos importantes incluem conexão/reconexão WebSocket, lag de broadcast, refresh de filtros, request weight, sinais, sizing, ordens, OCO, PnL, bloqueios e reconciliação.

| Sinal | Interpretação | Ação operacional |
| --- | --- | --- |
| `REST request weight reached 80 percent` | A janela de peso está próxima do limite configurado. | Reduza polling externo e confirme o limite da conta antes de ampliar o universo. |
| `execution status unknown` | Um 5xx ocorreu em ordem e o resultado não é confirmável. | Não repita manualmente antes de consultar ordens abertas e saldo. O símbolo foi bloqueado. |
| `live entries are paused` | O estado local está aguardando reconciliação. | Verifique REST, credenciais e conectividade; aguarde o ciclo de reconciliação. |
| `emergency sell failed` | Uma posição pode estar sem proteção e a saída de emergência falhou. | Trate como incidente crítico; confirme a posição na Binance e intervenha manualmente. |
| `WebSocket disconnected` | O stream perdeu conexão e iniciará backoff. | Verifique rede, limites e disponibilidade; não force restart repetido. |

## Pausa e encerramento

Para interromper o processo, envie `SIGINT`/Ctrl+C. O bootstrap cancela tasks, aguarda finalização e encerra. Não mate o container abruptamente durante uma janela em que uma ordem acabou de ser enviada; consulte o status REST caso isso aconteça.

A pausa operacional mais segura é parar novas entradas por `DRY_RUN=true` e reiniciar o serviço após confirmar ordens abertas e saldos. Não altere `ALLOW_LIVE_TRADING` sem uma revisão de mudança e sem registrar a justificativa.

## Incidente de status desconhecido

Se um endpoint de ordem retorna 5xx, a aplicação não faz retry automático. Consulte ordens abertas, account balances e histórico na Binance. Se houver uma posição, confirme se existe OCO. Se não houver proteção, faça a saída manual somente depois de confirmar quantidade disponível. Em seguida, mantenha o símbolo bloqueado até a reconciliação e revisão do incidente.

## Incidente de User Data Stream

Um `RecvError::Lagged` marca `reconciliation_required`, altera o health check para 503 e pausa entradas. O processo tenta `get_account_info` e `get_all_open_orders` a cada ciclo de cinco segundos. Se as chamadas continuarem falhando, preserve a pausa; não remova a trava editando estado em runtime.

## Checklist de mudança

Antes de qualquer mudança de parâmetros ou código, confirme que o branch está limpo, que a suíte completa passa e que o ambiente permanece em Testnet. Depois do deploy, valide `/health`, logs de preflight, número de símbolos cacheados e ausência de ordens inesperadas. Para promover a produção, exija revisão humana independente e chaves sem permissão de saque.
