# Política de segurança

## Segredos

Nunca faça commit de `.env`, chaves Binance, assinaturas, tokens ou dumps de requests. Use o `.env.example` apenas como template. Em Easypanel, injete segredos pelo recurso de environment/secrets do serviço. Rotacione imediatamente qualquer chave que tenha sido exposta.

## Permissões Binance

Use chaves separadas para Testnet e produção. Conceda somente leitura e Spot Trading quando necessário. A permissão de saque deve permanecer desabilitada. Restringir por IP, quando compatível com a operação, reduz o impacto de vazamento.

## Travas do aplicativo

O modo seguro combina `USE_TESTNET=true`, `DRY_RUN=true` e `ALLOW_LIVE_TRADING=false`. A produção só é aceita quando todas as travas live forem alteradas explicitamente. Respostas de ordem com status desconhecido não são repetidas automaticamente; o símbolo é bloqueado até reconciliação.

## Relato de incidente

Em caso de exposição de chave, falha de proteção OCO, status desconhecido ou posição não reconciliada, interrompa novas entradas, revogue/rotacione as chaves afetadas, confirme ordens e saldos na Binance e preserve os logs. Não publique dados sensíveis em issues ou pull requests; reporte pelo canal privado da equipe responsável.

## Escopo

Este documento descreve controles do software. Ele não substitui revisão de segurança de infraestrutura, gestão de chaves do provedor, auditoria de estratégia, validação financeira ou aprovação operacional para negociação real.
