# Deploy no GitHub e Easypanel

## Publicação no GitHub

O repositório deve ser criado como privado por padrão. Antes do primeiro push, confirme que `.env` não aparece no status do Git, que `Cargo.lock` está versionado e que a suíte local passa. O workflow de CI deve ser executado no primeiro push para validar o ambiente do runner.

```bash
git add .
git commit -m "feat: implement Binance momentum trading system"
gh repo create binance-momentum --private --source . --remote origin --push
```

Se o repositório já existir, use `git remote add origin ...` e `git push -u origin main` sem duplicar o remote.

## Serviço Easypanel

Crie um serviço baseado no repositório e selecione **Dockerfile** como método de build. O diretório de contexto é a raiz do projeto; o processo expõe a porta TCP `8080`. Configure o health check como `GET /health`, com intervalo compatível com o provedor e tolerância inicial suficiente para o preflight REST.

As variáveis mínimas são:

| Variável | Testnet recomendado | Observação |
| --- | --- | --- |
| `BINANCE_API_KEY` | Chave Spot Testnet | Injete como secret. |
| `BINANCE_SECRET_KEY` | Secret Spot Testnet | Injete como secret. |
| `USE_TESTNET` | `true` | Deve permanecer true durante homologação. |
| `DRY_RUN` | `true` | Evita envio de ordens. |
| `ALLOW_LIVE_TRADING` | `false` | Só altere com aprovação explícita. |
| `RUST_LOG` | `info` | Use `debug` apenas temporariamente. |
| `BINANCE_CONFIG_PATH` | `/config/default.toml` ou vazio | O binário usa o fallback local se vazio. |

Monte a pasta `config` como somente leitura se a plataforma usar volume externo. Caso a imagem já contenha o arquivo padrão, não é necessário volume, mas uma alteração de parâmetros deve gerar nova imagem e novo deploy auditável.

Use restart policy equivalente a `unless-stopped`. Configure retenção/rotação de logs. Não publique a porta REST da Binance nem o listenKey; somente o health server deve ser exposto externamente, idealmente atrás da rede privada ou de uma regra de acesso restrita.

## Primeira validação pós-deploy

Após o serviço iniciar, confirme nos logs o ambiente, `dry_run`, clock delta, quantidade de símbolos cacheados, reconciliação concluída e conexão dos três WebSockets. Consulte `/health` e confirme `status: ok`. Em Testnet e dry-run, não deve haver `POST /api/v3/order` enviado.

## Promoção para produção

Produção exige revisão humana independente, chaves específicas sem saque, whitelist de IP quando aplicável, `USE_TESTNET=false`, `DRY_RUN=false` e `ALLOW_LIVE_TRADING=true`. Faça uma mudança por vez, mantenha o primeiro período em observação e defina um procedimento de rollback que não dependa de repetir ordens com resultado desconhecido.
