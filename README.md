# binance-momentum

Sistema assíncrono de trading momentum para **Binance Spot**, escrito em Rust e projetado para iniciar em **Spot Testnet + `DRY_RUN=true`**. O software mantém a execução live bloqueada por padrão e só aceita habilitá-la quando o operador altera explicitamente as três condições de segurança descritas abaixo.

> Este repositório é infraestrutura de automação financeira, não uma promessa de rentabilidade. Use somente chaves com permissões mínimas, valide o comportamento em Testnet e faça uma revisão humana antes de habilitar ordens reais.

## Estado da implementação

O sistema já contém autenticação HMAC-SHA256, sincronização de relógio, cliente REST com timeout e tracker de request weight, cache de filtros de exchange, streams WebSocket com reconexão, scanner de momentum, sizing por saldo, quantização de lote e preço, proteção OCO, reconciliação REST, health check, shutdown gracioso, testes automatizados, Dockerfile e CI.

| Componente | Responsabilidade |
| --- | --- |
| `src/config.rs` | Carregamento TOML/env, URLs Testnet/produção e travas de execução. |
| `src/binance/auth.rs` | HMAC-SHA256, timestamp e clock delta. |
| `src/binance/client.rs` | REST público/assinado, ordens, OCO, account, exchangeInfo e listenKey. |
| `src/binance/exchange_info.rs` | Cache thread-safe de `LOT_SIZE`, `MARKET_LOT_SIZE`, `PRICE_FILTER`, notional e percent-price. |
| `src/binance/ws.rs` | Mini ticker, book ticker e User Data Stream com reconexão. |
| `src/engine/scanner.rs` | Ring buffers, momentum, volume surge, spread e elegibilidade USDT. |
| `src/engine/risk.rs` | Limite de posições, saldo, sizing, quantização e validação de filtros. |
| `src/engine/executor.rs` | MARKET BUY, proteção OCO SELL, rollback de emergência e fechamento por execution report. |
| `src/engine/state.rs` | Posições, reservas, bloqueios, saldos e reconciliação. |
| `src/service.rs` | Health check HTTP, refresh de filtros, reconciliação pós-gap e keepalive. |
| `src/main.rs` | Preflight, criação das tasks, canais e shutdown. |

## Segurança operacional

A configuração padrão usa `USE_TESTNET=true` e `DRY_RUN=true`. A execução live exige simultaneamente:

```dotenv
USE_TESTNET=false
DRY_RUN=false
ALLOW_LIVE_TRADING=true
```

Sem essas três condições, o processo encerra ou permanece sem enviar ordens. A chave Binance deve ter apenas as permissões indispensáveis; não habilite saque. Segredos devem ser injetados por variáveis de ambiente ou pelo gerenciador de segredos do Easypanel, nunca por `config/default.toml`, commit, log ou imagem Docker.

O executor trata respostas HTTP 5xx de ordem como **status desconhecido**. Ele não repete automaticamente uma ordem cujo resultado não foi confirmado, bloqueia o símbolo e exige reconciliação. Se a OCO falhar com status conhecido, tenta uma venda MARKET de emergência; se essa venda também falhar, o símbolo permanece bloqueado. Perda de eventos no User Data Stream pausa novas entradas até uma sincronização REST de conta e ordens abertas.

## Configuração

Copie o arquivo de exemplo e preencha somente as variáveis necessárias ao ambiente escolhido:

```bash
cp .env.example .env
```

O arquivo `config/default.toml` concentra os parâmetros de estratégia. Os valores padrão são os da especificação: no máximo duas posições, 10% do saldo por posição, stop de 1,5%, primeiro alvo de 3%, janela de momentum de 60 segundos, gatilho de 2,5%, volume surge de 2x, spread máximo de 0,3% e volume mínimo de 5 milhões USDT em 24 horas.

O scanner calcula a mudança entre o primeiro e o último preço dentro da janela, a média de volume dos ticks, a razão de surge, o spread pelo book ticker e o volume 24h. O filtro de volume 24h é fornecido pelo bloco `[risk]` e aplicado no scanner pelo bootstrap.

## Execução local

Pré-requisitos: Rust estável compatível com Edition 2021 e OpenSSL development headers para a feature `native-tls`.

```bash
cargo fmt --all -- --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo run --release
```

Com o processo em execução, o endpoint de saúde pode ser consultado por:

```bash
curl http://localhost:8080/health
```

O processo realiza ping, sincronização de relógio, leitura de conta, refresh inicial de filtros e reconciliação antes de abrir os WebSockets. Sem credenciais válidas ele falha imediatamente, por desenho.

## Docker e Easypanel

O `Dockerfile` usa build multi-stage e executa o binário em imagem runtime Debian mínima com usuário numérico não-root. Localmente, quando Docker estiver disponível:

```bash
cp .env.example .env
# preencha BINANCE_API_KEY e BINANCE_SECRET_KEY da Spot Testnet
# mantenha USE_TESTNET=true e DRY_RUN=true durante a validação

docker compose up --build
```

Para o Easypanel, crie um serviço a partir do repositório GitHub, selecione build por Dockerfile, exponha a porta `8080`, monte ou copie `config/default.toml` como somente leitura e injete as variáveis do `.env` pelo painel de secrets. Use uma política de restart equivalente a `unless-stopped`, habilite health check HTTP em `/health` e mantenha logs persistidos/rotacionados.

A imagem pode ser construída por CI ou pelo próprio Easypanel. O sandbox utilizado para desenvolver este repositório não possui o executável Docker, portanto a construção efetiva da imagem deve ser confirmada no runner GitHub ou no ambiente de deploy.

## CI

O workflow `.github/workflows/ci.yml` executa `cargo fmt --check`, `cargo check --all-targets`, `cargo test --all-targets` e `cargo clippy --all-targets --all-features -- -D warnings` em pushes para `main`/`develop` e pull requests para `main`. O objetivo é impedir que warnings de qualidade ou testes quebrados cheguem ao branch principal.

## Testes

Os testes unitários cobrem o vetor HMAC oficial consistente da Binance, desserialização de mini ticker e execution report, parsing de exchangeInfo, URLs WebSocket, quantização, notional, OCO, reconciliação de estado e decisões do scanner. Os testes de integração ficam em `tests/auth_test.rs`, `tests/health_test.rs`, `tests/risk_test.rs` e `tests/scanner_test.rs`.

A suíte local validada durante a construção foi:

```bash
cargo fmt --all -- --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
```

## Limites conhecidos antes do live trading

A estratégia não foi validada financeiramente nem submetida a backtest; os percentuais são parâmetros operacionais da especificação, não recomendação de investimento. A integração real com a Binance requer chaves Testnet e ambiente de rede autorizado. Antes de live, deve-se executar uma validação humana das respostas de ordem, comportamento de filtros em cada símbolo, custos de comissão, recuperação após restart e reconciliação de ordens manuais.

### Referências

[1]: https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/trade "Binance Spot REST Trade API"
[2]: https://developers.binance.com/en/docs/products/spot/filters "Binance Spot Symbol Filters"
[3]: https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~ "Binance Spot WebSocket Market Streams"
[4]: https://developers.binance.com/en/docs/products/spot/user-data-stream "Binance Spot User Data Stream"
[5]: https://developers.binance.com/en/docs/binance-spot-api-docs/testnet/rest-api "Binance Spot Testnet REST API"
