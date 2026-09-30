# API Endpoints Design

External endpoints used by the system and the internal config-api surface. Facts verified on 2026-09-27 (`plans/.../research/researcher-01-api-facts.md`). Items marked **UNVERIFIED** must be confirmed with a real call before they are relied on.

## 1. CoinMarketCap
- Base keyed: `https://pro-api.coinmarketcap.com`, header `X-CMC_PRO_API_KEY` (never logged).
- Base keyless: `https://pro-api.coinmarketcap.com/public-api/...`, GET (and POST for `/v1/dex/holders/list`), no key header, shared IP rate pool with no published number; on 429/1005 fall back to keyed only while the credit governor has budget.
- Envelope: `status` (`error_code`, `credit_count`, `timestamp`) + `data`.
- Credit rules: 1 credit per call returning 200; list endpoints +1 per 100 data points beyond 100; derivatives endpoints 1 credit per 250 items with a 60 s cache; OHLCV historical 1 credit per 100 data points; `/v1/key/info` costs 0 credits. Error responses are not charged.
- WebSocket: `wss://pro-stream.coinmarketcap.com/v1`, header auth, channel `market@crypto_latest_price`, 0.025 credit per message received (one coin per push, about every 5 s for top 500), 10 connections x 100 subscriptions on Startup, beta (outside SLA). Close codes 4100 (bad key), 4102 (plan).
- Error codes: 1001-1005, 1007 key errors (configuration errors, route `failing`); 1006 plan not authorized (HTTP 403, not charged): the route is "not on current CMC plan" (`route_health` `disabled`, no alert, keeps its cadence and returns to `ok` after a plan upgrade); 1008 per-minute rate (retry with backoff); **1009 daily cap, 1010 monthly cap, 1011 IP limit** are separate halt states handled by the credit governor, never retried blindly.
- Route catalog (30 routes, cadence, consumers, keyless flags): `docs/system-architecture.md` section 5.1 and `config/cmc_routes.yaml`.
- **UNVERIFIED**: every `Startup+` and `keyless` label in `config/cmc_routes.yaml` must be confirmed with a real call once a key exists; results are recorded in section 6 below. Startup monthly credit and per-minute rate limit are read from `/v1/key/info` (`credit_limit_monthly`, `rate_limit_minute`). `cmc.rate_limit_per_min` in `config/settings.yaml` is only a ceiling: the recorder reads `/v1/key/info` before its first CMC job and caps its rolling 60 s request window to the plan's `rate_limit_minute` (30/min until key info answers), re-reads it hourly and whenever a new key version is sealed. Verified 2026-09-29: the current key reports 50/min and 15,000 credits/month (Basic), not the Startup design values.

## 2. Binance USDS-M Futures
### 2.1 Hosts
| Purpose | Live | Demo (testnet) |
|:--|:--|:--|
| REST | `https://fapi.binance.com` | `https://demo-fapi.binance.com` |
| WS public (depth) | `wss://fstream.binance.com/public` | `wss://demo-fstream.binance.com` + path **UNVERIFIED** |
| WS market (kline, aggTrade, markPrice, forceOrder) | `wss://fstream.binance.com/market` | same, **UNVERIFIED** |
| WS private (user data) | `wss://fstream.binance.com/private/ws/<listenKey>` | same, **UNVERIFIED** |
The legacy `/ws` and `/stream` URLs were discontinued on 2026-04-23. Phase 09 must probe `/public`, `/market`, `/private` on the demo host before writing the adapter.

### 2.2 Public data
| Endpoint | Weight / limit |
|:--|:--|
| `GET /fapi/v1/exchangeInfo` | 1 |
| `GET /fapi/v1/fundingInfo` | shares 500 req / 5 min / IP with `fundingRate` |
| `GET /fapi/v1/klines`, `/fapi/v1/markPriceKlines` | 1-10 by limit |
| `GET /fapi/v1/depth` | 2-20 by limit |
| `GET /fapi/v1/openInterest` | 1 |
| `GET /futures/data/openInterestHist` | weight 0, 1000 req / 5 min, last 1 month only |
| `GET /fapi/v1/premiumIndex` | 1 per symbol |
| `GET /fapi/v1/time` | time sync for signing |
WS streams: `<symbol>@depth20@100ms` (`/public`), `<symbol>@kline_5m`, `_15m`, `<symbol>@aggTrade`, `<symbol>@markPrice@1s`, `!forceOrder@arr` (`/market`). Connection limits: 24 h per connection, ping every 3 min, 10 incoming messages/s, 1024 streams per connection.

### 2.3 Signed (execution service only)
| Endpoint | Notes |
|:--|:--|
| `POST /fapi/v1/order` | LIMIT GTX entry, IOC fallback, reduceOnly market close; `newClientOrderId` |
| `POST /fapi/v1/algoOrder` | `algoType=CONDITIONAL`, `STOP_MARKET` / `TAKE_PROFIT_MARKET`, `triggerPrice`, `workingType=MARK_PRICE`, `priceProtect`, `clientAlgoId`. Sending these types to `/fapi/v1/order` returns `-4120` since 2025-12-09 |
| `DELETE /fapi/v1/order`, `/fapi/v1/allOpenOrders` | regular orders only; `allOpenOrders` does not cancel algo orders |
| `DELETE /fapi/v1/algoOrder`, `/fapi/v1/algoOpenOrders`; `GET /fapi/v1/openAlgoOrders`, `/fapi/v1/allAlgoOrders` | algo order management; untriggered conditional orders cannot be modified (cancel + re-place, new stop first) |
| `POST /fapi/v1/leverage`, `/fapi/v1/marginType` | <= 3x, `ISOLATED`, per symbol before the first order |
| `GET/POST /fapi/v1/positionSide/dual` | must be One-way (`dualSidePosition=false`); refuse to run otherwise |
| `GET /fapi/v3/positionRisk`, `/fapi/v3/account`, `/fapi/v3/balance` | weight 5 each; reconciliation and `AccountState` |
| `POST/PUT/DELETE /fapi/v1/listenKey` | keepalive every 30-50 min; `-1125` or `listenKeyExpired` -> recreate |
| `GET /fapi/v1/userTrades`, `/fapi/v1/allOrders` | re-sync and ledger rebuild |
Rate limits (read live from `exchangeInfo`): 2400 request weight / min / IP, 1200 orders / min, 300 orders / 10 s. 429 -> back off; 418 -> stop the namespace and alert. 503 "Unknown error" -> UNKNOWN state, verify by client id before retrying.

### 2.4 Key permission check (phase 12 step 1, verified 2026-09-27)
Endpoint: `GET /sapi/v1/account/apiRestrictions` on `https://api.binance.com` ("Get API Key Permission", security `USER_DATA`: header `X-MBX-APIKEY` + HMAC `signature` over `timestamp` and optional `recvWindow`; weight(IP) 1). It is a wallet (SAPI) endpoint, so it exists only on the live host; the demo (testnet) key is checked with `GET /fapi/v3/account` alone.

Response fields (all booleans unless noted): `ipRestrict`, `createTime` (ms), `enableReading`, `enableWithdrawals`, `enableInternalTransfer`, `permitsUniversalTransfer`, `enableMargin`, `enableFutures`, `enableSpotAndMarginTrading`, `enableVanillaOptions`, `enablePortfolioMarginTrading`, `enableFixApiTrade`, `enableFixReadOnly`, `tradingAuthorityExpirationTime` (ms). Binance notes that withdrawals can only be enabled on a key that already has the IP access restriction, and that a key created before the futures account was opened cannot use the futures API (`enableFutures=false`).

Rule for the live key (implemented in the phase 09 execution owner check, activation refused on any violation):
- `enableWithdrawals == false` (no withdrawal),
- `ipRestrict == true` (IP whitelist on); because Binance rejects a restricted key called from a non-whitelisted IP, a successful signed call made from the VPS proves the VPS egress IP is on the whitelist, so no owner checkbox is needed,
- `enableFutures == true` (the key can trade USDS-M),
- "trade only" (Design Contract section 10): `enableInternalTransfer == false` and `permitsUniversalTransfer == false`.
The check then runs the signed `GET /fapi/v3/account` on `fapi.binance.com`. Only the booleans above are stored as non-secret check details for the API Keys page.

## 3. OpenRouter
- Base `https://openrouter.ai/api/v1`, used through the `openai` Python SDK.
- `GET /models`: `supported_parameters` must include `structured_outputs`; cached 24 h for the console model dropdown.
- `GET /key`: `limit_remaining` for the owner self-test.
- `POST /chat/completions`: `response_format={type: json_schema, json_schema: {strict: true, schema}}`, provider preferences `order`, `only`, `allow_fallbacks=false`, `require_parameters=true`, `data_collection=deny`, `zdr`, `max_price`. Record requested model, returned model, provider, generation id and `usage.cost`.
- Production accepts only pinned slugs; `openrouter/auto` and `:free` variants are rejected.
- The router refuses, before any request, `allow_fallbacks=true`, `data_collection` other than `deny` and every `:` variant slug (`:free`, `:online`, `:nitro`, `:floor`, `:thinking`); a reply served by a provider outside `provider.only` (compared in slug form) or by an unpinned model is refused. A role whose model self-test failed is closed at the router for every pipeline (`LlmRoleClosedError`). As sent by `hdt.agents.llm_router.LlmRouter` (phase 05): `model` (plus a `models` list when fallbacks are configured), `temperature`, `max_tokens`, either `response_format` json_schema strict (structured calls) or `tools`/`tool_choice` (the agents' ReAct steps), `provider {only|order, allow_fallbacks=false, require_parameters=true, data_collection=deny}`, `usage {include: true}`. Every billed generation is written to `llm_calls`; replies are cached in `llm_cache` under `(prompt_hash, model_slug, provider, params_hash)`; replay reads only the cache.

## 3a. News sources (phase 07)
- Binance announcements: `www.binance.com/bapi/composite/v1/public/cms/article/list/query` (catalog 48 new listings, 161 delistings); the only T0 listing source.
- Full-text news feed, licensed news API and unlock calendar: **pending owner selection**. The adapters are generic and config-driven (`config/news.yaml`: RSS/Atom feeds, a JSON news API, a JSON unlock calendar) and stay disabled until a source is chosen. Selection criteria: has `published_at`, maps to coins (or can be mapped by symbol and name), license permits internal storage. When a source is enabled, add its host to `deploy/squid/allowlist/news.txt`; an API key goes in vault scope `data`, secret `news_feed`, field `api_key`.
- Article copies are fetched only by the council's live `fetch_source` tool through the sandboxed `fetcher` (`POST` with `item_id` and the item's ingested URL; returns bytes, sha256 and the redirect chain).

## 3b. DeepSeek (temporary LLM gateway, owner decision 2026-09-28)
Facts from `api-docs.deepseek.com`, read 2026-09-28. Used only by roles whose `models` config has `gateway: deepseek`; OpenRouter stays the default.
- Base `https://api.deepseek.com` (OpenAI-compatible), used through the `openai` Python SDK; key `llm/deepseek_api_key` as `Authorization: Bearer`. Egress host `api.deepseek.com` in `deploy/squid/allowlist/llm.txt`.
- Models: `deepseek-flash` (DeepSeek-V4.1-Flash) and `deepseek-v4-pro` (DeepSeek-V4-Pro-0813), context 1M tokens. The reply's `model` must be the requested name or a documented alias of it (`hdt.llm.deepseek.SERVED_NAMES`): `deepseek-flash` accepts `deepseek-flash` and the legacy `deepseek-v4-flash` (served by V4.1-Flash); `deepseek-v4-pro` accepts only itself. A Flash request never accepts a Pro reply and the reverse; any other name is refused and not cached. The docs do not state the `model` value a reply carries, so the set is to be confirmed by a live probe.
- `POST /chat/completions`: `model`, `messages`, `max_tokens`, `tools` / `tool_choice=auto` for tool steps; structured calls send `response_format={type: json_object}` (JSON Output; no json_schema) and the router appends the compact Pydantic JSON Schema to the system message (the prompt must contain "json"; the reply may occasionally be empty and is then retried once like any unparseable reply). Thinking: `thinking={type: disabled}` with `temperature` (the router default), or `thinking={type: enabled}` with `reasoning_effort` `low|high|max` and no temperature (ignored in thinking mode). In thinking mode the reply carries `reasoning_content`, which must be sent back with the assistant message of every tool round (HTTP 400 otherwise); `tool_choice` `required` or a named tool is not supported there.
- Usage: `prompt_tokens`, `prompt_cache_hit_tokens`, `prompt_cache_miss_tokens`, `completion_tokens` (includes `completion_tokens_details.reasoning_tokens`). No cost field: the router prices hit, miss and output tokens from `config/deepseek.yaml` at the peak or off-peak rate of the request time; `llm_calls.provider` is `deepseek`.
- `GET /user/balance`: `{is_available, balance_infos: [{currency (USD|CNY), total_balance, granted_balance, topped_up_balance}]}` (amounts are strings); the owner key test (`hdt.llm.deepseek_key_check`): 401/403 invalid, `is_available` false invalid, 200 valid with `total_balance_usd` / `total_balance_cny`.
- No provider routing, fallbacks, ZDR or price caps; a DeepSeek role config must leave those fields unset.

## 4. Telegram Bot API
- `getMe`, `getWebhookInfo` (owner self-test and monitoring), `sendMessage`, `getUpdates` or webhook.
- Authorization by `from.id` in a private chat against the allowlist stored in scope `ops`.

## 5. Internal config-api (FastAPI, Tailscale/SSH tunnel only)
All routes require a server-side session (argon2 password + mandatory TOTP), cookie `HttpOnly; Secure; SameSite=Strict`, 30 min idle expiry, lockout after 5 failures, and a CSRF token on mutating requests. Step-up TOTP is required for writing a secret, changing run mode, changing Risk while live, and resuming after an automatic kill.
| Area | Routes (prefix `/api`) |
|:--|:--|
| Auth | `POST /auth/login`, `POST /auth/logout`, `GET /auth/session`, `POST /auth/step-up` |
| Config sections | `GET /config/{section}`, `GET /config/{section}/versions`, `POST /config/{section}` (new immutable version), `POST /config/{section}/rollback`. Retired keys are dropped from stored payloads on read and never saved: risk `decision_ttl_s` (removed 2026-09-28, decisions have no TTL) |
| Models | `GET /models/catalog?gateway=` `openrouter` (default; OpenRouter `/models` cached 24 h) or `deepseek` (from `config/deepseek.yaml`), `POST /models/test` (runs in the `llm` scope owner, on the role config's gateway) |
| Secrets | `GET /secrets` (metadata only: last4, fingerprint, status, checked_at), `GET /secrets/public-key/{scope}`, `POST /secrets/{scope}/{name}` (sealed blob only), `POST /secrets/{scope}/{name}/test` |
| CMC | credit projection for a proposed route config (save blocked above 90 % of quota) |
| Mode | run mode `paper | testnet | live` with the live gate (newest G4 final passed with the pinned config still active, live key valid, checklist, TOTP); while live, `models` and `council` saves and rollbacks answer 409 `live_config_locked`, and lowering `size_multiplier` needs no G4 re-check |
| Controls | pause, resume, kill, flatten |
| Lessons | `GET /lessons`, `POST /lessons/{id}/approve {note, expected_state?}`, `POST /lessons/{id}/retire {note, expected_state?}`; 409 when the transition is not allowed (e.g. approving before an improving A/B) or on a concurrent review, 422 on a blank note, 404 for an unknown lesson; the store event and the audit row are written in one transaction |
| Audit | filtered audit log |
There is no endpoint that returns a secret value.

## 6. Verification log
| Item | Status | Date | Evidence |
|:--|:--|:--|:--|
| Keyless labels, context/system routes #11, #12, #16 | verified keyless (HTTP 200, `credit_count` 1) | 2026-09-27 | live calls to `/public-api/v3/fear-and-greed/latest`, `/public-api/v1/cryptocurrency/map?aux=platform,is_active` (`platform` = `{id, name, symbol, slug, token_address}` or null); recorder smoke run |
| Keyless labels, DEX routes #24-27 | verified keyless; query names from the reference pages work (`platformName`/`platform` = map `platform.slug`, e.g. `solana`, `ethereum`; `address`); `/v1/dex/holders/list` is **POST** with JSON body `{tokenAddress, platform, tag}` and answered for `solana` but returned `400 Parameter error` for `ethereum`/`eth` | 2026-09-27 | live calls with BONK (`DezXAZ8z...B263`, Solana) and PEPE (`0x6982...1933`, Ethereum); `eth` as platform returns 400/500 |
| Keyed Startup labels, REST routes #1-10, #13-15, #23, #28, #29 | verified on a Startup key (`/v1/key/info`: 450,000 credits/month, 600 requests/min); every route `ok` in `route_health` | 2026-09-29 | local recorder run after the plan upgrade (`route_health`, `cmc_key_info`) |
| Keyed Startup label, WS #30 `market@crypto_latest_price` | pending: disabled in `config/cmc_routes.yaml` (enabled only while positions are held), never exercised | - | - |
| Keyed Basic key, plan refusals | `1006` (HTTP 403, `credit_count` 0) on #7 `/v2/cryptocurrency/ohlcv/historical`, #10 `/v2/cryptocurrency/price-performance-stats/latest`, #13 `/v1/cryptocurrency/trending/latest`, #14 `/v1/cryptocurrency/trending/gainers-losers`, #15 `/v1/cryptocurrency/listings/new`; left empty as "not on current plan"; all five `ok` after the Startup upgrade the same day | 2026-09-29 | local recorder run (`route_health`, lake error captures) |
| DEX holders #27, `ethereum` and HyperCore tokens | POST `{tokenAddress, platform, tag}` answered 200 for every `ethereum` contract target (7 coins, e.g. UNI, LINK); `400 Parameter error` for HYPE (`platform` `hyperliquid`, `token_address` `0x0d01...b011ec`, a 16-byte HyperCore token index, not a contract), now left out of the DEX targets | 2026-09-29 | local recorder run (lake captures) |
| Binance public REST + WS on the new hosts (`/public` depth20@100ms, `/market` kline, aggTrade, markPrice@1s, `!forceOrder@arr`) | verified live: combined `/stream?streams=` payload `{stream, data}`, 20 levels per depth frame, `X-MBX-USED-WEIGHT-1M` header present | 2026-09-27 | recorder smoke run (`hdt.ingest.binance_ws`, `hdt.ingest.binance_public`) |
| Demo WS paths `/public`, `/market`, `/private` | pending probe (phase 09) | - | - |
| Binance key-permission endpoint | verified from official sources (live call runs in the phase 09 owner check) | 2026-09-27 | Binance developer docs index (`developers.binance.com/en/docs/llms.txt`, Wallet REST API: "`GET /sapi/v1/account/apiRestrictions` Get API Key Permission (USER_DATA)"); official OpenAPI spec `github.com/binance/binance-api-swagger` `spot_api.yaml` (path `/sapi/v1/account/apiRestrictions`, weight(IP) 1, required fields incl. `ipRestrict`, `enableWithdrawals`, `enableFutures`); official SDK `binance-connector-python` `binance_sdk_wallet` `GetApiKeyPermissionResponse` |
