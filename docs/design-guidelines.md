# Design Guidelines (admin console + public dashboard)

Full specification: `plans/260927-0641-ai-derivatives-trading-council/design/design-system.md`. Visual source of truth: `visuals/console-mockup.html` (`?app=admin` for the console, `?app=public` for the public dashboard). Mockup numbers are sample data.

## 0. Brand
- Product name **Moirai**; mark, lockups, colours, type and voice are in `brand/README.md` and `brand/brand-book.html` (generator `scripts/brand_logo.py`).
- The console and the public dashboard use the Moirai visual system (tokens below, `src/hdt/console/theme.py`) and take the name, mark, lockup and favicon from `hdt.brand` (a stdlib-only module, so the public dashboard never imports console code). Oxblood marks only the blade (mark, heading rule, watermark); loss uses its own red with a sign or arrow, never Oxblood.
- Marketing landing page: `web/landing/` (static HTML, CSS and JS, fonts and assets local, works under `script-src 'self'; style-src 'self'`). Caddy serves it at `/`; the public dashboard lives under `/app` (Streamlit `baseUrlPath = "app"`). Light glass like the apps: drifting aurora with scroll parallax, glass panels, floating example cards around the hero mark (council ballot, order ticket, decision-cycle pill; each labelled EXAMPLE, no prices or probabilities). Motion runs on one `requestAnimationFrame` scheduler, pauses off-screen, when the tab is hidden or when the visitor presses pause, and is replaced by static frames under `prefers-reduced-motion`. Copy stays true after go-live ("Paper-first", real money gated by G4); no results, testimonials or invented figures.

## 1. Direction
- Two deployments share one design system: `console` (private admin, data-dense, high-risk configuration) and `dashboard` (public, read-only, no sign-in, no admin links, no controls).
- Light glass by default ("glass trading desk"), with night glass behind the theme toggle. Canvas: base colour plus four large blurred aurora fields (gilt, lilac, sky, mint), static in the apps. Panels: translucent white (`--surface`, `backdrop-filter: blur(18px) saturate(160%)`), white edge, inner top highlight, soft shadow; radius 20 px cards, 14 px controls, 999 px pills; primary buttons use the gilt gradient with ink text. Readability never depends on blur: every text token is checked at 4.5:1 against the glass composite over the brightest and darkest aurora point in both themes, and panels turn solid under `prefers-reduced-transparency` or without `backdrop-filter` support. Motion only as short (150-300 ms) fades, meter growth and the live dot, off under `prefers-reduced-motion`.
- Priorities: (1) safety state visible immediately (mode, kill, STOP), (2) dangerous actions cannot be triggered by mistake, (3) figures read quickly.

## 2. Color tokens
| Token | Light (default) | Dark (night glass) | Use |
|:--|:--|:--|:--|
| `--bg` | #F5F3EF | #0E0C14 | canvas under the aurora |
| `--surface` | rgba(255,255,255,.58) | rgba(255,255,255,.06) | glass card, table |
| `--glass-strong` / `--glass-solid` | rgba(255,255,255,.72) / .92 | rgba(22,19,30,.72) / #1A1722 | nav and top bar / reduced-transparency fallback |
| `--text` | #17141F | #F2EEE6 | main text |
| `--text-2` | #4A4453 | #D2CBDB | secondary text |
| `--text-3` | #625B6B | #B3ACBF | small labels |
| `--primary` | #B8903F (gradient #D9B872 to #B8903F, ink text) | #D9B872 | primary button |
| `--link` / `--focus` | #72541A | #E3C68A / #E3CB94 | link, focus ring |
| `--pos` | #0B6E3D | #6FDDA1 | profit, valid |
| `--neg` | #B02E25 | #F9978C | loss, error |
| `--warn` | #8A5100 | #F6B46C | warning |
| `--info` | #245C9C | #A3C8F2 | information |
| `--danger` | #C0362C | #D92D27 | kill/flatten |
| `--blade` | #8E2424 | #B23A34 | brand blade only, never data |
| `--cmc` / `--bnb` / `--llm` | #0B6371 / #855400 / #6A40C4 | #66D3E0 / #F3BE45 / #C7B5F8 | data source labels |
Profit/loss always carries a sign (and an arrow on KPIs); state always has text, never color alone. Every text/background pair is at least 4.5:1.

## 3. Typography and layout
- Cinzel 600 for page titles and the wordmark, Fira Sans for body (14 px dense, 16 px inputs), Fira Code with tabular numbers for numbers, codes, slugs and hashes. Scale 12/14/16/18/22/28. All fonts are self-hosted woff2 with their OFL licenses (`static/fonts/`, `web/landing/fonts/`); nothing loads from Google Fonts.
- Sidebar 248 px (>= 1024 px) or off-canvas. Console: fixed 56 px top bar with mode, system status, config version, UTC time, user, theme toggle. Public dashboard: no top bar; the UTC clock and theme toggle sit at the bottom of the sidebar (container key `hdt-side-foot`), and a stale snapshot shows a warning callout above the page.
- 12-column grid, 16 px gap and card padding, 40 px table rows, sticky headers; breakpoints 375/768/1024/1440; no horizontal page scroll.
- Navigation groups: Performance (Overview, Positions, Decisions, Agents, AI costs), Internal (Data & credits), Configuration (Models, API keys, CoinMarketCap, Binance & universe, Risk, Council, Run mode), Operations (Controls, Lessons, Audit & versions). The public dashboard shows only Performance.

## 4. Copy
- English, sentence case, US number format `1,234.56`, UTC timestamps `YYYY-MM-DD HH:MM`, money with a sign, `$` for LLM cost.
- The public dashboard states its publishing rules: 60 s snapshot cadence, and everything about trading published as it happens for the testnet and live accounts (paper is never public, owner decision 2026-09-30): each council session with its forecasts, claims, timeline, the Risk review per account (council action, applied action, result, reason) and the orders the event sent, open positions with their stop/TP/liquidation levels, closed trades and every order except the rejected ones (owner decision 2026-09-30).
- Internal codes get plain words on the public dashboard: forecast targets show as `Price up in 12 h` (`RAW_12H`) and `Beats BTC in 12 h` (`RESID_12H`), with a one-line explanation under the Agents target switch.
- Never public: API keys, config, CMC credits, ops alerts, order/algo/client ids, reconcile state, audit log.

## 5. Components
- KPI card with label, tabular value, footnote and a progress bar toward its threshold.
- Status badge: dot + text (`Valid`, `Stale`, `Error`, `Pending approval`).
- Tables: sticky header, right-aligned numbers, clickable rows with real deep links.
- Column header help (`ui.Column(tip=...)`): an info icon after the label; the tip opens below the header on hover, tap or keyboard focus, is the label's `aria-describedby` description, and escapes the scroll box on tables with fewer than 5 rows. Tip text explains the figure in words, never quotes tunable config values.
- Tabs (`st.tabs`): spaced pill buttons (36 px, 999 px radius, glass fill); the selected tab has a gilt border and gilt-soft fill, no sliding underline. Shared by the console and the public dashboard.
- Outcome tag (decision card title): pill with text plus color, LONG `--pos` with an up arrow, SHORT `--neg` with a down arrow, NO_TRADE `--text-2` on `--surface-2`, HOLD `--info`, EXIT `--warn`; size multiplier on LONG / SHORT.
- Forms: visible labels, helper text, inline errors with `role="alert"`, the hard ceiling shown next to every risk parameter and enforced by the input max.
- Secrets: write-only input, no reveal button, only `last4` + fingerprint + check status.
- Dangerous actions (Kill, Flatten, enable Live, Rollback): danger color, far from normal buttons, confirmation dialog with typed text + TOTP and concrete consequences.
- Async buttons disable with a spinner, then a polite toast that dismisses after 4 s.

## 6. Charts
| Data | Type |
|:--|:--|
| Equity, drawdown | line + drawdown area |
| Agent weights over time | multi-series line with distinct strokes and a toggleable legend |
| Calibration | reliability diagram + diagonal + bin table |
| CMC credits per day | bars + governor budget line + table |
| LLM cost per day | bars (today muted, marked in progress) + totals by role and model |
Streamlit mapping: `st.navigation` sections, `st.metric` in `st.columns`, `st.dataframe` with `column_config`, `st.tabs`, `st.dialog`, `st.form` + `st.number_input(max_value=ceiling)`, Altair/Plotly charts, a custom JS component (libsodium.js) for secret sealing.

## 7. Motion and anti-patterns
- 150-250 ms, `transform`/`opacity` only, respect `prefers-reduced-motion`, no counting-number animation on money pages.
- Forbidden: emoji icons, color-only signals, Kill next to Save or without confirmation, re-displaying a stored secret, accepting above-ceiling risk values and only failing on the server.
