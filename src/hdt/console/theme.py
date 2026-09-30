"""Moirai design tokens and the shared stylesheet of the admin console and the public dashboard.

Every color either app renders comes from the semantic tokens below; components never use a raw hex.
The look is light-first "glass trading": a warm paper canvas under a soft aurora mesh (champagne gilt,
lilac, sky, mint), frosted glass panels (translucent white, backdrop blur, a white edge and an inner top
highlight), Gilt as the one accent (gradient primary action, focus, selection, the equity line). Profit,
loss, warning and info keep their own hues and always travel with an arrow, a sign or a label; Oxblood is
only the blade of the mark and the cut of the thread rule, never a loss color. Light is the default; the
top-bar toggle switches to "night glass" (dimmed aurora on #0E0C14). Without `backdrop-filter` support or
under `prefers-reduced-transparency` every panel becomes solid. The stylesheet maps the tokens onto CSS
custom properties, styles the apps' own HTML (hero, KPIs, cards, tables, badges, callouts, empty states)
and re-skins the native Streamlit widgets through the same variables, so the toggle switches the whole UI
without a page reload. Fonts (Fira Sans, Fira Code, Cinzel for page titles) are self-hosted under
`static/fonts` and registered in `.streamlit/config.toml`; neither app requests a third-party font.
Motion is CSS only (`st.html` strips scripts), 150-250 ms, and switched off under `prefers-reduced-motion`.
"""

from __future__ import annotations

from typing import Final, Literal
from urllib.parse import quote

from hdt import brand

ThemeMode = Literal["dark", "light"]
THEME_MODES: Final[tuple[ThemeMode, ...]] = ("dark", "light")
DEFAULT_THEME: Final[ThemeMode] = "light"

LIGHT: Final[dict[str, str]] = {
    "bg": "#F5F3EF",
    "surface": "rgba(255,255,255,.58)",
    "surface-2": "rgba(23,20,31,.05)",
    "elev": "rgba(255,255,255,.94)",
    "border": "rgba(23,20,31,.08)",
    "border-2": "rgba(23,20,31,.16)",
    "field": "#8A8171",
    "text": brand.INK,
    "text-2": "#4A4453",
    "text-3": "#625B6B",
    "primary": "#B8903F",
    "primary-h": "#A9832F",
    "primary-grad": "linear-gradient(135deg,#D9B872,#B8903F)",
    "primary-grad-h": "linear-gradient(135deg,#E2C688,#C29A48)",
    "primary-glow": "0 6px 18px rgba(184,144,63,.32),0 1px 2px rgba(122,90,28,.25)",
    "on-primary": brand.INK,
    "link": "#72541A",
    "gilt": "#B8903F",
    "gilt-soft": "rgba(184,144,63,.16)",
    "gilt-area": "rgba(184,144,63,.30)",
    "focus": "#72541A",
    "blade": brand.OXBLOOD_ON_LIGHT,
    "tile": brand.OBSIDIAN,
    "pos": "#0B6E3D",
    "neg": "#B02E25",
    "warn": "#8A5100",
    "info": "#245C9C",
    "pos-bg": "rgba(15,122,69,.12)",
    "neg-bg": "rgba(192,54,44,.12)",
    "warn-bg": "rgba(154,91,0,.12)",
    "info-bg": "rgba(36,92,156,.12)",
    "danger": "#C0362C",
    "danger-h": "#A52D24",
    "on-danger": "#FFFFFF",
    "cmc": "#0B6371",
    "bnb": "#855400",
    "llm": "#6A40C4",
    "grid": "rgba(23,20,31,.07)",
    "scrim": "rgba(23,20,31,.35)",
    "shadow": "0 10px 30px rgba(23,20,31,.08),0 1px 2px rgba(23,20,31,.06)",
    "glass-strong": "rgba(255,255,255,.72)",
    "glass-solid": "rgba(255,255,255,.92)",
    "glass-edge": "rgba(255,255,255,.75)",
    "glass-hi": "inset 0 1px 0 rgba(255,255,255,.9)",
    "zebra": "rgba(23,20,31,.025)",
    "aurora-1": "rgba(233,201,138,.55)",
    "aurora-2": "rgba(190,176,245,.45)",
    "aurora-3": "rgba(160,205,245,.45)",
    "aurora-4": "rgba(170,228,205,.40)",
}

DARK: Final[dict[str, str]] = {
    "bg": "#0E0C14",
    "surface": "rgba(255,255,255,.06)",
    "surface-2": "rgba(255,255,255,.06)",
    "elev": "#1C1826",
    "border": "rgba(255,255,255,.08)",
    "border-2": "rgba(255,255,255,.16)",
    "field": "#807896",
    "text": "#F2EEE6",
    "text-2": "#D2CBDB",
    "text-3": "#B3ACBF",
    "primary": "#D9B872",
    "primary-h": "#E3C88E",
    "primary-grad": "linear-gradient(135deg,#D9B872,#B8903F)",
    "primary-grad-h": "linear-gradient(135deg,#E2C688,#C29A48)",
    "primary-glow": "0 6px 20px rgba(217,184,114,.22),0 1px 2px rgba(0,0,0,.4)",
    "on-primary": brand.INK,
    "link": "#E3C68A",
    "gilt": "#D9B872",
    "gilt-soft": "rgba(217,184,114,.12)",
    "gilt-area": "rgba(217,184,114,.24)",
    "focus": "#E3CB94",
    "blade": brand.OXBLOOD_ON_DARK,
    "tile": brand.OBSIDIAN,
    "pos": "#6FDDA1",
    "neg": "#F9978C",
    "warn": "#F6B46C",
    "info": "#A3C8F2",
    "pos-bg": "rgba(95,211,148,.14)",
    "neg-bg": "rgba(244,124,112,.14)",
    "warn-bg": "rgba(242,163,80,.14)",
    "info-bg": "rgba(141,184,236,.14)",
    "danger": "#D92D27",
    "danger-h": "#B92420",
    "on-danger": "#FFFFFF",
    "cmc": "#66D3E0",
    "bnb": "#F3BE45",
    "llm": "#C7B5F8",
    "grid": "rgba(242,238,230,.07)",
    "scrim": "rgba(6,5,9,.6)",
    "shadow": "0 10px 30px rgba(0,0,0,.35),0 1px 2px rgba(0,0,0,.3)",
    "glass-strong": "rgba(22,19,30,.72)",
    "glass-solid": "#1A1722",
    "glass-edge": "rgba(255,255,255,.12)",
    "glass-hi": "inset 0 1px 0 rgba(255,255,255,.08)",
    "zebra": "rgba(255,255,255,.025)",
    "aurora-1": "rgba(233,201,138,.12)",
    "aurora-2": "rgba(190,176,245,.12)",
    "aurora-3": "rgba(160,205,245,.12)",
    "aurora-4": "rgba(170,228,205,.10)",
}

TOKENS: Final[dict[ThemeMode, dict[str, str]]] = {"dark": DARK, "light": LIGHT}

SANS: Final[str] = "'Fira Sans','Segoe UI',system-ui,sans-serif"
MONO: Final[str] = "'Fira Code',Consolas,monospace"
DISPLAY: Final[str] = "'Cinzel','Fira Sans',Georgia,serif"

# One 2px-stroke icon set (the mockup's symbols). Rendered as CSS masks so `currentColor` still applies:
# the HTML sanitizer of `st.html` strips inline SVG.
ICON_PATHS: Final[dict[str, str]] = {
    "dash": '<rect x="3" y="3" width="7" height="9" rx="1"/><rect x="14" y="3" width="7" height="5" rx="1"/>'
    '<rect x="14" y="12" width="7" height="9" rx="1"/><rect x="3" y="16" width="7" height="5" rx="1"/>',
    "key": '<circle cx="7.5" cy="15.5" r="4.5"/><path d="m10.7 12.3 9.8-9.8M16 7l3 3M18.5 4.5l2 2"/>',
    "ok": '<circle cx="12" cy="12" r="9"/><path d="m8 12 3 3 5-6"/>',
    "no": '<circle cx="12" cy="12" r="9"/><path d="m15 9-6 6M9 9l6 6"/>',
    "wait": '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
    "alert": '<path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/>'
    '<path d="M12 9v4M12 17h.01"/>',
    "info": '<circle cx="12" cy="12" r="9"/><path d="M12 16v-4M12 8h.01"/>',
    "lock": '<rect x="4" y="11" width="16" height="10" rx="2"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/>',
    "check": '<path d="M20 6 9 17l-5-5"/>',
    "up-a": '<path d="M12 19V5M6 11l6-6 6 6"/>',
    "down-a": '<path d="M12 5v14M6 13l6 6 6-6"/>',
    "hash": '<path d="M4 9h16M4 15h16M10 3 8 21M16 3l-2 18"/>',
    "power": '<path d="M12 3v9"/><path d="M6.3 6.3a8 8 0 1 0 11.4 0"/>',
    "book": '<path d="M2 5h7a3 3 0 0 1 3 3v12a2 2 0 0 0-2-2H2zM22 5h-7a3 3 0 0 0-3 3v12a2 2 0 0 1 2-2h8z"/>',
    "flask": '<path d="M9 3h6M10 3v6l-5.4 9.2A2 2 0 0 0 6.3 21h11.4a2 2 0 0 0 1.7-2.8L14 9V3M7 15h10"/>',
    "db": '<ellipse cx="12" cy="5" rx="8" ry="3"/>'
    '<path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
    "users": '<circle cx="9" cy="8" r="3.5"/>'
    '<path d="M2.5 20a6.5 6.5 0 0 1 13 0M16 4.6a3.5 3.5 0 0 1 0 6.8M21.5 20a6.5 6.5 0 0 0-4-6"/>',
    "stop": '<path d="M7.9 2h8.2L22 7.9v8.2L16.1 22H7.9L2 16.1V7.9z"/><path d="M15 9l-6 6M9 9l6 6"/>',
}


def _icon_data_uri(paths: str) -> str:
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="black" '
        f'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">{paths}</svg>'
    )
    return "data:image/svg+xml," + quote(svg, safe="")


def token(mode: ThemeMode, name: str) -> str:
    """Resolved value of one token (charts need concrete colors, not CSS variables)."""
    return TOKENS[mode][name]


def root_variables(mode: ThemeMode) -> str:
    body = ";".join(f"--{k}:{v}" for k, v in TOKENS[mode].items())
    return (
        f":root{{{body};--font-sans:{SANS};--font-mono:{MONO};--font-display:{DISPLAY};"
        f"--ease:cubic-bezier(.25,1,.5,1);color-scheme:{mode}}}"
    )


_ICON_CSS: Final[str] = "".join(
    f'.i-{name}{{--icon:url("{_icon_data_uri(paths)}")}}' for name, paths in ICON_PATHS.items()
)

_BRAND_CSS: Final[str] = (
    f':root{{--brand-mark:url("{brand.MARK_DATA_URI}");--strands:url("{brand.STRANDS_DATA_URI}");'
    f'--wordmark:url("{brand.WORDMARK_MASK_URI}")}}'
)

# Shared components (the mockup's classes, scoped under .hdt so they never clash with Streamlit).
# Scale: 4 px spacing grid; text 11/12/13/14/16/20/28 px, hero figure 44 px. Shape and depth (radii 20 cards,
# 14 controls, 999 pills; frosted glass with edge, highlight and soft shadow) are set by `_GLASS_CSS`.
_COMPONENT_CSS: Final[str] = """
.hdt{font-family:var(--font-sans);color:var(--text);font-size:14px;line-height:1.6;letter-spacing:.005em}
.hdt .mono,.hdt code,.hdt .num{font-family:var(--font-mono);font-variant-numeric:tabular-nums;letter-spacing:0}
.hdt code{font-size:12.5px;background:var(--surface-2);padding:2px 6px;border-radius:6px;color:var(--text)}
.hdt a{color:var(--link);text-decoration-color:color-mix(in srgb,var(--link) 45%,transparent);text-underline-offset:3px;
 transition:text-decoration-color 150ms var(--ease)}
.hdt a:hover{text-decoration-color:var(--link)}
.hdt a:focus-visible{outline:2px solid var(--focus);outline-offset:2px;border-radius:4px}
.hdt h1,.hdt h2,.hdt h3{margin:0;line-height:1.2;color:var(--text);padding:0;text-wrap:balance}
.hdt h1{font-family:var(--font-display);font-size:28px;font-weight:600;letter-spacing:.03em}
.hdt h1:focus{outline:none}
.hdt h2{font-size:16px;font-weight:600}.hdt h3{font-size:14px;font-weight:600}
.hdt p{margin:0;text-wrap:pretty}
.hdt .i{display:inline-block;width:18px;height:18px;flex:none;background-color:currentColor;
 -webkit-mask:var(--icon) no-repeat center/contain;mask:var(--icon) no-repeat center/contain;vertical-align:-3px}
.hdt .i.sm{width:14px;height:14px;vertical-align:-2px}
.hdt .page-head{display:flex;align-items:flex-end;justify-content:space-between;gap:16px;flex-wrap:wrap;margin:4px 0 8px}
.hdt .page-head h1::after{content:"";display:block;width:72px;height:2px;margin-top:12px;
 background:linear-gradient(90deg,var(--gilt) 0 52px,transparent 52px 58px,var(--blade) 58px 72px)}
.hdt .page-head p{color:var(--text-2);margin-top:12px;max-width:72ch}
.hdt .crumb{font-size:12.5px;color:var(--text-3);margin-bottom:8px;display:flex;gap:8px;align-items:center}
.hdt .row-actions{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
@keyframes hdt-in{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
.hdt .card,.hdt .kpi,.hdt .hero,.hdt .tbl-wrap,.hdt .empty{animation:hdt-in 240ms var(--ease) backwards}
.hdt .kpi:nth-child(2){animation-delay:40ms}.hdt .kpi:nth-child(3){animation-delay:80ms}
.hdt .kpi:nth-child(4){animation-delay:120ms}.hdt .kpi:nth-child(n+5){animation-delay:160ms}
.hdt .card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px 20px;min-width:0}
.hdt .card-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px;flex-wrap:wrap}
.hdt .card-head p{color:var(--text-3);font-size:12.5px;margin-top:2px}
.hdt .card h3{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px}
.hdt .hero{position:relative;overflow:hidden;display:grid;grid-template-columns:minmax(0,1fr) auto;gap:24px;align-items:end;
 padding:24px;border:1px solid var(--border);border-radius:12px;background:var(--surface);margin-bottom:12px}
.hdt .hero::before{content:"";position:absolute;left:0;right:0;top:0;height:1px;
 background:linear-gradient(90deg,var(--gilt),color-mix(in srgb,var(--gilt) 0%,transparent) 70%)}
.hdt .hero::after{content:"";position:absolute;right:296px;bottom:-40px;width:184px;height:184px;opacity:.16;pointer-events:none;
 background:var(--strands) no-repeat center/contain}
.hdt .hero>*{position:relative;z-index:1}
.hdt .hero-lab{display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:13px;color:var(--text-2);font-weight:500}
.hdt .hero-val{font-family:var(--font-mono);font-variant-numeric:tabular-nums;font-size:44px;line-height:1.1;font-weight:500;
 letter-spacing:-.01em;margin:8px 0;color:var(--text);overflow-wrap:anywhere}
.hdt .hero-delta{display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-family:var(--font-mono);font-variant-numeric:tabular-nums;font-size:14px}
.hdt .hero-delta .muted{font-family:var(--font-sans)}
.hdt .hero-meta{display:grid;grid-template-columns:auto auto;gap:8px 16px;margin:0;font-size:12.5px;align-items:center}
.hdt .hero-meta dt{color:var(--text-3)}
.hdt .hero-meta dd{margin:0;color:var(--text);font-family:var(--font-mono);font-variant-numeric:tabular-nums;text-align:right}
.hdt .kpis{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(176px,1fr));margin-bottom:4px}
.hdt .kpi{position:relative;background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px;min-width:0}
.hdt .kpi::before{content:"";position:absolute;left:16px;right:16px;top:-1px;height:1px;
 background:linear-gradient(90deg,var(--gilt),color-mix(in srgb,var(--gilt) 0%,transparent))}
.hdt .kpi .lab{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--text-3);font-weight:600;
 display:flex;align-items:center;gap:8px}
.hdt .kpi .val{font-family:var(--font-mono);font-variant-numeric:tabular-nums;font-size:24px;font-weight:500;line-height:1.25;
 margin:12px 0 4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.hdt .kpi .val .i{vertical-align:-2px;margin-right:2px}
.hdt .kpi .sub{font-size:12.5px;color:var(--text-2);line-height:1.5}
.hdt .up{color:var(--pos)}.hdt .down{color:var(--neg)}.hdt .warn{color:var(--warn)}.hdt .muted{color:var(--text-3)}
.hdt .meter{position:relative;height:6px;border-radius:3px;background:var(--surface-2);overflow:hidden;margin-top:12px}
.hdt .meter>span{display:block;height:100%;border-radius:3px;background:var(--gilt);transform-origin:left;
 animation:hdt-grow 400ms var(--ease) backwards}
@keyframes hdt-grow{from{transform:scaleX(0)}}
.hdt .meter>i{position:absolute;top:0;bottom:0;width:2px;background:var(--neg)}
.hdt .meter.m-warn>span{background:var(--warn)}.hdt .meter.m-neg>span{background:var(--neg)}
.hdt .meter.m-pos>span{background:var(--pos)}.hdt .meter.tall{height:10px}
.hdt .meter-cap{display:flex;justify-content:space-between;font-size:12px;color:var(--text-3);margin-top:4px;gap:8px;
 font-family:var(--font-mono);font-variant-numeric:tabular-nums}
.hdt .badge{display:inline-flex;align-items:center;gap:6px;height:24px;padding:0 8px;border-radius:6px;font-size:12px;
 font-weight:600;white-space:nowrap;letter-spacing:.01em}
.hdt .badge .dot{width:8px;height:8px;border-radius:50%;background:currentColor;flex:none}
.hdt .b-pos{color:var(--pos);background:var(--pos-bg)}.hdt .b-neg{color:var(--neg);background:var(--neg-bg)}
.hdt .b-warn{color:var(--warn);background:var(--warn-bg)}.hdt .b-info{color:var(--info);background:var(--info-bg)}
.hdt .b-mute{color:var(--text-2);background:var(--surface-2)}
.hdt .b-gilt{color:var(--link);background:var(--gilt-soft)}
.hdt .tag{display:inline-flex;align-items:center;height:24px;padding:0 8px;border-radius:6px;font-size:11.5px;
 font-weight:600;font-family:var(--font-mono);border:1px solid var(--border-2);color:var(--text-2)}
.hdt .t-cmc{color:var(--cmc);border-color:color-mix(in srgb,var(--cmc) 40%,transparent)}
.hdt .t-bnb{color:var(--bnb);border-color:color-mix(in srgb,var(--bnb) 40%,transparent)}
.hdt .t-llm{color:var(--llm);border-color:color-mix(in srgb,var(--llm) 40%,transparent)}
.hdt .side-long{color:var(--pos);font-weight:600}.hdt .side-short{color:var(--neg);font-weight:600}
.hdt .side-long::before{content:"\\2191\\00A0" / "";font-weight:600}.hdt .side-short::before{content:"\\2193\\00A0" / "";font-weight:600}
.hdt .th-tip{position:relative;display:inline-flex;align-items:center;gap:4px;cursor:help;border-radius:4px}
.hdt .th-tip>.i{opacity:.65;transition:opacity 150ms var(--ease)}
.hdt .th-tip:hover>.i,.hdt .th-tip:focus>.i{opacity:1}
.hdt .th-tip:focus-visible{outline:2px solid var(--focus);outline-offset:2px}
.hdt .th-tip .tip{position:absolute;top:calc(100% + 12px);left:-8px;z-index:5;width:max-content;max-width:280px;
 padding:10px 12px;border-radius:10px;background:var(--elev);border:1px solid var(--border-2);box-shadow:var(--shadow);
 color:var(--text);font:400 12.5px/1.5 var(--font-sans);letter-spacing:normal;text-transform:none;white-space:normal;
 text-align:left;visibility:hidden;opacity:0;transform:translateY(-4px);pointer-events:none;
 transition:opacity 150ms var(--ease),transform 150ms var(--ease),visibility 150ms}
.hdt th.r .th-tip .tip,.hdt th:last-child .th-tip .tip{left:auto;right:-8px}
.hdt .th-tip:hover .tip,.hdt .th-tip:focus .tip{visibility:visible;opacity:1;transform:none}
.hdt th:has(.th-tip:hover),.hdt th:has(.th-tip:focus){z-index:3}
.hdt .tbl-wrap.tbl-short:has(.th-tip:hover),.hdt .tbl-wrap.tbl-short:has(.th-tip:focus){overflow:visible;position:relative;z-index:10}
.hdt .outcome-tag{display:inline-flex;align-items:center;gap:8px;height:32px;padding:0 14px;margin-left:4px;border-radius:999px;
 vertical-align:4px;font:600 14px/1 var(--font-mono);letter-spacing:.06em;border:1px solid transparent;white-space:nowrap}
.hdt .outcome-tag::before{content:"";width:8px;height:8px;border-radius:50%;background:currentColor;flex:none}
.hdt .o-long::before{content:"\\2191" / "";width:auto;height:auto;background:none}
.hdt .o-short::before{content:"\\2193" / "";width:auto;height:auto;background:none}
.hdt .o-long{color:var(--pos);background:var(--pos-bg);border-color:color-mix(in srgb,var(--pos) 30%,transparent)}
.hdt .o-short{color:var(--neg);background:var(--neg-bg);border-color:color-mix(in srgb,var(--neg) 30%,transparent)}
.hdt .o-no_trade{color:var(--text-2);background:var(--surface-2);border-color:var(--border-2)}
.hdt .o-hold{color:var(--info);background:var(--info-bg);border-color:color-mix(in srgb,var(--info) 30%,transparent)}
.hdt .o-exit{color:var(--warn);background:var(--warn-bg);border-color:color-mix(in srgb,var(--warn) 30%,transparent)}
.hdt .rows>*+*{margin-top:16px}.hdt .rows b{font-size:13.5px;font-weight:600}
.hdt h1.form-title{font-size:20px;margin-bottom:8px}
.hdt .tbl-wrap{overflow:auto;max-height:560px;border:1px solid var(--border);border-radius:12px;background:var(--surface)}
.hdt table{width:100%;border-collapse:separate;border-spacing:0;font-size:13.5px}
.hdt th,.hdt td{text-align:left;padding:0 16px;height:44px;border-bottom:1px solid var(--border);white-space:nowrap;color:var(--text)}
.hdt th{position:sticky;top:0;height:40px;background:var(--surface);font-size:11px;font-weight:600;letter-spacing:.08em;
 text-transform:uppercase;color:var(--text-3);z-index:1;border-bottom-color:var(--border-2)}
.hdt tbody tr:last-child td{border-bottom:0}
.hdt tbody tr{transition:background-color 150ms var(--ease)}
.hdt tbody tr:hover{background:var(--surface-2)}
.hdt td.r,.hdt th.r{text-align:right}
.hdt td.wrap{white-space:normal;min-width:240px;line-height:1.5;padding-top:12px;padding-bottom:12px}
.hdt td.stack{padding-top:8px;padding-bottom:8px;line-height:1.4}
.hdt tr.is-chosen{background:var(--gilt-soft)}
.hdt tr.is-chosen td:first-child{box-shadow:inset 2px 0 0 var(--gilt)}
.hdt .tbl-foot{display:flex;justify-content:space-between;align-items:center;padding:12px 2px 0;font-size:12.5px;
 color:var(--text-3);flex-wrap:wrap;gap:8px}
.hdt .help{font-size:12.5px;color:var(--text-3)}
.hdt .callout{display:flex;gap:12px;padding:12px 16px;border-radius:12px;border:1px solid var(--border);
 background:var(--surface);font-size:13px;color:var(--text-2);line-height:1.55}
.hdt .callout>.i{margin-top:1px;color:var(--text-3)}
.hdt .callout.c-warn{border-color:color-mix(in srgb,var(--warn) 45%,transparent);background:var(--warn-bg);color:var(--text)}
.hdt .callout.c-neg{border-color:color-mix(in srgb,var(--neg) 45%,transparent);background:var(--neg-bg);color:var(--text)}
.hdt .callout.c-info{border-color:color-mix(in srgb,var(--info) 40%,transparent);background:var(--info-bg);color:var(--text)}
.hdt .callout.c-pos{border-color:color-mix(in srgb,var(--pos) 40%,transparent);background:var(--pos-bg);color:var(--text)}
.hdt .callout.c-warn>.i{color:var(--warn)}.hdt .callout.c-neg>.i{color:var(--neg)}
.hdt .callout.c-info>.i{color:var(--info)}.hdt .callout.c-pos>.i{color:var(--pos)}
.hdt .empty{display:flex;gap:16px;align-items:flex-start;padding:20px;border:1px dashed var(--border-2);border-radius:12px;
 background:var(--surface)}
.hdt .empty .ei{flex:none;width:40px;height:40px;border-radius:8px;display:grid;place-items:center;background:var(--gilt-soft);
 color:var(--link)}
.hdt .empty b{display:block;font-size:14px;font-weight:600;color:var(--text);line-height:1.4}
.hdt .empty p{margin-top:4px;color:var(--text-2);font-size:13px;max-width:68ch}
.hdt .list{list-style:none;margin:0;padding:0}
.hdt .list li{display:flex;gap:12px;align-items:flex-start;padding:12px 0;border-bottom:1px solid var(--border)}
.hdt .list li:last-child{border-bottom:0}
.hdt .list .t{font-size:12px;color:var(--text-3);white-space:nowrap;margin-left:auto}
.hdt .kv{display:grid;grid-template-columns:auto 1fr;gap:8px 16px;font-size:13px;margin:0}
.hdt .kv dt{color:var(--text-3)}
.hdt .kv dd{margin:0;font-family:var(--font-mono);font-variant-numeric:tabular-nums;text-align:right;overflow-wrap:anywhere}
.hdt .kv.text dd{font-family:var(--font-sans);text-align:left}
.hdt .check-list{list-style:none;margin:0;padding:0;display:grid;gap:8px}
.hdt .check-list li{display:flex;gap:12px;align-items:center;font-size:13.5px}
.hdt .ok-i{color:var(--pos)}.hdt .no-i{color:var(--neg)}.hdt .wait-i{color:var(--warn)}
.hdt .steps{counter-reset:s;list-style:none;margin:0;padding:0;display:grid;gap:12px}
.hdt .steps li{counter-increment:s;display:flex;gap:12px;align-items:flex-start;font-size:13.5px}
.hdt .steps li::before{content:counter(s);flex:none;width:24px;height:24px;border-radius:50%;background:var(--surface-2);
 border:1px solid var(--border-2);display:grid;place-items:center;font-size:12px;font-weight:600;font-family:var(--font-mono)}
.hdt .timeline{list-style:none;margin:0;padding:0 0 0 16px;border-left:1px solid var(--border-2);display:grid;gap:12px}
.hdt .timeline li{position:relative;font-size:13px}
.hdt .timeline li::before{content:"";position:absolute;left:-21px;top:6px;width:9px;height:9px;border-radius:50%;
 background:var(--gilt);border:2px solid var(--surface)}
.hdt .timeline li.t-neg::before{background:var(--neg)}.hdt .timeline li.t-pos::before{background:var(--pos)}
.hdt .timeline .when{font-family:var(--font-mono);font-size:12px;color:var(--text-3);margin-right:8px}
.hdt .divider{height:1px;background:var(--border);margin:16px 0}
.hdt .chips{display:flex;flex-wrap:wrap;gap:8px}
.hdt .chip{display:inline-flex;align-items:center;gap:6px;height:28px;padding:0 12px;border-radius:8px;
 background:var(--surface-2);border:1px solid var(--border);font-size:12.5px}
.hdt .status-pill{display:inline-flex;align-items:center;gap:8px;height:28px;padding:0 12px;border-radius:8px;
 font-size:12px;font-weight:600;letter-spacing:.04em;border:1px solid var(--border);background:var(--surface);white-space:nowrap}
.hdt .status-pill .dot{width:7px;height:7px;border-radius:50%;background:currentColor;flex:none}
.hdt .pill-info{color:var(--info);background:var(--info-bg);border-color:color-mix(in srgb,var(--info) 30%,transparent)}
.hdt .pill-warn{color:var(--warn);background:var(--warn-bg);border-color:color-mix(in srgb,var(--warn) 30%,transparent)}
.hdt .pill-neg{color:var(--neg);background:var(--neg-bg);border-color:color-mix(in srgb,var(--neg) 30%,transparent)}
.hdt .pill-pos{color:var(--pos);background:var(--pos-bg);border-color:color-mix(in srgb,var(--pos) 30%,transparent)}
.hdt .pill-mute{color:var(--text-2);background:var(--surface-2);border-color:var(--border)}
.hdt .pill-gilt{color:var(--link);background:var(--gilt-soft);border-color:color-mix(in srgb,var(--gilt) 35%,transparent)}
.hdt .top-meta{font-size:12.5px;color:var(--text-3);white-space:nowrap}
.hdt .topbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap;min-height:40px}
.hdt .topbar .spacer{flex:1}
.hdt .avatar{width:32px;height:32px;border-radius:50%;background:var(--gilt-soft);display:inline-grid;place-items:center;
 font-weight:600;color:var(--link);font-size:12px}
.hdt .user{display:inline-flex;align-items:center;gap:8px;font-size:13px;color:var(--text-2)}
.hdt .key-card .head{display:flex;gap:12px;align-items:center;margin-bottom:12px}
.hdt .key-card .logo{width:36px;height:36px;border-radius:8px;display:grid;place-items:center;background:var(--surface-2);
 font:700 12px var(--font-mono)}
.hdt .key-card .head p{color:var(--text-3);font-size:12.5px}
.hdt .key-card .head .badge{margin-left:auto}
.hdt .mode-opt{padding:16px;border-radius:12px;border:2px solid var(--border);background:var(--surface);color:var(--text)}
.hdt .mode-opt.on{border-color:var(--gilt);background:var(--gilt-soft)}
.hdt .mode-opt b{display:flex;align-items:center;gap:8px;font-size:15px;margin-bottom:4px}
.hdt .mode-opt p{color:var(--text-2);font-size:13px}
.hdt .danger-zone h2{color:var(--neg);display:flex;align-items:center;gap:8px}
.hdt .save-bar p{color:var(--text-2);font-size:13px}
.hdt .login-note{display:flex;gap:8px;font-size:12.5px;color:var(--text-3);justify-content:center;text-align:center}
.hdt .brand{display:flex;align-items:center;gap:12px}
.hdt .brand-mark{width:32px;height:32px;border-radius:8px;background:var(--tile) var(--brand-mark) no-repeat center/22px;flex:none}
.hdt .brand b{display:block;font-size:14px}.hdt .brand span{font-size:12px;color:var(--text-3)}
.hdt .wordmark{display:block;width:124px;height:24px;background-color:var(--text);
 -webkit-mask:var(--wordmark) no-repeat left center/contain;mask:var(--wordmark) no-repeat left center/contain}
.hdt .brand-lg{flex-direction:column;gap:16px;padding:32px 0 24px;text-align:center}
.hdt .brand-lg .brand-mark{width:64px;height:64px;border-radius:14px;background-size:44px}
.hdt .brand-lg .wordmark{width:144px;height:28px;margin:0 auto 8px}
.hdt .brand-lg span.sub{font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:var(--text-3)}
.hdt .side-note{font-size:12px;line-height:1.55;color:var(--text-3);padding:4px 0 0}
.hdt .side-note b{display:block;color:var(--text-2);font-weight:600;margin-bottom:4px}
.hdt .legend{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px;font-size:12.5px;color:var(--text-2)}
.hdt .legend span{display:inline-flex;align-items:center;gap:8px}
.hdt .legend i{display:inline-block;width:22px;height:0;border-top:2.5px solid currentColor}
.hdt .sr{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap}
@media (max-width:767px){.hdt .kpis{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.hdt .kpi{padding:12px}
 .hdt .kpi .val{font-size:19px;margin-top:8px}.hdt h1{font-size:22px}.hdt .top-meta.hide-sm{display:none}
 .hdt .hero{grid-template-columns:1fr;padding:20px 16px;gap:16px}.hdt .hero-val{font-size:34px}
 .hdt .hero::after{right:-24px;bottom:auto;top:-16px;width:128px;height:128px;opacity:.12}.hdt .hero-meta dd{text-align:left}
 .hdt .card{padding:16px}.hdt .empty{padding:16px;gap:12px}.hdt th,.hdt td{padding:0 12px}}
@media (max-width:360px){.hdt .kpis{grid-template-columns:1fr}}
"""

# Native Streamlit widgets re-skinned through the same tokens (base theme is light in config.toml; these rules
# make the night-glass mode complete and keep both modes on the exact token values).
_NATIVE_CSS: Final[str] = """
html,body{background:var(--bg)}
[data-testid="stAppViewContainer"],[data-testid="stMain"],[data-testid="stBottom"]>div{background:transparent;color:var(--text)}
.stApp{font-family:var(--font-sans)}
::selection{background:color-mix(in srgb,var(--gilt) 32%,transparent)}
[data-testid="stHeader"]{background:transparent;border-bottom:0}
[data-testid="stAppDeployButton"],[data-testid="stMainMenu"],[data-testid="stDecoration"],[data-testid="stHeaderActionElements"]{display:none!important}
[data-testid="stMainBlockContainer"]{max-width:1440px;padding-top:3.5rem;padding-left:24px;padding-right:24px;padding-bottom:64px}
[data-testid="stSidebar"]{background:var(--surface);border-right:1px solid var(--border);min-width:256px;max-width:256px}
[data-testid="stSidebar"] *{color:var(--text-2)}
[data-testid="stSidebarHeader"]{padding-top:20px;padding-bottom:12px}
[data-testid="stSidebarLogo"],[data-testid="stHeaderLogo"]{height:32px;max-width:none}
[data-testid="stNavSectionHeader"],[data-testid="stNavSectionHeader"] *{font-size:11px!important;font-weight:600;
 letter-spacing:.1em;text-transform:uppercase;color:var(--text-3)!important}
[data-testid="stSidebarNavLink"]{border-radius:8px;min-height:40px;position:relative;transition:background-color 150ms var(--ease)}
[data-testid="stSidebarNavLink"]:hover{background:var(--surface-2)}
[data-testid="stSidebarNavLink"]:focus-visible{outline:2px solid var(--focus);outline-offset:1px}
[data-testid="stSidebarNavLink"][aria-current="page"]{background:var(--gilt-soft)}
[data-testid="stSidebarNavLink"][aria-current="page"] *{color:var(--text)!important;font-weight:600}
[data-testid="stSidebarNavLink"][aria-current="page"] [data-testid="stIconMaterial"]{color:var(--link)!important}
[data-testid="stSidebarNavLink"][aria-current="page"]::before{content:"";position:absolute;left:-8px;top:10px;bottom:10px;
 width:2px;border-radius:1px;background:var(--gilt)}
.stApp h1,.stApp h2,.stApp h3,.stApp h4,.stApp p,.stApp label,.stApp li{color:var(--text)}
.stApp [data-testid="stHeadingWithActionElements"] h3,.stApp [data-testid="stHeading"] h3{font-family:var(--font-sans);
 font-size:17px;font-weight:600;letter-spacing:.005em;padding:16px 0 4px}
[data-testid="stWidgetLabel"] p,[data-testid="stWidgetLabel"] label{color:var(--text)!important;font-weight:600;font-size:13px}
[data-testid="stCaptionContainer"],.stApp small{color:var(--text-3)!important}
[data-testid="stCaptionContainer"] p{color:var(--text-3)!important;font-size:12.5px}
[data-testid="stMarkdownContainer"] code{background:var(--surface-2);color:var(--text)}
[data-testid="stTextInputRootElement"],[data-testid="stTextAreaRootElement"],[data-testid="stNumberInputContainer"],
[data-testid="stDateInputField"],[data-testid="stDateTimeInputField"],[data-testid="stSelectbox"] [role="group"],
[data-testid="stMultiSelect"] [role="group"],[data-testid="stTimeInput"] [role="group"]{background:var(--surface-2)!important;
 border-color:var(--field)!important;color:var(--text)!important}
[data-testid="stTextInputRootElement"]:focus-within,[data-testid="stTextAreaRootElement"]:focus-within,
[data-testid="stNumberInputContainer"]:focus-within,[data-testid="stDateInputField"]:focus-within,
[data-testid="stSelectbox"] [role="group"]:focus-within,[data-testid="stMultiSelect"] [role="group"]:focus-within{
 border-color:var(--focus)!important;box-shadow:0 0 0 1px var(--focus)}
.stApp input,.stApp textarea,.stApp [role="combobox"],.stApp [role="spinbutton"]{color:var(--text)!important;
 -webkit-text-fill-color:var(--text)!important}
[data-testid="stSelectbox"] button,[data-testid="stMultiSelect"] button,[data-testid="stDateInput"] button{color:var(--text-2)!important}
[data-testid="stNumberInputField"],[data-testid="stTextInputField"]{font-family:var(--font-mono)}
input::placeholder,textarea::placeholder{color:var(--text-3)!important;-webkit-text-fill-color:var(--text-3)!important}
[data-testid="stNumberInputStepDown"],[data-testid="stNumberInputStepUp"]{background:var(--surface-2)!important;color:var(--text-2)!important}
body{color:var(--text)}
[data-testid="stSelectboxVirtualDropdown"],[data-testid="stMultiSelectDropdown"],[data-testid="stDateInputCalendar"],
[data-testid="stDateInputQuickSelectPopover"],[data-testid="stDateTimeInputCalendar"]{background:var(--elev)!important;
 border:1px solid var(--border-2);color:var(--text)!important;box-shadow:var(--shadow)}
[role="listbox"] [role="option"],[role="listbox"] [role="option"] *{color:var(--text)!important}
[role="listbox"] [role="option"]:hover>div{background:var(--surface-2)!important}
[role="listbox"] [role="option"][aria-selected="true"]>div{background:var(--gilt-soft)!important}
[data-testid="stDateInputCalendar"] *,[data-testid="stDateTimeInputCalendar"] *{color:var(--text)!important}
[data-testid="stDateInputCalendar"] [data-outside-month],[data-testid="stDateInputCalendar"] [data-disabled]{color:var(--text-3)!important}
[data-testid="stDateInputCalendar"] [data-selected],[data-testid="stDateTimeInputCalendar"] [data-selected]{
 background:var(--primary)!important;color:var(--on-primary)!important}
[data-testid="stMultiSelectTagsContainer"] [role="row"],[data-testid="stMultiSelectTagsContainer"] [role="gridcell"]{
 background:var(--surface)!important;color:var(--text)!important}
.stApp button{transition:background-color 150ms var(--ease),border-color 150ms var(--ease),color 150ms var(--ease),transform 100ms var(--ease)}
.stApp button:focus-visible{outline:2px solid var(--focus)!important;outline-offset:2px;box-shadow:none!important}
.stApp button:not(:disabled):active{transform:translateY(1px)}
[data-testid="stBaseButton-secondary"],[data-testid="stBaseButton-secondaryFormSubmit"]{background:var(--surface)!important;
 border:1px solid var(--border-2)!important;color:var(--text)!important;font-weight:600}
[data-testid="stBaseButton-secondary"]:hover,[data-testid="stBaseButton-secondaryFormSubmit"]:hover{background:var(--surface-2)!important;
 border-color:color-mix(in srgb,var(--gilt) 55%,var(--border-2))!important}
[data-testid="stBaseButton-primary"],[data-testid="stBaseButton-primaryFormSubmit"]{background:var(--primary)!important;
 border-color:var(--primary)!important;color:var(--on-primary)!important;font-weight:600}
[data-testid="stBaseButton-primary"] *,[data-testid="stBaseButton-primaryFormSubmit"] *{color:var(--on-primary)!important}
[data-testid="stBaseButton-primary"]:hover,[data-testid="stBaseButton-primaryFormSubmit"]:hover{background:var(--primary-h)!important;
 border-color:var(--primary-h)!important}
[data-testid="stBaseButton-tertiary"]{color:var(--text-2)!important}
[data-testid="stButtonGroup"] button[data-variant="segmented_control"]{background:var(--surface)!important;
 border-color:var(--border-2)!important;color:var(--text-2)!important;min-height:36px;padding-left:16px;padding-right:16px}
[data-testid="stButtonGroup"] button[data-variant="segmented_control"] *{color:inherit!important}
[data-testid="stButtonGroup"] button[data-variant="segmented_control"]:hover{background:var(--surface-2)!important;color:var(--text)!important}
[data-testid="stButtonGroup"] button[data-variant="segmented_control"][aria-checked="true"]{background:var(--gilt-soft)!important;
 border-color:var(--gilt)!important;color:var(--text)!important;font-weight:600;position:relative;z-index:1}
@media (max-width:767px),(pointer:coarse){.stApp button{min-height:44px!important;min-width:44px}
 [data-testid="stSidebarNavLink"]{min-height:44px}}
.stApp button:disabled{opacity:.45;cursor:not-allowed}
.st-key-danger-zone{border:1px solid color-mix(in srgb,var(--neg) 50%,transparent);border-radius:12px;padding:16px;
 background:color-mix(in srgb,var(--neg) 5%,var(--surface))}
[class*="st-key-danger-"] [data-testid="stBaseButton-primary"],[class*="st-key-danger-"] [data-testid="stBaseButton-primaryFormSubmit"]{
 background:var(--danger)!important;border-color:var(--danger)!important;color:var(--on-danger)!important}
[class*="st-key-danger-"] [data-testid="stBaseButton-primary"] *,[class*="st-key-danger-"] [data-testid="stBaseButton-primaryFormSubmit"] *{color:var(--on-danger)!important}
[class*="st-key-danger-"] [data-testid="stBaseButton-primary"]:hover{background:var(--danger-h)!important}
[class*="st-key-danger-"] [data-testid="stBaseButton-secondary"]{color:var(--neg)!important;border-color:var(--neg)!important}
.st-key-hdt-topbar{position:sticky;top:3.75rem;z-index:30;background:color-mix(in srgb,var(--bg) 92%,transparent);
 backdrop-filter:blur(8px);border-bottom:1px solid var(--border);padding:8px 0;margin-bottom:16px}
[class*="st-key-hdt-card"]{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px}
[class*="st-key-hdt-savebar"]{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:12px 16px}
[data-testid="stTabs"] [role="tablist"]{gap:8px;flex-wrap:wrap;border:0;padding:4px 0}
[data-testid="stTabs"] [role="tablist"]::after{display:none}
[data-testid="stExpandSidebarButton"],[data-testid="stExpandSidebarButton"] *,[data-testid="stSidebarCollapseButton"] *{
 color:var(--text-2)!important}
[data-testid="stTab"]{height:36px;padding:0 16px!important;margin:0!important;border-radius:999px!important;
 border:1px solid var(--border-2)!important;background:var(--surface)!important;color:var(--text-2)!important;font-weight:600;
 transition:color 150ms var(--ease),background-color 150ms var(--ease),border-color 150ms var(--ease)}
[data-testid="stTab"] *{color:inherit!important;font-weight:600}
[data-testid="stTab"]:hover{color:var(--text)!important;background:var(--surface-2)!important}
[data-testid="stTab"][aria-selected="true"]{color:var(--text)!important;background:var(--gilt-soft)!important;
 border-color:var(--gilt)!important}
[data-testid="stTab"]:focus-visible{outline:2px solid var(--focus);outline-offset:2px}
[data-testid="stTabs"] .react-aria-SelectionIndicator{display:none!important}
[data-testid="stDialog"] [role="dialog"]{background:var(--surface)!important;border:1px solid var(--border-2);
 border-radius:12px;color:var(--text);box-shadow:var(--shadow)}
[data-testid="stDialog"] [role="dialog"] *{color:var(--text)}
[data-testid="stExpander"] details{background:var(--surface);border:1px solid var(--border)!important;border-radius:12px}
[data-testid="stExpander"] summary *{color:var(--text)!important}
[data-testid="stToast"]{background:var(--elev)!important;border:1px solid var(--border-2);color:var(--text)!important;
 box-shadow:var(--shadow)}
[data-testid="stToast"] *{color:var(--text)!important}
[data-testid="stCheckbox"] label *,[data-testid="stRadio"] label *{color:var(--text)!important}
[data-testid="stForm"]{border:1px solid var(--border)!important;border-radius:12px;background:var(--surface);padding:24px}
[data-testid="stVegaLiteChart"]{background:transparent}
#vg-tooltip-element{background:var(--elev);color:var(--text);border:1px solid var(--border-2);border-radius:8px;
 font-family:var(--font-mono);font-size:12px;box-shadow:var(--shadow)}
#vg-tooltip-element td.key{color:var(--text-3)}
[data-testid="stAlert"]{border-radius:12px}
[data-testid="stSpinner"] *{color:var(--text-2)!important}
@media (prefers-reduced-motion:reduce){*,*::before,*::after{animation:none!important;transition:none!important}}
"""

# The glass layer: aurora canvas, frosted panels and the trading details. Last in the cascade on purpose, so it
# sets shape and depth for the component and native rules above. Blur only where content sits on the aurora.
_GLASS_CSS: Final[str] = """
:root{--blur:blur(18px) saturate(160%);--r-card:20px;--r-ctl:14px}
.stApp{background:
 radial-gradient(ellipse 46% 42% at 6% 4%,var(--aurora-1),transparent 72%),
 radial-gradient(ellipse 42% 40% at 96% 8%,var(--aurora-2),transparent 72%),
 radial-gradient(ellipse 50% 46% at 84% 96%,var(--aurora-3),transparent 72%),
 radial-gradient(ellipse 44% 42% at 18% 92%,var(--aurora-4),transparent 72%),
 var(--bg)!important;background-attachment:fixed!important;color:var(--text)}
.hdt .card,.hdt .kpi,.hdt .hero,.hdt .tbl-wrap,.hdt .empty,.hdt .callout,.hdt .mode-opt,
[class*="st-key-hdt-card"],[class*="st-key-hdt-savebar"],[data-testid="stForm"],[data-testid="stExpander"] details,
.st-key-danger-zone{background-color:var(--surface);-webkit-backdrop-filter:var(--blur);backdrop-filter:var(--blur);
 border:1px solid var(--glass-edge)!important;box-shadow:var(--glass-hi),var(--shadow);border-radius:var(--r-card)}
.hdt .callout{border-radius:var(--r-ctl)}
.hdt .callout.c-warn{background:linear-gradient(var(--warn-bg),var(--warn-bg)),var(--surface)}
.hdt .callout.c-neg{background:linear-gradient(var(--neg-bg),var(--neg-bg)),var(--surface)}
.hdt .callout.c-info{background:linear-gradient(var(--info-bg),var(--info-bg)),var(--surface)}
.hdt .callout.c-pos{background:linear-gradient(var(--pos-bg),var(--pos-bg)),var(--surface)}
.hdt .empty{border-style:dashed!important;border-color:var(--border-2)!important}
.hdt .mode-opt.on{border-color:var(--gilt)!important;background:linear-gradient(var(--gilt-soft),var(--gilt-soft)),var(--surface)}
.st-key-danger-zone{border-color:color-mix(in srgb,var(--neg) 50%,transparent)!important;
 background:linear-gradient(var(--neg-bg),var(--neg-bg)),var(--surface)}
[data-testid="stForm"]{padding:28px}
.hdt .hero{padding:28px;background:linear-gradient(120deg,color-mix(in srgb,var(--gilt) 10%,transparent),transparent 55%),var(--surface)}
.hdt .hero::before{left:24px;right:24px;height:2px;border-radius:2px}
.hdt .kpis{gap:12px}
.hdt .kpi{padding:16px 18px}
.hdt .kpi::before{left:18px;right:auto;width:28px;top:0;height:2px;border-radius:0 0 2px 2px;background:var(--gilt)}
.hdt .kpi .lab::after{content:"";flex:1;height:1px;background:linear-gradient(90deg,var(--border),transparent)}
.hdt .kpi .val{letter-spacing:-.01em}
.hdt .tbl-wrap{padding:0}
.hdt th{background:var(--glass-strong);-webkit-backdrop-filter:var(--blur);backdrop-filter:var(--blur);border-bottom-color:var(--border)}
.hdt tbody tr:nth-child(even){background:var(--zebra)}
.hdt tbody tr:hover{background:var(--surface-2)}
.hdt tbody tr.is-chosen{background:var(--gilt-soft)}
.hdt .badge,.hdt .status-pill,.hdt .chip,.hdt .tag{border-radius:999px}
.hdt .badge,.hdt .tag{padding:0 10px}
.hdt .status-pill{background:var(--glass-strong);-webkit-backdrop-filter:var(--blur);backdrop-filter:var(--blur);
 box-shadow:var(--glass-hi)}
.hdt .status-pill.pill-info{background:linear-gradient(var(--info-bg),var(--info-bg)),var(--glass-strong)}
.hdt .status-pill.pill-warn{background:linear-gradient(var(--warn-bg),var(--warn-bg)),var(--glass-strong)}
.hdt .status-pill.pill-neg{background:linear-gradient(var(--neg-bg),var(--neg-bg)),var(--glass-strong)}
.hdt .status-pill.pill-pos{background:linear-gradient(var(--pos-bg),var(--pos-bg)),var(--glass-strong)}
.hdt .status-pill.pill-gilt{background:linear-gradient(var(--gilt-soft),var(--gilt-soft)),var(--glass-strong)}
.hdt .dot.live{box-shadow:0 0 0 3px color-mix(in srgb,currentColor 25%,transparent),0 0 10px currentColor;
 animation:hdt-pulse 2.4s ease-in-out infinite}
@keyframes hdt-pulse{50%{box-shadow:0 0 0 6px color-mix(in srgb,currentColor 0%,transparent),0 0 4px currentColor}}
.hdt .side-long,.hdt .side-short{display:inline-flex;align-items:center;height:24px;padding:0 10px;border-radius:999px;
 font-size:12px;letter-spacing:.04em;font-family:var(--font-mono);border:1px solid transparent}
.hdt .side-long{background:var(--pos-bg);border-color:color-mix(in srgb,var(--pos) 25%,transparent)}
.hdt .side-short{background:var(--neg-bg);border-color:color-mix(in srgb,var(--neg) 25%,transparent)}
.hdt .meter{background:var(--surface-2)}
.hdt .meter>span{background:var(--primary-grad)}
.hdt .empty .ei,.hdt .avatar{background:var(--gilt-soft);box-shadow:var(--glass-hi)}
.hdt .brand-mark{box-shadow:0 6px 16px rgba(14,12,20,.18)}
.hdt .brand-lg{padding:16px 0 20px}
[data-testid="stHeader"]{background:var(--glass-strong);-webkit-backdrop-filter:var(--blur);backdrop-filter:var(--blur);
 border-bottom:1px solid var(--border)}
[data-testid="stSidebar"]{background:var(--glass-strong)!important;-webkit-backdrop-filter:var(--blur);backdrop-filter:var(--blur);
 border-right:1px solid var(--glass-edge);box-shadow:var(--shadow)}
[data-testid="stSidebar"]>div{background:transparent}
[data-testid="stSidebarNavLink"]{border-radius:12px}
[data-testid="stSidebarNavLink"][aria-current="page"]{background:var(--surface);box-shadow:var(--glass-hi),0 2px 8px rgba(23,20,31,.06);
 border:1px solid var(--glass-edge)}
.st-key-hdt-topbar{background:var(--glass-strong)!important;-webkit-backdrop-filter:var(--blur);backdrop-filter:var(--blur);
 border:1px solid var(--glass-edge)!important;border-radius:var(--r-card);box-shadow:var(--glass-hi),var(--shadow);
 padding:8px 12px!important;top:4.25rem!important}
.stApp button{border-radius:var(--r-ctl)!important}
[data-testid="stBaseButton-secondary"],[data-testid="stBaseButton-secondaryFormSubmit"]{background:var(--glass-strong)!important;
 border-color:var(--glass-edge)!important;box-shadow:var(--glass-hi),0 1px 2px rgba(23,20,31,.06)}
[data-testid="stBaseButton-secondary"]:hover,[data-testid="stBaseButton-secondaryFormSubmit"]:hover{background:var(--surface)!important;
 border-color:color-mix(in srgb,var(--gilt) 55%,transparent)!important}
[data-testid="stBaseButton-primary"],[data-testid="stBaseButton-primaryFormSubmit"]{background:var(--primary-grad)!important;
 border:1px solid color-mix(in srgb,var(--gilt) 70%,transparent)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.45),var(--primary-glow)}
[data-testid="stBaseButton-primary"]:hover,[data-testid="stBaseButton-primaryFormSubmit"]:hover{background:var(--primary-grad-h)!important;
 transform:translateY(-1px)}
[class*="st-key-danger-"] [data-testid="stBaseButton-primary"],[class*="st-key-danger-"] [data-testid="stBaseButton-primaryFormSubmit"]{
 background:var(--danger)!important;box-shadow:none}
[data-testid="stButtonGroup"] button[data-variant="segmented_control"]{background:var(--glass-strong)!important;
 border-color:var(--glass-edge)!important;border-radius:999px!important}
[data-testid="stButtonGroup"] button[data-variant="segmented_control"][aria-checked="true"]{
 background:linear-gradient(var(--gilt-soft),var(--gilt-soft)),var(--glass-strong)!important;border-color:var(--gilt)!important}
[data-testid="stTextInputRootElement"],[data-testid="stTextAreaRootElement"],[data-testid="stNumberInputContainer"],
[data-testid="stDateInputField"],[data-testid="stSelectbox"] [role="group"],[data-testid="stMultiSelect"] [role="group"]{
 background:var(--glass-strong)!important;border-radius:var(--r-ctl)!important;box-shadow:inset 0 1px 2px rgba(23,20,31,.06)}
[data-testid="stTabs"] [role="tablist"]{background:transparent;box-shadow:none}
[data-testid="stTab"]{background:var(--glass-strong)!important;-webkit-backdrop-filter:var(--blur);backdrop-filter:var(--blur);
 border-color:var(--glass-edge)!important;box-shadow:var(--glass-hi),0 1px 2px rgba(23,20,31,.06)}
[data-testid="stTab"]:hover{background:var(--surface)!important;border-color:color-mix(in srgb,var(--gilt) 55%,transparent)!important}
[data-testid="stTab"][aria-selected="true"]{background:linear-gradient(var(--gilt-soft),var(--gilt-soft)),var(--glass-strong)!important;
 border-color:var(--gilt)!important}
[data-testid="stDialog"] [role="dialog"],[data-testid="stToast"],[data-testid="stSelectboxVirtualDropdown"],
[data-testid="stMultiSelectDropdown"],[data-testid="stDateInputCalendar"]{border-radius:var(--r-card)!important;
 border-color:var(--glass-edge)!important}
[data-testid="stDialog"] [role="dialog"]{background:var(--glass-solid)!important;-webkit-backdrop-filter:var(--blur);backdrop-filter:var(--blur)}
[data-testid="stVegaLiteChart"]{box-sizing:border-box;padding:16px 0 8px;background:var(--surface);
 -webkit-backdrop-filter:var(--blur);backdrop-filter:var(--blur);border:1px solid var(--glass-edge);
 border-radius:var(--r-card);box-shadow:var(--glass-hi),var(--shadow)}
[data-testid="stTextInputRootElement"] button,[data-testid="stTextInputRootElement"] button *,[data-testid="stTooltipIcon"],
[data-testid="stTooltipIcon"] *,[data-testid="stTooltipHoverTarget"],[data-testid="stTooltipHoverTarget"] *,
[data-testid="stWidgetLabel"] svg{color:var(--text-2)!important}
[data-testid="stTooltipHoverTarget"] svg{stroke:var(--text-2)!important}
[data-testid="stAlert"]>div{border-radius:var(--r-ctl);-webkit-backdrop-filter:var(--blur);backdrop-filter:var(--blur)}
#vg-tooltip-element{background:var(--glass-solid);border-radius:12px}
[data-testid="stHorizontalBlock"]:has([class*="st-key-hdt-card-panel-"]){align-items:stretch}
[data-testid="stColumn"]:has([class*="st-key-hdt-card-panel-"])>[data-testid="stVerticalBlock"]{height:100%}
[class*="st-key-hdt-card-panel-"]{padding:20px 24px!important;gap:8px}
[class*="st-key-hdt-card-panel-"] [data-testid="stHeading"] h3{padding-top:0!important}
[class*="st-key-hdt-card-panel-"] [data-testid="stVegaLiteChart"]{padding:8px 0 0;background:transparent;border:0;
 box-shadow:none;-webkit-backdrop-filter:none;backdrop-filter:none}
[class*="st-key-hdt-card-panel-"] .hdt .card,[class*="st-key-hdt-card-panel-"] .hdt .empty{box-shadow:none;
 -webkit-backdrop-filter:none;backdrop-filter:none;background:transparent}
[class*="st-key-hdt-card-panel-"] .hdt .card{border:0!important;padding:0}
[data-testid="stSidebarContent"]:has(.st-key-hdt-side-foot){display:flex;flex-direction:column}
[data-testid="stSidebarUserContent"]:has(.st-key-hdt-side-foot){flex:1 1 auto;display:flex;flex-direction:column}
[data-testid="stSidebarUserContent"]:has(.st-key-hdt-side-foot)>div{flex:1 1 auto;display:flex;flex-direction:column}
[data-testid="stSidebarUserContent"]:has(.st-key-hdt-side-foot)>div>[data-testid="stVerticalBlock"]{flex:1 1 auto}
[data-testid="stLayoutWrapper"]:has(>.st-key-hdt-side-foot){margin-top:auto}
.st-key-hdt-side-foot{justify-content:space-between;padding-top:12px;border-top:1px solid var(--border)}
@media (max-width:767px){.hdt .hide-sm{display:none}.st-key-hdt-topbar{border-radius:var(--r-ctl);padding:6px 8px!important}
 .hdt .hero{padding:20px 16px}.hdt .kpi{padding:12px 14px}[data-testid="stForm"]{padding:20px 16px}}
@media (prefers-reduced-transparency:reduce){:root{--surface:var(--glass-solid);--glass-strong:var(--glass-solid)}
 *,*::before,*::after{-webkit-backdrop-filter:none!important;backdrop-filter:none!important}}
@supports not ((backdrop-filter:blur(1px)) or (-webkit-backdrop-filter:blur(1px))){
 :root{--surface:var(--glass-solid);--glass-strong:var(--glass-solid)}}
"""


def stylesheet(mode: ThemeMode) -> str:
    """Complete `<style>` block for the given theme mode."""
    return (
        f"<style>{root_variables(mode)}{_ICON_CSS}{_BRAND_CSS}{_COMPONENT_CSS}{_NATIVE_CSS}{_GLASS_CSS}</style>"
    )


def chart_palette(mode: ThemeMode) -> dict[str, str]:
    """Concrete colors for Altair charts (charts are rendered with explicit colors, not the Streamlit theme)."""
    t = TOKENS[mode]
    return {
        "background": "transparent",
        "text": t["text-2"],
        "grid": t["grid"],
        "line": t["gilt"],
        "area": t["gilt-area"],
        "pos": t["pos"],
        "neg": t["neg"],
        "warn": t["warn"],
        "muted": t["text-3"],
        "cmc": t["cmc"],
        "bnb": t["bnb"],
        "llm": t["llm"],
        "surface": t["glass-solid"],
    }


# Agent series in the weight chart: distinct color AND stroke pattern (never color alone).
AGENT_SERIES: Final[tuple[tuple[str, str, tuple[int, ...]], ...]] = (
    ("crowding", "cmc", ()),
    ("technical", "link", (6, 4)),
    ("micro", "pos", (2, 3)),
    ("fundamental", "bnb", (10, 3, 2, 3)),
    ("news", "llm", (1, 5)),
    ("macro", "warn", (8, 2)),
)
