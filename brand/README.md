# Moirai brand

Moirai is the product name of this repository (package `hdt`). The full guide is `brand-book.html`; open it through a local server from the repository root so the fonts and images load:

```
.venv/Scripts/python.exe -m http.server 8765 --bind 127.0.0.1
# then http://127.0.0.1:8765/brand/brand-book.html
```

A static preview is `export/brand-book-preview.png`.

## Idea
The Moirai spin, measure and cut the thread of fate. Clotho spins (the council's thesis), Lachesis measures (Risk sizes and signs), Atropos cuts (the stop). The mark is one thread in three strands drawing an M, which is also a price path; a thin oxblood blade crosses the last leg. Tagline: **Spin · Measure · Cut**.

## Files
| File | Use |
|---|---|
| `logo/moirai-mark.svg`, `-ink.svg` | Three-strand mark, 48 px and up. Gilt on dark, ink on light |
| `logo/moirai-mark-solid.svg`, `-ink.svg` | One-strand mark, 24 to 47 px |
| `logo/favicon.svg`, `export/favicon-16.png`, `export/favicon-32.png` | Browser tab icon |
| `logo/moirai-app-icon.svg`, `export/app-icon-512.png`, `export/apple-touch-icon-180.png` | Telegram bot avatar, app icon (safe in a circle crop) |
| `logo/moirai-wordmark.svg`, `-ink.svg` | Wordmark alone |
| `logo/moirai-lockup-horizontal.svg`, `-ink.svg` | Default lockup: console header, documents |
| `logo/moirai-lockup-stacked.svg`, `-ink.svg` | Covers, splash screens |
| `export/og-card-1200x630.png` | Link preview card |
| `export/x-header-1500x500.png` | Social header |
| `export/business-card-front.png`, `-back.png` | 85 x 55 mm card at 300 dpi (the back is a template) |
| `brand.css` | Colour and type tokens |

## Tokens
| Name | Hex | Use |
|---|---|---|
| Obsidian | `#0B0A0F` | ground |
| Night | `#16131D` | surfaces |
| Gilt | `#C9A55C` | the thread, accents |
| Bone | `#ECE6D8` | text on dark |
| Ash | `#8C8697` | secondary text |
| Oxblood | `#B23A34` on dark, `#8E2424` on light | the blade only, never text on dark |
| Ink | `#17141F` | mark on light |
| Paper | `#F4F0E6` | light ground |

Type: Cinzel (display capitals only), Fira Sans (text), Fira Code with tabular figures (numbers). Gain and loss in data keep the console colours; Oxblood is not a loss colour.

## Editing
The logo SVGs are generated from their geometry by `scripts/brand_logo.py`. Change a parameter there, run it, then re-export the PNGs: serve the repository root as above, open `brand/applications.html` and screenshot each `.art` element at its exact size (1200x630, 1500x500, 512x512, 1004x650); favicons are the SVGs rendered at 16, 32 and 180 px.
