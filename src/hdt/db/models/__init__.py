"""Business table models, one module per area. Modules are discovered automatically; never register here."""

from __future__ import annotations

import importlib
import pkgutil


def import_all_models() -> list[str]:
    """Import every submodule so all tables are registered on `hdt.db.base.Base.metadata`."""
    imported: list[str] = []
    for info in sorted(pkgutil.iter_modules(__path__), key=lambda i: i.name):
        name = f"{__name__}.{info.name}"
        importlib.import_module(name)
        imported.append(name)
    return imported
