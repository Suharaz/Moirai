"""Owner self-tests of the `llm` scope secrets, registered on the `llm` scope `OwnerWorker` (council)."""

from __future__ import annotations

from typing import Final

from hdt.llm import deepseek_key_check, openrouter_key_check
from hdt.vault.owner import SecretCheck

LLM_SECRET_CHECKS: Final[dict[str, SecretCheck]] = {
    openrouter_key_check.SECRET_NAME: openrouter_key_check.owner_check,
    deepseek_key_check.SECRET_NAME: deepseek_key_check.owner_check,
}
