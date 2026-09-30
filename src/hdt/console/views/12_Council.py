"""Council quorum, debate and manager policy with audit history."""

from hdt.console.views._shared import config_editor, version_history
from hdt.settings.schemas import Section

config_editor(Section.COUNCIL, description="Quorum, agreement thresholds, debate rounds and manager rules.")
version_history(Section.COUNCIL)
