import os
from typing import Callable, Optional

def detect_interest(text: str) -> Optional[str]:
    """
    Returns 'interested', 'not_interested', or None if inconclusive.

    Rules:
    - NOT_INTERESTED is always checked first to prevent false positives.
    - INTERESTED only matches specific intent phrases — not bare "yes", "okay",
      "sure" which are acknowledgements that can appear in any context.
    - Bare single-word replies ("yes", "no", "okay") are handled separately
      using whole-word matching so "yes" doesn't fire inside "yes, remove me".
    """
    lowered = text.lower().strip()

    # 1. Check NOT_INTERESTED first (highest priority)
    for phrase in NOT_INTERESTED_SIGNALS:
        if phrase in lowered:
            return "not_interested"

    # 2. Check specific INTERESTED phrases
    for phrase in INTERESTED_SIGNALS:
        if phrase in lowered:
            return "interested"

    # 3. Bare single-word replies — only fire if the utterance is very short
    #    and contains ONLY the acknowledgement word (no negation context).
    bare_interested = {"yes", "yep", "yup", "yeah", "sure", "okay", "ok", "alright", "fine"}
    bare_not_interested = {"no", "nope", "nah", "never"}

    # Only treat as bare reply if the whole utterance is ≤ 4 words
    words = lowered.split()
    if len(words) <= 4:
        if words and words[0] in bare_not_interested:
            return "not_interested"
        if words and words[0] in bare_interested and "not" not in words and "don't" not in words:
            return "interested"

    return None

# ─────────────────────────────────────────────────────────────
# Interest signals
# ─────────────────────────────────────────────────────────────

# ── Specific interest phrases (require real intent, not just acknowledgements) ──
INTERESTED_SIGNALS = [
    "i am interested",
    "i'm interested",
    "yes i am",
    "yes i'm",
    "yes, i am",
    "yes, i'm",
    "i am looking",
    "i'm looking",
    "i am actively looking",
    "definitely interested",
    "absolutely interested",
    "sounds good",
    "i would like to",
    "please proceed",
    "go ahead",
    "open to",
    "keen on",
    "looking forward",
    "great opportunity",
    "tell me more",
    "send the whatsapp",
    "send it",
    "share my resume",
    "i'll share",
    "i will share",
    "yes please",
    "yes, please",
    "i want to apply",
    "i want this",
    "count me in",
]

# ── NOT_INTERESTED covers: no-interest, DNC, hostile, wrong-number, distress signals ──
NOT_INTERESTED_SIGNALS = [
    # Explicit disinterest (Sc 02)
    "not interested",
    "no thank you",
    "no thanks",
    "not looking",
    "not actively looking",
    "happy where i am",
    "not available",
    "not right now",
    "not suitable",
    "not for me",
    "declined",
    "no opportunity",
    "i have a job",
    "already placed",
    "got a job",
    # Do-not-call requests (Sc 11)
    "remove my number",
    "remove me",
    "please remove",
    "take me off",
    "take off my number",
    "don't call me",
    "do not call",
    "don't call",
    "stop calling",
    "stop contacting",
    "no more calls",
    "don't disturb",
    "don't contact",
    "unsubscribe",
    "opt out",
    "don't want to be contacted",
    # Hostile (Sc 06)
    "stop bothering",
    "why do you keep calling",
    "always calling me",
    "i'll report",
    "i will report",
    "harassment",
    "this is harassment",
    # Wrong number (Sc 05)
    "wrong number",
    "not arun",
    "no one by that name",
    "he's not here",
    "she's not here",
    "they're not here",
    # Distress signals (Sc 04) — captured as not_interested to stop qualification script
    "lost my job",
    "lost job",
    "i was laid off",
    "got laid off",
    "recently laid off",
    "lost my work",
    "no job",
    "urgently need",
    "desperately need",
    "financial pressure",
    "financial crisis",
    "can't afford",
    "struggling financially",
]