from __future__ import annotations

import re

MAX_INPUT_LEN = 2200
MAX_OUTPUT_LEN = 10000

INJECTION_RULES: list[tuple[str, str]] = [
    (r"ignore\s+(all|previous)\s+instructions", "ignore-instructions"),
    (r"you\s+are\s+now\s+(developer|system)", "role-hijack"),
    (r"(system|developer)\s+prompt", "prompt-exfiltration"),
    (r"reveal\s+(secret|api[_ -]?key|token)", "secret-exfiltration"),
    (r"(execute|run)\s+(shell|command|sql)", "command-injection"),
]

SECRET_PATTERNS: list[tuple[str, str]] = [
    (r"sk-[A-Za-z0-9]{16,}", "sk-***"),
    (r"(?i)(api[_ -]?key\s*[:=]\s*)([A-Za-z0-9_\-]{12,})", r"\1***"),
    (r"(?i)(authorization\s*:\s*bearer\s+)([A-Za-z0-9._\-]{12,})", r"\1***"),
]


def _normalize(text: str) -> str:
    no_ctrl = text.replace("\x00", "").replace("\r", "\n")
    no_zero_width = re.sub(r"[\u200B-\u200F\uFEFF]", "", no_ctrl)
    return no_zero_width.strip()


def sanitize_input_text(text: str, *, label: str = "input", max_len: int = MAX_INPUT_LEN) -> str:
    clean = _normalize(text or "")
    if not clean:
        raise ValueError(f"{label} 不能为空")
    if len(clean) > max_len:
        raise ValueError(f"{label} 过长，最多 {max_len} 字符")
    return clean


def detect_prompt_injection(text: str) -> list[str]:
    low = (text or "").lower()
    hits: list[str] = []
    for pattern, name in INJECTION_RULES:
        if re.search(pattern, low):
            hits.append(name)
    return hits


def enforce_safe_user_prompt(text: str, *, label: str = "input", max_len: int = MAX_INPUT_LEN) -> str:
    clean = sanitize_input_text(text, label=label, max_len=max_len)
    hits = detect_prompt_injection(clean)
    if hits:
        rules = ", ".join(hits[:3])
        raise ValueError(f"{label} 命中高风险注入特征: {rules}")
    return clean


def redact_secrets(text: str) -> str:
    out = text or ""
    for pattern, repl in SECRET_PATTERNS:
        out = re.sub(pattern, repl, out)
    return out


def sanitize_output_text(text: str, *, max_len: int = MAX_OUTPUT_LEN) -> str:
    clean = _normalize(redact_secrets(text or ""))
    if len(clean) <= max_len:
        return clean
    return clean[:max_len] + "\n...[truncated]"
