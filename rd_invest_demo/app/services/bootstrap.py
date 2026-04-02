from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..config import AUTO_LOAD_SOURCE_DATA, SOURCE_FILES
from ..db import db_session
from .data_loader import load_all_sources

logger = logging.getLogger(__name__)


def source_file_status() -> dict[str, Any]:
    files: dict[str, str] = {}
    missing: list[str] = []
    for key, path in SOURCE_FILES.items():
        resolved = str(Path(path).resolve())
        files[key] = resolved
        if not Path(path).exists():
            missing.append(key)
    return {
        "auto_load_enabled": AUTO_LOAD_SOURCE_DATA,
        "files": files,
        "missing": missing,
    }


def auto_load_initial_data_if_needed() -> dict[str, Any]:
    status = source_file_status()
    if not AUTO_LOAD_SOURCE_DATA:
        return {"loaded": False, "reason": "disabled", **status}
    if status["missing"]:
        logger.warning("Skip initial data load, missing source files: %s", ", ".join(status["missing"]))
        return {"loaded": False, "reason": "missing_source_files", **status}

    with db_session() as conn:
        existing = conn.execute("SELECT 1 FROM import_log LIMIT 1").fetchone()
    if existing:
        return {"loaded": False, "reason": "already_initialized", **status}

    try:
        stats = load_all_sources()
        logger.info("Initial source data loaded: %s", stats)
        return {"loaded": True, "reason": "loaded", "stats": stats, **status}
    except Exception:  # noqa: BLE001
        logger.exception("Initial source data load failed")
        return {"loaded": False, "reason": "load_failed", **status}
