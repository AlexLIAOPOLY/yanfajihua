from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
WORK_DIR = BASE_DIR.parent


def _path_from_env(env_name: str, default: Path) -> Path:
    raw = (os.getenv(env_name) or "").strip()
    if not raw:
        return default
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = BASE_DIR / candidate
    return candidate


DB_PATH = _path_from_env("DB_PATH", BASE_DIR / "data" / "rd_demo.db")
UPLOAD_DIR = _path_from_env("UPLOAD_DIR", BASE_DIR / "uploads")
TMP_DIR = _path_from_env("TMP_DIR", BASE_DIR / "tmp")

SOURCE_FILES = {
    "dept_target": WORK_DIR / "2026年度研发投入目标-分部门.xlsx",
    "project_progress": WORK_DIR / "2026年研发项目进展表模板.xlsx",
    "labor_monthly": WORK_DIR / "人工成本分析-按月度.xlsx",
    "plan_data": WORK_DIR / "研发计划数据.xlsx",
    "timesheet": WORK_DIR / "研发费用工时记录表2026 (XXX研发项目).xlsx",
    "dashboard": WORK_DIR / "研发项目双周报表-看板主表.xlsx",
    "finance_1031": WORK_DIR / "项目维度研发经费投入表2025_10月31日.xlsx",
    "finance_prev": WORK_DIR / "项目维度研发经费投入表2025_10月XX日.xlsx",
}

DEFAULT_REPORT_MONTH = (os.getenv("DEFAULT_REPORT_MONTH") or "2025-11").strip()
try:
    DEFAULT_REPORT_YEAR = int((os.getenv("DEFAULT_REPORT_YEAR") or "2025").strip())
except ValueError:
    DEFAULT_REPORT_YEAR = 2025

AUTO_LOAD_SOURCE_DATA = (os.getenv("AUTO_LOAD_SOURCE_DATA") or "true").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
