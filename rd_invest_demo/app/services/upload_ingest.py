from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ..db import db_session
from .data_loader import extract_project_code, insert_import_log, normalize_text, to_float


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        return pd.read_excel(path)
    if suffix in (".csv", ".tsv"):
        sep = "\t" if suffix == ".tsv" else ","
        return pd.read_csv(path, sep=sep)
    raise ValueError("不支持的文件类型")


def _pick_column(columns: list[str], candidates: list[str]) -> str | None:
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand in columns:
            return cand
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def import_erp(path: Path) -> int:
    df = _read_table(path).fillna("")
    cols = [str(c).strip() for c in df.columns]
    code_col = _pick_column(cols, ["project_code", "项目编码", "研发项目编码"])
    month_col = _pick_column(cols, ["month", "月份", "期间"])
    doc_col = _pick_column(cols, ["doc_type", "单据类型", "类型"])
    amt_col = _pick_column(cols, ["amount_hkd", "金额", "金额(港币)", "港币金额"])
    cur_col = _pick_column(cols, ["currency", "币种"])
    if not all([code_col, month_col, doc_col, amt_col]):
        raise ValueError("ERP 文件字段不完整，需要 project_code/month/doc_type/amount_hkd")

    loaded = 0
    with db_session() as conn:
        for _, r in df.iterrows():
            code = extract_project_code(normalize_text(r[code_col])) or normalize_text(r[code_col])
            if not code:
                continue
            month = normalize_text(r[month_col])[:7]
            doc_type = normalize_text(r[doc_col]).upper()
            amount = to_float(r[amt_col])
            currency = normalize_text(r[cur_col]) if cur_col else "HKD"
            if not month or amount is None:
                continue
            conn.execute(
                """
                INSERT INTO erp_pr_po (month, project_code, doc_type, amount_hkd, currency, source_file)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (month, code, doc_type or "PO", amount, currency or "HKD", path.name),
            )
            conn.execute(
                """
                INSERT INTO monthly_costs
                (month, year, project_code, dept_code, cost_class, cost_component, amount_hkd, source_file, raw_tag)
                VALUES (?, ?, ?, (SELECT dept_code FROM projects WHERE project_code = ?), 'OPEX', 'OUTSOURCED', ?, ?, 'erp_upload')
                """,
                (month, int(month[:4]), code, code, amount, path.name),
            )
            loaded += 1
        insert_import_log(conn, path.name, loaded, "ERP PO/PR 导入")
    return loaded


def import_attendance(path: Path) -> int:
    df = _read_table(path).fillna("")
    cols = [str(c).strip() for c in df.columns]
    name_col = _pick_column(cols, ["employee_name", "员工姓名", "姓名"])
    date_col = _pick_column(cols, ["work_date", "日期"])
    hour_col = _pick_column(cols, ["total_hours", "总工时", "考勤工时"])
    present_col = _pick_column(cols, ["present", "是否出勤", "出勤"])
    if not all([name_col, date_col, hour_col]):
        raise ValueError("考勤文件字段不完整，需要 employee_name/work_date/total_hours")

    loaded = 0
    with db_session() as conn:
        for _, r in df.iterrows():
            name = normalize_text(r[name_col])
            work_date = normalize_text(r[date_col])[:10]
            total_hours = to_float(r[hour_col])
            raw_present = normalize_text(r[present_col]) if present_col else "1"
            present = 1 if raw_present in ("1", "是", "Y", "YES", "true", "TRUE") else 0
            if not name or not work_date or total_hours is None:
                continue
            conn.execute(
                """
                INSERT INTO attendance (employee_name, work_date, total_hours, present, source_file)
                VALUES (?, ?, ?, ?, ?)
                """,
                (name, work_date, total_hours, present, path.name),
            )
            loaded += 1
        insert_import_log(conn, path.name, loaded, "考勤导入")
    return loaded

