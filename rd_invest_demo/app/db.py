from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import DB_PATH


def ensure_dirs() -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)


def get_conn() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db_session() -> Iterator[sqlite3.Connection]:
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with db_session() as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS departments (
                dept_code TEXT PRIMARY KEY,
                dept_name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS projects (
                project_code TEXT PRIMARY KEY,
                project_name TEXT NOT NULL,
                dept_code TEXT,
                category TEXT,
                plan_year INTEGER,
                source_file TEXT,
                FOREIGN KEY(dept_code) REFERENCES departments(dept_code)
            );

            CREATE TABLE IF NOT EXISTS kpi_targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL,
                scope_type TEXT NOT NULL, -- company/department/project
                scope_key TEXT NOT NULL,
                cost_class TEXT NOT NULL, -- TOTAL/OPEX/CAPEX
                target_hkd REAL NOT NULL,
                source_file TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS monthly_costs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month TEXT NOT NULL, -- YYYY-MM
                year INTEGER NOT NULL,
                project_code TEXT,
                dept_code TEXT,
                cost_class TEXT NOT NULL, -- OPEX/CAPEX/TOTAL
                cost_component TEXT NOT NULL, -- LABOR/OUTSOURCED/OTHER/ROLLUP
                amount_hkd REAL NOT NULL,
                amount_cny REAL,
                source_file TEXT NOT NULL,
                raw_tag TEXT
            );

            CREATE TABLE IF NOT EXISTS progress_forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month TEXT NOT NULL,
                project_code TEXT,
                dept_code TEXT,
                expected_hkd REAL,
                actual_hkd REAL,
                note TEXT,
                source_file TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS labor_timesheets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_date TEXT NOT NULL,
                month TEXT NOT NULL,
                employee_name TEXT NOT NULL,
                project_code TEXT,
                project_name TEXT,
                dept_code TEXT,
                declared_hours REAL NOT NULL,
                manager_approved INTEGER NOT NULL DEFAULT 0,
                dept_approved INTEGER NOT NULL DEFAULT 0,
                rd_approved INTEGER NOT NULL DEFAULT 0,
                hr_approved INTEGER NOT NULL DEFAULT 0,
                finance_approved INTEGER NOT NULL DEFAULT 0,
                source_file TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS attendance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_name TEXT NOT NULL,
                work_date TEXT NOT NULL,
                total_hours REAL NOT NULL,
                present INTEGER NOT NULL,
                source_file TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS erp_pr_po (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month TEXT NOT NULL,
                project_code TEXT NOT NULL,
                doc_type TEXT NOT NULL, -- PR/PO
                amount_hkd REAL NOT NULL,
                currency TEXT NOT NULL,
                source_file TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS approval_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timesheet_id INTEGER NOT NULL,
                stage TEXT NOT NULL, -- manager/dept/rd/hr/finance
                actor TEXT NOT NULL,
                decision TEXT NOT NULL, -- approved/rejected
                action_time TEXT NOT NULL,
                comment TEXT,
                FOREIGN KEY(timesheet_id) REFERENCES labor_timesheets(id)
            );

            CREATE TABLE IF NOT EXISTS import_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                loaded_rows INTEGER NOT NULL,
                loaded_at TEXT NOT NULL,
                note TEXT
            );
            """
        )


def reset_business_tables(conn: sqlite3.Connection) -> None:
    table_names = [
        "departments",
        "projects",
        "kpi_targets",
        "monthly_costs",
        "progress_forecasts",
        "labor_timesheets",
        "attendance",
        "erp_pr_po",
        "approval_actions",
        "import_log",
    ]
    for name in table_names:
        conn.execute(f"DELETE FROM {name}")
