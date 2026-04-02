from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..db import db_session


def prev_month(month: str) -> str:
    dt = datetime.strptime(month + "-01", "%Y-%m-%d")
    year = dt.year - 1 if dt.month == 1 else dt.year
    mon = 12 if dt.month == 1 else dt.month - 1
    return f"{year}-{mon:02d}"


def prev_year_month(month: str) -> str:
    dt = datetime.strptime(month + "-01", "%Y-%m-%d")
    return f"{dt.year - 1}-{dt.month:02d}"


def ratio(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    return (current - previous) / previous


def scope_filter(scope_type: str, scope_key: str) -> tuple[str, list[Any]]:
    if scope_type == "company":
        return "1=1", []
    if scope_type == "department":
        return (
            """
            (
                mc.dept_code = ?
                OR (
                    mc.dept_code IS NULL
                    AND mc.project_code IN (
                        SELECT project_code FROM projects WHERE dept_code = ?
                    )
                )
            )
            """,
            [scope_key, scope_key],
        )
    return "mc.project_code = ?", [scope_key]


def source_condition(month: str) -> tuple[str, list[Any]]:
    # 优先使用财务快照作为“完成值”；财务无数据再退回其他来源。
    if month == "2025-10":
        return "mc.raw_tag = 'finance_1031' AND mc.cost_component = 'ROLLUP'", []
    if month == "2025-09":
        return "mc.raw_tag = 'finance_prev' AND mc.cost_component = 'ROLLUP'", []
    return "mc.source_file != 'SYSTEM'", []


def completed_amount(
    conn,
    month: str,
    scope_type: str,
    scope_key: str,
    cost_class: str,
) -> float:
    scope_sql, scope_params = scope_filter(scope_type, scope_key)
    src_sql, src_params = source_condition(month)
    if cost_class == "TOTAL":
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM(mc.amount_hkd), 0) AS total
            FROM monthly_costs mc
            WHERE mc.month = ?
              AND {scope_sql}
              AND {src_sql}
              AND mc.cost_class IN ('OPEX', 'CAPEX', 'TOTAL')
              AND (
                    mc.cost_component = 'ROLLUP'
                    OR mc.raw_tag LIKE 'dashboard_%'
              )
            """,
            [month, *scope_params, *src_params],
        ).fetchone()
        total = float(row["total"] or 0)
        if total > 0:
            return total
        # 兜底：从 OPEX/CAPEX 可用数据合成
        opex = completed_amount(conn, month, scope_type, scope_key, "OPEX")
        capex = completed_amount(conn, month, scope_type, scope_key, "CAPEX")
        return opex + capex

    row = conn.execute(
        f"""
        SELECT COALESCE(SUM(mc.amount_hkd), 0) AS total
        FROM monthly_costs mc
        WHERE mc.month = ?
          AND {scope_sql}
          AND {src_sql}
          AND mc.cost_class = ?
          AND mc.cost_component = 'ROLLUP'
        """,
        [month, *scope_params, *src_params, cost_class],
    ).fetchone()
    total = float(row["total"] or 0)
    if total > 0:
        return total
    # 对 OPEX 兜底到人工成本；CAPEX 无兜底数据则返回 0
    if cost_class == "OPEX":
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM(mc.amount_hkd), 0) AS total
            FROM monthly_costs mc
            WHERE mc.month = ?
              AND {scope_sql}
              AND mc.cost_class = 'OPEX'
              AND mc.cost_component IN ('LABOR', 'OUTSOURCED')
              AND mc.source_file != 'SYSTEM'
            """,
            [month, *scope_params],
        ).fetchone()
        return float(row["total"] or 0)
    return 0.0


def target_amount(conn, report_year: int, scope_type: str, scope_key: str, cost_class: str) -> float:
    row = conn.execute(
        """
        SELECT target_hkd
        FROM kpi_targets
        WHERE scope_type = ? AND scope_key = ? AND cost_class = ?
        ORDER BY ABS(year - ?) ASC, year DESC
        LIMIT 1
        """,
        (scope_type, scope_key, cost_class, report_year),
    ).fetchone()
    if row:
        return float(row["target_hkd"] or 0)

    # 汇总项目目标作为部门/公司目标兜底。
    if scope_type in ("department", "company") and cost_class in ("OPEX", "CAPEX"):
        if scope_type == "company":
            clause = "1=1"
            params: list[Any] = [cost_class, report_year]
        else:
            clause = "p.dept_code = ?"
            params = [scope_key, cost_class, report_year]
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM(k.target_hkd), 0) AS total
            FROM kpi_targets k
            JOIN projects p ON p.project_code = k.scope_key
            WHERE k.scope_type = 'project'
              AND {clause}
              AND k.cost_class = ?
            ORDER BY ABS(k.year - ?) ASC, k.year DESC
            """,
            params,
        ).fetchone()
        return float(row["total"] or 0)

    if cost_class == "TOTAL":
        return target_amount(conn, report_year, scope_type, scope_key, "OPEX") + target_amount(
            conn, report_year, scope_type, scope_key, "CAPEX"
        )
    return 0.0


def proxy_last_year_amount(conn, scope_type: str, scope_key: str, cost_class: str) -> float:
    # 若缺同月上年数据，使用“立项年份=上年”的财务累计作为代理值（真实数据，不做模拟）。
    scope_sql, scope_params = scope_filter(scope_type, scope_key)
    if cost_class == "TOTAL":
        classes = ("OPEX", "CAPEX")
    else:
        classes = (cost_class,)
    placeholders = ",".join(["?"] * len(classes))
    row = conn.execute(
        f"""
        SELECT COALESCE(SUM(mc.amount_hkd), 0) AS total
        FROM monthly_costs mc
        JOIN projects p ON p.project_code = mc.project_code
        WHERE {scope_sql}
          AND mc.cost_component = 'ROLLUP'
          AND mc.raw_tag IN ('finance_prev', 'finance_1031')
          AND p.plan_year = 2024
          AND mc.cost_class IN ({placeholders})
        """,
        [*scope_params, *classes],
    ).fetchone()
    return float(row["total"] or 0)


def metric_block(conn, month: str, scope_type: str, scope_key: str, cost_class: str) -> dict[str, Any]:
    report_year = int(month[:4])
    completed = completed_amount(conn, month, scope_type, scope_key, cost_class)
    last_m = completed_amount(conn, prev_month(month), scope_type, scope_key, cost_class)
    last_y = completed_amount(conn, prev_year_month(month), scope_type, scope_key, cost_class)
    if last_y == 0:
        last_y = proxy_last_year_amount(conn, scope_type, scope_key, cost_class)
    target = target_amount(conn, report_year, scope_type, scope_key, cost_class)
    yoy = ratio(completed, last_y) if last_y else None
    mom = ratio(completed, last_m) if last_m else None
    support_rate = (completed / target) if target else None
    gap = (target - completed) if target else None
    return {
        "target_hkd": target,
        "completed_hkd": completed,
        "last_month_hkd": last_m,
        "last_year_hkd": last_y,
        "yoy": yoy,
        "mom": mom,
        "support_rate": support_rate,
        "gap_hkd": gap,
    }


def get_dashboard_metrics(month: str, scope_type: str, scope_key: str) -> dict[str, Any]:
    with db_session() as conn:
        data = {
            "TOTAL": metric_block(conn, month, scope_type, scope_key, "TOTAL"),
            "OPEX": metric_block(conn, month, scope_type, scope_key, "OPEX"),
            "CAPEX": metric_block(conn, month, scope_type, scope_key, "CAPEX"),
        }
        scope_name = scope_key
        if scope_type == "department":
            row = conn.execute(
                "SELECT dept_name FROM departments WHERE dept_code = ?",
                (scope_key,),
            ).fetchone()
            scope_name = row["dept_name"] if row else scope_key
        if scope_type == "project":
            row = conn.execute(
                "SELECT project_name FROM projects WHERE project_code = ?",
                (scope_key,),
            ).fetchone()
            scope_name = row["project_name"] if row else scope_key
        return {
            "month": month,
            "scope_type": scope_type,
            "scope_key": scope_key,
            "scope_name": scope_name,
            "metrics": data,
        }


def list_scopes() -> dict[str, list[dict[str, str]]]:
    with db_session() as conn:
        departments = [
            {"code": r["dept_code"], "name": r["dept_name"]}
            for r in conn.execute("SELECT dept_code, dept_name FROM departments ORDER BY dept_code")
        ]
        projects = [
            {"code": r["project_code"], "name": r["project_name"], "dept_code": r["dept_code"] or ""}
            for r in conn.execute(
                "SELECT project_code, project_name, dept_code FROM projects ORDER BY project_code"
            )
        ]
        return {"departments": departments, "projects": projects}


def list_import_log() -> list[dict[str, Any]]:
    with db_session() as conn:
        rows = conn.execute(
            """
            SELECT source_name, loaded_rows, loaded_at, note
            FROM import_log
            ORDER BY id DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def generate_progress_alerts(month: str, scope_type: str, scope_key: str) -> list[dict[str, Any]]:
    data = get_dashboard_metrics(month=month, scope_type=scope_type, scope_key=scope_key)
    alerts: list[dict[str, Any]] = []
    for cost_class, block in data["metrics"].items():
        support_rate = block.get("support_rate")
        gap_hkd = block.get("gap_hkd")
        if support_rate is not None and support_rate < 0.8:
            alerts.append(
                {
                    "type": "support_rate_low",
                    "level": "high" if support_rate < 0.5 else "medium",
                    "scope": data["scope_name"],
                    "cost_class": cost_class,
                    "message": f"{cost_class} 目标支撑率仅 {support_rate * 100:.2f}%",
                }
            )
        if gap_hkd is not None and gap_hkd > 0:
            alerts.append(
                {
                    "type": "gap_exists",
                    "level": "high" if gap_hkd > 10_000_000 else "medium",
                    "scope": data["scope_name"],
                    "cost_class": cost_class,
                    "message": f"{cost_class} 仍有缺口 HK$ {gap_hkd:,.2f}",
                }
            )
    return alerts
