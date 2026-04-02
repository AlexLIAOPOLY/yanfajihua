from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np

from ..db import db_session
from .metrics import scope_filter


def _month_index(month: str) -> int:
    return int(month[:4]) * 12 + int(month[5:7])


def _index_month(index: int) -> str:
    year = index // 12
    mon = index % 12
    if mon == 0:
        year -= 1
        mon = 12
    return f"{year}-{mon:02d}"


def monthly_series(scope_type: str, scope_key: str, cost_class: str) -> list[dict[str, Any]]:
    with db_session() as conn:
        scope_sql, params = scope_filter(scope_type, scope_key)
        rows = conn.execute(
            f"""
            SELECT month, SUM(amount_hkd) AS amount
            FROM monthly_costs mc
            WHERE {scope_sql}
              AND mc.cost_component = 'ROLLUP'
              AND (
                (? = 'TOTAL' AND mc.cost_class IN ('TOTAL', 'OPEX', 'CAPEX'))
                OR mc.cost_class = ?
              )
              AND mc.source_file != 'SYSTEM'
            GROUP BY month
            ORDER BY month
            """,
            [*params, cost_class, cost_class],
        ).fetchall()
        return [{"month": r["month"], "amount_hkd": float(r["amount"] or 0)} for r in rows]


def forecast(scope_type: str, scope_key: str, cost_class: str, horizon: int = 2) -> dict[str, Any]:
    series = monthly_series(scope_type, scope_key, cost_class)
    if len(series) < 3:
        return {
            "scope_type": scope_type,
            "scope_key": scope_key,
            "cost_class": cost_class,
            "history": series,
            "forecast": [],
            "note": "历史样本不足，至少需要3个周期。",
        }
    x = np.array([_month_index(item["month"]) for item in series], dtype=float)
    y = np.array([item["amount_hkd"] for item in series], dtype=float)
    slope, intercept = np.polyfit(x, y, deg=1)
    last_idx = _month_index(series[-1]["month"])
    f = []
    for i in range(1, horizon + 1):
        idx = last_idx + i
        month = _index_month(idx)
        pred = max(0.0, slope * idx + intercept)
        f.append({"month": month, "predicted_hkd": float(pred)})
    return {
        "scope_type": scope_type,
        "scope_key": scope_key,
        "cost_class": cost_class,
        "history": series,
        "forecast": f,
        "model": {"type": "linear_regression", "slope": float(slope), "intercept": float(intercept)},
    }


def dynamic_hour_suggestions(month: str) -> dict[str, Any]:
    # 基于“剩余预算 / 历史每工时成本”给出下月建议工时
    with db_session() as conn:
        budget_rows = conn.execute(
            """
            SELECT scope_key AS project_code, SUM(target_hkd) AS target_hkd
            FROM kpi_targets
            WHERE scope_type = 'project'
            GROUP BY scope_key
            """
        ).fetchall()
        target_map = {r["project_code"]: float(r["target_hkd"] or 0) for r in budget_rows}

        used_rows = conn.execute(
            """
            SELECT project_code, SUM(amount_hkd) AS used_hkd
            FROM monthly_costs
            WHERE month <= ?
              AND project_code IS NOT NULL
              AND cost_class = 'OPEX'
              AND cost_component IN ('ROLLUP', 'LABOR', 'OUTSOURCED')
            GROUP BY project_code
            """,
            (month,),
        ).fetchall()
        used_map = {r["project_code"]: float(r["used_hkd"] or 0) for r in used_rows}

        hour_rows = conn.execute(
            """
            SELECT project_code, SUM(declared_hours) AS hours
            FROM labor_timesheets
            WHERE month <= ?
              AND project_code IS NOT NULL
            GROUP BY project_code
            """,
            (month,),
        ).fetchall()
        hours_map = {r["project_code"]: float(r["hours"] or 0) for r in hour_rows}

        labor_rows = conn.execute(
            """
            SELECT project_code, SUM(amount_hkd) AS labor_hkd
            FROM monthly_costs
            WHERE month <= ?
              AND project_code IS NOT NULL
              AND cost_component = 'LABOR'
            GROUP BY project_code
            """,
            (month,),
        ).fetchall()
        labor_map = {r["project_code"]: float(r["labor_hkd"] or 0) for r in labor_rows}

        project_rows = conn.execute(
            "SELECT project_code, project_name, dept_code FROM projects ORDER BY project_code"
        ).fetchall()

        suggestions = []
        for r in project_rows:
            code = r["project_code"]
            target = target_map.get(code, 0.0)
            used = used_map.get(code, 0.0)
            remain = max(0.0, target - used)
            history_hours = hours_map.get(code, 0.0)
            labor_cost = labor_map.get(code, 0.0)
            cost_per_hour = (labor_cost / history_hours) if history_hours > 0 else 0.0
            suggest_hours = (remain / cost_per_hour) if cost_per_hour > 0 else 0.0
            if target <= 0 and used <= 0:
                continue
            suggestions.append(
                {
                    "project_code": code,
                    "project_name": r["project_name"],
                    "dept_code": r["dept_code"] or "",
                    "target_hkd": target,
                    "used_hkd": used,
                    "remain_hkd": remain,
                    "history_hours": history_hours,
                    "avg_cost_per_hour_hkd": cost_per_hour,
                    "recommended_next_month_hours": suggest_hours,
                }
            )

        suggestions.sort(key=lambda x: x["remain_hkd"], reverse=True)
        return {"month": month, "suggestions": suggestions}

