from __future__ import annotations

from datetime import datetime
from typing import Any

from ..db import db_session

STAGE_FIELD = {
    "manager": "manager_approved",
    "dept": "dept_approved",
    "rd": "rd_approved",
    "hr": "hr_approved",
    "finance": "finance_approved",
}

STAGE_ORDER = ["manager", "dept", "rd", "hr", "finance"]


def mask_name(name: str) -> str:
    clean = (name or "").strip()
    if len(clean) <= 1:
        return clean
    if len(clean) == 2:
        return clean[0] + "*"
    return clean[0] + "*" * (len(clean) - 2) + clean[-1]


def _prev_stage_ok_sql(stage: str) -> str:
    index = STAGE_ORDER.index(stage)
    if index == 0:
        return "1=1"
    fields = [STAGE_FIELD[s] for s in STAGE_ORDER[:index]]
    return " AND ".join(f"{f} = 1" for f in fields)


def list_pending_approvals(stage: str, month: str | None = None) -> list[dict[str, Any]]:
    if stage not in STAGE_FIELD:
        raise ValueError("invalid stage")
    current_field = STAGE_FIELD[stage]
    prev_ok_sql = _prev_stage_ok_sql(stage)
    with db_session() as conn:
        where_month = "AND t.month = ?" if month else ""
        params = [month] if month else []
        rows = conn.execute(
            f"""
            SELECT t.id, t.month, t.employee_name, t.project_code, t.project_name, t.declared_hours, d.dept_name
            FROM labor_timesheets t
            LEFT JOIN departments d ON d.dept_code = t.dept_code
            WHERE {prev_ok_sql}
              AND t.{current_field} = 0
              {where_month}
            ORDER BY t.month, t.project_code, t.employee_name
            """,
            params,
        ).fetchall()
        result = []
        for r in rows:
            item = dict(r)
            item["employee_name"] = mask_name(item["employee_name"])
            result.append(item)
        return result


def approve_timesheet(timesheet_id: int, stage: str, actor: str, decision: str, comment: str = "") -> dict[str, Any]:
    if stage not in STAGE_FIELD:
        raise ValueError("invalid stage")
    if decision not in ("approved", "rejected"):
        raise ValueError("invalid decision")
    actor = (actor or "").strip()
    if len(actor) < 2:
        raise ValueError("actor required")
    with db_session() as conn:
        row = conn.execute(
            "SELECT * FROM labor_timesheets WHERE id = ?",
            (timesheet_id,),
        ).fetchone()
        if not row:
            raise ValueError("timesheet not found")

        field = STAGE_FIELD[stage]
        # 审批顺序防护：当前环节前面的环节必须已通过。
        index = STAGE_ORDER.index(stage)
        for prev_stage in STAGE_ORDER[:index]:
            prev_field = STAGE_FIELD[prev_stage]
            if int(row[prev_field] or 0) != 1:
                raise ValueError(f"previous stage not approved: {prev_stage}")
        if int(row[field] or 0) == 1:
            raise ValueError("current stage already approved")
        if decision == "rejected" and len((comment or "").strip()) < 2:
            raise ValueError("rejected decision requires comment")

        if decision == "approved":
            conn.execute(
                f"UPDATE labor_timesheets SET {field} = 1 WHERE id = ?",
                (timesheet_id,),
            )
        conn.execute(
            """
            INSERT INTO approval_actions (timesheet_id, stage, actor, decision, action_time, comment)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (timesheet_id, stage, actor, decision, datetime.now().isoformat(timespec="seconds"), comment),
        )
        latest = conn.execute(
            "SELECT * FROM labor_timesheets WHERE id = ?",
            (timesheet_id,),
        ).fetchone()
        return dict(latest)


def labor_growth_anomalies(month: str) -> dict[str, Any]:
    with db_session() as conn:
        prev_month = _previous_month(month)
        result = {
            "month": month,
            "threshold": 1.0,
            "company": [],
            "department": [],
            "project": [],
            "person": [],
        }
        comp = _growth_value(conn, month, prev_month, "company")
        if comp and comp["growth"] > 1.0:
            result["company"].append(comp)
        result["department"] = [
            r for r in _growth_list(conn, month, prev_month, "department") if r["growth"] > 1.0
        ]
        result["project"] = [r for r in _growth_list(conn, month, prev_month, "project") if r["growth"] > 1.0]
        result["person"] = [r for r in _person_growth(conn, month, prev_month) if r["growth"] > 1.0]
        return result


def compliance_checks(month: str) -> dict[str, Any]:
    with db_session() as conn:
        check1 = _check_exceed_attendance(conn, month)
        check2 = _check_multi_project_ratio(conn, month)
        check3 = _check_absent_but_reported(conn, month)
        check4 = _check_project_over_budget(conn, month)
        return {
            "month": month,
            "checks": [
                check1,
                check2,
                check3,
                check4,
            ],
        }


def _growth_value(conn, month: str, prev_month: str, level: str) -> dict[str, Any] | None:
    if level != "company":
        return None
    current = conn.execute(
        """
        SELECT COALESCE(SUM(amount_hkd), 0) AS total
        FROM monthly_costs
        WHERE month = ? AND cost_class = 'OPEX' AND cost_component = 'LABOR'
        """,
        (month,),
    ).fetchone()["total"]
    prev = conn.execute(
        """
        SELECT COALESCE(SUM(amount_hkd), 0) AS total
        FROM monthly_costs
        WHERE month = ? AND cost_class = 'OPEX' AND cost_component = 'LABOR'
        """,
        (prev_month,),
    ).fetchone()["total"]
    if prev == 0:
        return None
    return {
        "key": "COMPANY",
        "name": "公司整体",
        "current_hkd": float(current or 0),
        "previous_hkd": float(prev or 0),
        "growth": (float(current or 0) - float(prev or 0)) / float(prev),
    }


def _growth_list(conn, month: str, prev_month: str, level: str) -> list[dict[str, Any]]:
    if level == "department":
        key_col = "dept_code"
        name_join = "LEFT JOIN departments d ON d.dept_code = k.key"
        name_col = "d.dept_name"
    elif level == "project":
        key_col = "project_code"
        name_join = "LEFT JOIN projects d ON d.project_code = k.key"
        name_col = "d.project_name"
    else:
        return []
    rows = conn.execute(
        f"""
        WITH now_cost AS (
            SELECT {key_col} AS key, SUM(amount_hkd) AS amt
            FROM monthly_costs
            WHERE month = ?
              AND cost_class = 'OPEX'
              AND cost_component = 'LABOR'
              AND {key_col} IS NOT NULL
            GROUP BY {key_col}
        ),
        prev_cost AS (
            SELECT {key_col} AS key, SUM(amount_hkd) AS amt
            FROM monthly_costs
            WHERE month = ?
              AND cost_class = 'OPEX'
              AND cost_component = 'LABOR'
              AND {key_col} IS NOT NULL
            GROUP BY {key_col}
        )
        SELECT k.key, k.amt AS current_amt, p.amt AS prev_amt, {name_col} AS name
        FROM now_cost k
        JOIN prev_cost p ON p.key = k.key
        {name_join}
        WHERE p.amt > 0
        ORDER BY ((k.amt - p.amt) / p.amt) DESC
        """,
        (month, prev_month),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        growth = (float(row["current_amt"]) - float(row["prev_amt"])) / float(row["prev_amt"])
        result.append(
            {
                "key": row["key"],
                "name": row["name"] or row["key"],
                "current_hkd": float(row["current_amt"]),
                "previous_hkd": float(row["prev_amt"]),
                "growth": growth,
            }
        )
    return result


def _person_growth(conn, month: str, prev_month: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        WITH now_hours AS (
            SELECT employee_name AS name, SUM(declared_hours) AS hours
            FROM labor_timesheets
            WHERE month = ?
            GROUP BY employee_name
        ),
        prev_hours AS (
            SELECT employee_name AS name, SUM(declared_hours) AS hours
            FROM labor_timesheets
            WHERE month = ?
            GROUP BY employee_name
        )
        SELECT n.name, n.hours AS current_hours, p.hours AS prev_hours
        FROM now_hours n
        JOIN prev_hours p ON p.name = n.name
        WHERE p.hours > 0
        ORDER BY ((n.hours - p.hours) / p.hours) DESC
        """,
        (month, prev_month),
    ).fetchall()
    return [
        {
            "key": row["name"],
            "name": mask_name(row["name"]),
            "current_hours": float(row["current_hours"]),
            "previous_hours": float(row["prev_hours"]),
            "growth": (float(row["current_hours"]) - float(row["prev_hours"])) / float(row["prev_hours"]),
        }
        for row in rows
    ]


def _check_exceed_attendance(conn, month: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        WITH ts AS (
            SELECT employee_name, SUM(declared_hours) AS declared
            FROM labor_timesheets
            WHERE month = ?
            GROUP BY employee_name
        ),
        att AS (
            SELECT employee_name, SUM(total_hours) AS attendance
            FROM attendance
            WHERE substr(work_date, 1, 7) = ?
            GROUP BY employee_name
        )
        SELECT ts.employee_name, ts.declared, att.attendance
        FROM ts
        LEFT JOIN att ON att.employee_name = ts.employee_name
        WHERE att.attendance IS NOT NULL AND ts.declared > att.attendance
        ORDER BY ts.declared - att.attendance DESC
        """,
        (month, month),
    ).fetchall()
    attendance_count = conn.execute(
        "SELECT COUNT(*) AS c FROM attendance WHERE substr(work_date, 1, 7) = ?",
        (month,),
    ).fetchone()["c"]
    return {
        "rule": "员工当月报工时数超过考勤总工时数",
        "violations": [{**dict(r), "employee_name": mask_name(r["employee_name"])} for r in rows],
        "has_attendance_data": attendance_count > 0,
    }


def _check_multi_project_ratio(conn, month: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        WITH ts AS (
            SELECT employee_name, SUM(declared_hours) AS declared
            FROM labor_timesheets
            WHERE month = ?
            GROUP BY employee_name
        ),
        att AS (
            SELECT employee_name, SUM(total_hours) AS attendance
            FROM attendance
            WHERE substr(work_date, 1, 7) = ?
            GROUP BY employee_name
        )
        SELECT ts.employee_name, ts.declared, att.attendance,
               (ts.declared / att.attendance) AS ratio
        FROM ts
        LEFT JOIN att ON att.employee_name = ts.employee_name
        WHERE att.attendance IS NOT NULL
          AND att.attendance > 0
          AND (ts.declared / att.attendance) > 0.5
        ORDER BY ratio DESC
        """,
        (month, month),
    ).fetchall()
    return {
        "rule": "员工多项目卷积申报当月总工时数超过50%",
        "violations": [{**dict(r), "employee_name": mask_name(r["employee_name"])} for r in rows],
    }


def _check_absent_but_reported(conn, month: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT t.employee_name, t.report_date, t.project_code, t.declared_hours
        FROM labor_timesheets t
        JOIN attendance a
          ON a.employee_name = t.employee_name
         AND a.work_date = substr(t.report_date, 1, 10)
        WHERE t.month = ?
          AND a.present = 0
          AND t.declared_hours > 0
        """,
        (month,),
    ).fetchall()
    return {
        "rule": "员工未出勤却申报当日研发工时",
        "violations": [{**dict(r), "employee_name": mask_name(r["employee_name"])} for r in rows],
    }


def _check_project_over_budget(conn, month: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        WITH used_budget AS (
            SELECT project_code, SUM(amount_hkd) AS used_hkd
            FROM monthly_costs
            WHERE month <= ?
              AND cost_class = 'OPEX'
              AND cost_component IN ('LABOR', 'OUTSOURCED', 'ROLLUP')
              AND project_code IS NOT NULL
            GROUP BY project_code
        ),
        target AS (
            SELECT scope_key AS project_code, SUM(target_hkd) AS target_hkd
            FROM kpi_targets
            WHERE scope_type = 'project'
            GROUP BY scope_key
        )
        SELECT u.project_code, p.project_name, u.used_hkd, t.target_hkd
        FROM used_budget u
        JOIN target t ON t.project_code = u.project_code
        LEFT JOIN projects p ON p.project_code = u.project_code
        WHERE u.used_hkd > t.target_hkd
        ORDER BY (u.used_hkd - t.target_hkd) DESC
        """,
        (month,),
    ).fetchall()
    return {
        "rule": "项目申报超预算",
        "violations": [dict(r) for r in rows],
    }


def _previous_month(month: str) -> str:
    year = int(month[:4])
    mon = int(month[5:7])
    if mon == 1:
        return f"{year - 1}-12"
    return f"{year}-{mon - 1:02d}"
