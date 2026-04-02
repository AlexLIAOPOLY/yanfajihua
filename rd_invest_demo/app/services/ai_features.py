from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ValidationError

from ..db import db_session
from .compliance import compliance_checks, labor_growth_anomalies, mask_name
from .forecast import forecast
from .llm_client import LLMError, chat_json
from .metrics import (
    completed_amount,
    generate_progress_alerts,
    get_dashboard_metrics,
    list_scopes,
    metric_block,
    prev_month,
    prev_year_month,
    target_amount,
)
from .safety import enforce_safe_user_prompt, sanitize_output_text


class CopilotStructured(BaseModel):
    key_risks: list[str]
    two_week_actions: list[str]
    next_month_coordination: list[str]
    confidence: float | None = None


class AskStructured(BaseModel):
    answer: str
    basis: list[str] = []
    insufficient_data: bool = False
    confidence: float | None = None


def _risk_score(
    dashboard: dict[str, Any],
    violation_count: int,
    anomaly_count: int,
) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []
    for k, block in dashboard["metrics"].items():
        support_rate = block.get("support_rate")
        gap = float(block.get("gap_hkd") or 0)
        mom = block.get("mom")
        if support_rate is not None and support_rate < 0.8:
            penalty = (0.8 - float(support_rate)) * 45
            score += penalty
            reasons.append(f"{k} 目标支撑率偏低({support_rate * 100:.2f}%)")
        if gap > 0:
            penalty = min(18.0, gap / 10_000_000 * 4.5)
            score += penalty
        if mom is not None and float(mom) > 0.3:
            score += min(10.0, float(mom) * 12)
            reasons.append(f"{k} 环比增长较快({float(mom) * 100:.2f}%)")
    if violation_count:
        score += min(22.0, violation_count * 3.5)
        reasons.append(f"存在 {violation_count} 条合规违规")
    if anomaly_count:
        score += min(14.0, anomaly_count * 4.0)
        reasons.append(f"存在 {anomaly_count} 项人工成本异常")
    return max(0.0, min(100.0, score)), reasons


def _risk_level(score: float) -> str:
    if score >= 70:
        return "high"
    if score >= 40:
        return "medium"
    return "low"


def _actions_from_data(
    dashboard: dict[str, Any],
    compliance: dict[str, Any],
    anomalies: dict[str, Any],
) -> list[str]:
    actions: list[str] = []
    total_block = dashboard["metrics"]["TOTAL"]
    if (total_block.get("support_rate") or 0) < 0.8:
        actions.append("优先对 TOP 缺口项目补齐预算执行计划，并按周更新 closing plan。")
    opex_block = dashboard["metrics"]["OPEX"]
    if (opex_block.get("mom") or 0) > 0.3:
        actions.append("OPEX 环比抬升，建议立即复核工时与委托费用的当月异常波动来源。")
    for check in compliance.get("checks", []):
        if check.get("violations"):
            if "考勤" in check.get("rule", ""):
                actions.append("补齐考勤数据联动校验，优先阻断超工时/未出勤报工记录进入财务。")
            if "超预算" in check.get("rule", ""):
                actions.append("对超预算项目设立审批闸口：财务环节必须附预算偏差说明。")
    if anomalies.get("department") or anomalies.get("project"):
        actions.append("将人工成本异常部门/项目纳入下月滚动复盘，设置红黄线阈值自动提醒。")
    if not actions:
        actions.append("当前风险可控，建议维持现有节奏并持续按月校验预算与工时一致性。")
    # 去重并保持顺序
    deduped: list[str] = []
    for x in actions:
        if x not in deduped:
            deduped.append(x)
    return deduped[:6]


def _all_violation_count(compliance: dict[str, Any]) -> int:
    count = 0
    for check in compliance.get("checks", []):
        count += len(check.get("violations") or [])
    return count


def _all_anomaly_count(anomalies: dict[str, Any]) -> int:
    return sum(
        len(anomalies.get(key) or [])
        for key in ("company", "department", "project", "person")
    )


def _scope_name(scope_type: str, scope_key: str) -> str:
    scopes = list_scopes()
    if scope_type == "department":
        for d in scopes["departments"]:
            if d["code"] == scope_key:
                return d["name"]
    if scope_type == "project":
        for p in scopes["projects"]:
            if p["code"] == scope_key:
                return p["name"]
    return "公司整体" if scope_type == "company" else scope_key


def _scope_sql(scope_type: str) -> str:
    if scope_type == "company":
        return "1=1"
    if scope_type == "department":
        return (
            """
            (
                t.dept_code = ?
                OR (
                    t.dept_code IS NULL
                    AND t.project_code IN (
                        SELECT project_code FROM projects WHERE dept_code = ?
                    )
                )
            )
            """
        )
    return "t.project_code = ?"


def _scope_sql_params(scope_type: str, scope_key: str) -> list[Any]:
    if scope_type == "company":
        return []
    if scope_type == "department":
        return [scope_key, scope_key]
    return [scope_key]


def _avg_labor_cost_per_hour(
    conn,
    month: str,
    scope_type: str,
    scope_key: str,
) -> float:
    sql = _scope_sql(scope_type)
    params = _scope_sql_params(scope_type, scope_key)
    labor = conn.execute(
        f"""
        SELECT COALESCE(SUM(mc.amount_hkd), 0) AS total
        FROM monthly_costs mc
        WHERE mc.month <= ?
          AND {sql.replace("t.", "mc.")}
          AND mc.cost_component = 'LABOR'
        """,
        [month, *params],
    ).fetchone()["total"]
    hours = conn.execute(
        f"""
        SELECT COALESCE(SUM(t.declared_hours), 0) AS total
        FROM labor_timesheets t
        WHERE t.month <= ?
          AND {sql}
        """,
        [month, *params],
    ).fetchone()["total"]
    if not hours:
        return 0.0
    return float(labor or 0) / float(hours)


def copilot_brief(
    month: str,
    scope_type: str,
    scope_key: str,
    api_key: str | None = None,
    provider: str = "deepseek",
    model: str | None = None,
) -> dict[str, Any]:
    dashboard = get_dashboard_metrics(month=month, scope_type=scope_type, scope_key=scope_key)
    compliance = compliance_checks(month=month)
    anomalies = labor_growth_anomalies(month=month)
    alerts = generate_progress_alerts(month=month, scope_type=scope_type, scope_key=scope_key)
    future = forecast(scope_type=scope_type, scope_key=scope_key, cost_class="TOTAL", horizon=2)
    violation_count = _all_violation_count(compliance)
    anomaly_count = _all_anomaly_count(anomalies)
    risk_score, reasons = _risk_score(dashboard, violation_count, anomaly_count)
    actions = _actions_from_data(dashboard, compliance, anomalies)
    output: dict[str, Any] = {
        "month": month,
        "scope_type": scope_type,
        "scope_key": scope_key,
        "scope_name": _scope_name(scope_type, scope_key),
        "risk_score": round(risk_score, 2),
        "risk_level": _risk_level(risk_score),
        "risk_reasons": reasons,
        "suggested_actions": actions,
        "alert_count": len(alerts),
        "violation_count": violation_count,
        "anomaly_count": anomaly_count,
        "forecast": future,
    }
    if api_key:
        prompt = (
            "请基于以下真实研发投入数据生成管理动作建议。\n"
            f"范围: {output['scope_name']} ({scope_type}/{scope_key})\n"
            f"月份: {month}\n"
            f"风险分: {output['risk_score']} ({output['risk_level']})\n"
            f"风险原因: {reasons}\n"
            f"建议动作: {actions}\n"
            f"未来预测: {future.get('forecast', [])}"
        )
        try:
            llm = chat_json(
                system_prompt=(
                    "你是研发投入治理顾问。仅根据给定数据推导，不得编造。"
                    "输出必须是结构化 JSON。"
                ),
                user_prompt=prompt,
                schema_hint=(
                    '{'
                    '"key_risks": ["string"], '
                    '"two_week_actions": ["string"], '
                    '"next_month_coordination": ["string"], '
                    '"confidence": 0.0'
                    "}"
                ),
                api_key=api_key,
                provider=provider,
                model=model,
            )
            parsed = CopilotStructured.model_validate(llm["object"])
            key_risks = [sanitize_output_text(x, max_len=200) for x in parsed.key_risks[:5]]
            two_week_actions = [sanitize_output_text(x, max_len=220) for x in parsed.two_week_actions[:6]]
            next_month = [sanitize_output_text(x, max_len=220) for x in parsed.next_month_coordination[:6]]
            output["llm_structured"] = {
                "key_risks": key_risks,
                "two_week_actions": two_week_actions,
                "next_month_coordination": next_month,
                "confidence": parsed.confidence,
                "model": llm.get("model"),
                "request_id": llm.get("request_id"),
            }
            output["llm_summary"] = (
                "关键风险:\n- "
                + ("\n- ".join(key_risks) if key_risks else "暂无")
                + "\n\n两周动作:\n- "
                + ("\n- ".join(two_week_actions) if two_week_actions else "暂无")
                + "\n\n下月协调:\n- "
                + ("\n- ".join(next_month) if next_month else "暂无")
            )
        except ValidationError as exc:
            output["llm_summary_error"] = f"LLM 结构校验失败: {exc.errors()[0]['msg']}"
        except LLMError as exc:
            output["llm_summary_error"] = str(exc)
    return output


def scenario_simulation(
    month: str,
    scope_type: str,
    scope_key: str,
    cost_class: str,
    add_outsourced_hkd: float,
    add_labor_hours: float,
    labor_cost_factor: float = 1.0,
) -> dict[str, Any]:
    if labor_cost_factor <= 0:
        labor_cost_factor = 1.0
    with db_session() as conn:
        base = metric_block(conn, month=month, scope_type=scope_type, scope_key=scope_key, cost_class=cost_class)
        avg_cost_per_hour = _avg_labor_cost_per_hour(conn, month, scope_type, scope_key)
        added_labor_cost = add_labor_hours * avg_cost_per_hour * labor_cost_factor
        projected_completed = float(base["completed_hkd"] or 0) + float(add_outsourced_hkd or 0) + added_labor_cost
        target = float(base["target_hkd"] or 0)
        projected_support_rate = (projected_completed / target) if target else None
        projected_gap = (target - projected_completed) if target else None
        needed_to_close_gap = max(0.0, projected_gap or 0.0)
        needed_hours_to_close_gap = (
            needed_to_close_gap / (avg_cost_per_hour * labor_cost_factor)
            if avg_cost_per_hour > 0
            else None
        )
        return {
            "month": month,
            "scope_type": scope_type,
            "scope_key": scope_key,
            "cost_class": cost_class,
            "base": base,
            "inputs": {
                "add_outsourced_hkd": add_outsourced_hkd,
                "add_labor_hours": add_labor_hours,
                "labor_cost_factor": labor_cost_factor,
            },
            "model_params": {
                "avg_labor_cost_per_hour_hkd": avg_cost_per_hour,
            },
            "projection": {
                "projected_completed_hkd": projected_completed,
                "projected_support_rate": projected_support_rate,
                "projected_gap_hkd": projected_gap,
                "needed_extra_hkd_to_close_gap": needed_to_close_gap,
                "needed_extra_hours_to_close_gap": needed_hours_to_close_gap,
            },
        }


def _rule_answer(
    question: str,
    month: str,
    scope_type: str,
    scope_key: str,
) -> tuple[str, dict[str, Any]]:
    q = (question or "").strip().lower()
    dashboard = get_dashboard_metrics(month=month, scope_type=scope_type, scope_key=scope_key)
    compliance = compliance_checks(month=month)
    suggestions = forecast(scope_type=scope_type, scope_key=scope_key, cost_class="TOTAL", horizon=2)

    if ("谁负责" in q) or ("负责人" in q) or ("owner" in q):
        text = (
            "当前数据底表不包含“项目周关闭计划负责人”字段，"
            "因此无法直接给出责任人名单。建议补充项目责任人映射表（project_code, owner）。"
        )
        return text, {"missing_field": "project_owner_mapping"}

    if ("top" in q and "缺口" in q) or ("缺口最大" in q and "项目" in q):
        with db_session() as conn:
            projects = conn.execute(
                """
                SELECT p.project_code, p.project_name
                FROM projects p
                ORDER BY p.project_code
                """
            ).fetchall()
            rows: list[dict[str, Any]] = []
            for p in projects:
                block = metric_block(conn, month=month, scope_type="project", scope_key=p["project_code"], cost_class="TOTAL")
                target = float(block.get("target_hkd") or 0)
                completed = float(block.get("completed_hkd") or 0)
                gap = float(block.get("gap_hkd") or 0)
                if target <= 0:
                    continue
                rows.append(
                    {
                        "project_code": p["project_code"],
                        "project_name": p["project_name"] or p["project_code"],
                        "target_hkd": target,
                        "completed_hkd": completed,
                        "gap_hkd": gap,
                    }
                )
        rows = sorted(rows, key=lambda x: x["gap_hkd"], reverse=True)
        top = rows[:3]
        if not top:
            return "当前没有可计算的项目级目标与缺口数据。", {"projects": []}
        text = "TOTAL 预算缺口 TOP3: " + "；".join(
            f"{x['project_code']}（{x['project_name']}）缺口 {x['gap_hkd']:,.2f} HKD"
            for x in top
        )
        return text, {"projects": top}

    if ("合规" in q and "受阻" in q) or ("违规" in q and "项目" in q):
        over_budget = []
        for c in compliance["checks"]:
            if "超预算" in c["rule"]:
                over_budget = c["violations"]
                break
        if not over_budget:
            return "当前未识别到由合规违规导致的预算执行受阻项目。", {"projects": []}
        over_budget = sorted(
            over_budget,
            key=lambda x: float(x.get("used_hkd", 0) or 0) - float(x.get("target_hkd", 0) or 0),
            reverse=True,
        )
        top = over_budget[:5]
        text = "预算执行受阻项目（超预算）: " + "；".join(
            f"{x['project_code']} 超出 {(x['used_hkd'] - x['target_hkd']):,.2f} HKD"
            for x in top
        )
        return text, {"projects": top}

    if ("超预算" in q and "审批" in q) or ("优先级" in q and "审批" in q):
        over_budget = []
        for c in compliance["checks"]:
            if "超预算" in c["rule"]:
                over_budget = c["violations"]
                break
        if not over_budget:
            return "当前没有检测到超预算项目，可维持现有审批节奏。", {"projects": []}
        ranked = sorted(
            over_budget,
            key=lambda x: float(x.get("used_hkd", 0) or 0) - float(x.get("target_hkd", 0) or 0),
            reverse=True,
        )[:5]
        text = (
            "建议按“超出金额”从高到低排序审批："
            + "；".join(f"{x['project_code']}（+{(x['used_hkd'] - x['target_hkd']):,.2f} HKD）" for x in ranked)
            + "。审批路径建议：项目经理→部门经理→研发归口→人力→财务。"
        )
        return text, {"projects": ranked}

    if ("capex" in q and ("为零" in q or "0" in q or "原因" in q)):
        capex = dashboard["metrics"]["CAPEX"]
        target = float(capex.get("target_hkd") or 0)
        completed = float(capex.get("completed_hkd") or 0)
        with db_session() as conn:
            capex_rows = conn.execute(
                """
                SELECT COUNT(1) AS cnt, COALESCE(SUM(amount_hkd), 0) AS amt
                FROM monthly_costs
                WHERE month = ?
                  AND cost_class = 'CAPEX'
                  AND cost_component = 'ROLLUP'
                """,
                (month,),
            ).fetchone()
        count = int(capex_rows["cnt"] or 0)
        amt = float(capex_rows["amt"] or 0)
        if target <= 0:
            return "CAPEX 目标值为 0，当前口径下不存在需要执行的 CAPEX 目标。", {"capex": capex}
        if completed <= 0 and count == 0:
            return (
                f"{month} CAPEX 完成值为 0 的直接原因是：当月尚无 CAPEX ROLLUP 入账记录。"
                "请核对 ERP PR/PO 是否已归类到 CAPEX 并完成财务入账。"
            ), {"capex": capex}
        if completed <= 0 and amt <= 0:
            return (
                f"{month} CAPEX 目标为 {target:,.2f} HKD，但实际完成值为 0，"
                "说明当前数据口径下 CAPEX 尚未形成有效发生额。"
            ), {"capex": capex}
        return (
            f"{month} CAPEX 已有发生额（{completed:,.2f} HKD），不是执行率为零场景。"
        ), {"capex": capex}

    if ("缺口" in q) or ("gap" in q):
        total_gap = dashboard["metrics"]["TOTAL"]["gap_hkd"]
        text = f"{month} 当前 TOTAL 缺口为 {total_gap:,.2f} HKD。"
        return text, {"dashboard": dashboard["metrics"]["TOTAL"]}
    if ("超预算" in q) or ("预算" in q and "超" in q):
        over_budget = []
        for c in compliance["checks"]:
            if "超预算" in c["rule"]:
                over_budget = c["violations"]
                break
        if not over_budget:
            return "当前没有检测到项目超预算。", {"projects": []}
        top = over_budget[:5]
        text = "超预算项目TOP5: " + "；".join(
            f"{x['project_code']} 超出 {(x['used_hkd'] - x['target_hkd']):,.2f} HKD"
            for x in top
        )
        return text, {"projects": top}
    if ("预测" in q) or ("forecast" in q):
        fc = suggestions.get("forecast", [])
        if not fc:
            return "历史样本不足，暂无法给出预测。", {"forecast": []}
        text = "未来预测: " + "；".join(f"{x['month']} 约 {x['predicted_hkd']:,.2f} HKD" for x in fc)
        return text, {"forecast": fc}
    text = (
        f"{month} 范围 {scope_type}/{scope_key} 的 TOTAL 支撑率为 "
        f"{(dashboard['metrics']['TOTAL']['support_rate'] or 0) * 100:.2f}% ，"
        f"缺口 {(dashboard['metrics']['TOTAL']['gap_hkd'] or 0):,.2f} HKD。"
    )
    return text, {"dashboard": dashboard["metrics"]["TOTAL"]}


def ask_data(
    question: str,
    month: str,
    scope_type: str,
    scope_key: str,
    api_key: str | None = None,
    provider: str = "deepseek",
    model: str | None = None,
) -> dict[str, Any]:
    safe_question = enforce_safe_user_prompt(question, label="question", max_len=600)
    base_answer, context = _rule_answer(safe_question, month, scope_type, scope_key)
    output = {
        "mode": "rule",
        "answer": base_answer,
        "context": context,
    }
    if api_key:
        prompt = (
            "请基于真实数据上下文回答问题，回答要简洁并给出明确结论，不得编造。\n"
            f"问题: {safe_question}\n"
            f"规则引擎回答: {base_answer}\n"
            f"上下文: {context}\n"
            "如果数据不足，请明确说明不足。"
        )
        try:
            llm = chat_json(
                system_prompt="你是研发投入数据问答助手，只可基于已给上下文作答，必须输出 JSON。",
                user_prompt=prompt,
                schema_hint=(
                    '{'
                    '"answer": "string", '
                    '"basis": ["string"], '
                    '"insufficient_data": false, '
                    '"confidence": 0.0'
                    "}"
                ),
                api_key=api_key,
                provider=provider,
                model=model,
            )
            parsed = AskStructured.model_validate(llm["object"])
            llm_answer = sanitize_output_text(parsed.answer, max_len=2000)
            if parsed.insufficient_data:
                output["mode"] = "rule"
                output["answer"] = sanitize_output_text(
                    f"{base_answer}\n\n说明：{llm_answer}",
                    max_len=2200,
                )
            else:
                output["mode"] = "llm"
                output["answer"] = llm_answer
            output["basis"] = [sanitize_output_text(x, max_len=180) for x in parsed.basis[:6]]
            output["insufficient_data"] = bool(parsed.insufficient_data)
            output["confidence"] = parsed.confidence
            output["model"] = llm.get("model")
            output["request_id"] = llm.get("request_id")
        except ValidationError as exc:
            output["llm_error"] = f"LLM 结构校验失败: {exc.errors()[0]['msg']}"
        except LLMError as exc:
            output["llm_error"] = str(exc)
    return output


def approval_recommendations(stage: str, month: str) -> dict[str, Any]:
    stage_fields = {
        "manager": "manager_approved",
        "dept": "dept_approved",
        "rd": "rd_approved",
        "hr": "hr_approved",
        "finance": "finance_approved",
    }
    order = ["manager", "dept", "rd", "hr", "finance"]
    if stage not in stage_fields:
        raise ValueError("invalid stage")
    idx = order.index(stage)
    prev_fields = [stage_fields[s] for s in order[:idx]]
    prev_sql = " AND ".join(f"t.{f}=1" for f in prev_fields) if prev_fields else "1=1"
    current = stage_fields[stage]
    with db_session() as conn:
        pending = conn.execute(
            f"""
            SELECT t.id, t.report_date, t.month, t.employee_name, t.project_code, t.project_name,
                   t.declared_hours, t.dept_code, d.dept_name
            FROM labor_timesheets t
            LEFT JOIN departments d ON d.dept_code = t.dept_code
            WHERE {prev_sql} AND t.{current}=0 AND t.month=?
            ORDER BY t.declared_hours DESC
            """,
            (month,),
        ).fetchall()
        if not pending:
            return {"stage": stage, "month": month, "requires_human_confirm": True, "items": []}

        over_budget = set(
            x["project_code"]
            for x in conn.execute(
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
                SELECT u.project_code
                FROM used_budget u
                JOIN target t ON t.project_code=u.project_code
                WHERE u.used_hkd > t.target_hkd
                """,
                (month,),
            ).fetchall()
        )

        att_map = {
            (r["employee_name"], r["work_date"]): (r["total_hours"], r["present"])
            for r in conn.execute(
                "SELECT employee_name, work_date, total_hours, present FROM attendance WHERE substr(work_date,1,7)=?",
                (month,),
            ).fetchall()
        }
        employee_month_hours = {
            r["employee_name"]: float(r["hours"] or 0)
            for r in conn.execute(
                """
                SELECT employee_name, SUM(declared_hours) AS hours
                FROM labor_timesheets
                WHERE month=?
                GROUP BY employee_name
                """,
                (month,),
            ).fetchall()
        }
        employee_project_count = {
            r["employee_name"]: int(r["cnt"] or 0)
            for r in conn.execute(
                """
                SELECT employee_name, COUNT(DISTINCT project_code) AS cnt
                FROM labor_timesheets
                WHERE month=?
                GROUP BY employee_name
                """,
                (month,),
            ).fetchall()
        }

        items: list[dict[str, Any]] = []
        for row in pending:
            employee = row["employee_name"]
            report_date = (row["report_date"] or "")[:10]
            score = 0.0
            reasons: list[str] = []
            if float(row["declared_hours"] or 0) >= 12:
                score += 24
                reasons.append("单条报工时长较高")
            if employee_project_count.get(employee, 0) >= 3:
                score += 14
                reasons.append("当月涉及多项目报工")
            if row["project_code"] in over_budget:
                score += 28
                reasons.append("项目已超预算")
            att = att_map.get((employee, report_date))
            if att:
                total_hours, present = att
                monthly_hours = employee_month_hours.get(employee, 0.0)
                if float(total_hours or 0) > 0 and monthly_hours > float(total_hours):
                    score += 18
                    reasons.append("当月报工超过考勤工时")
                if int(present or 0) == 0 and float(row["declared_hours"] or 0) > 0:
                    score += 28
                    reasons.append("未出勤日存在报工")
            recommendation = "approved"
            if score >= 55:
                recommendation = "rejected"
            elif score >= 35 and stage in ("hr", "finance"):
                recommendation = "rejected"
            priority = "high" if score >= 60 else "medium" if score >= 35 else "low"
            items.append(
                {
                    "timesheet_id": row["id"],
                    "month": row["month"],
                    "employee_name": mask_name(employee),
                    "project_code": row["project_code"],
                    "project_name": row["project_name"],
                    "dept_name": row["dept_name"] or "",
                    "declared_hours": float(row["declared_hours"] or 0),
                    "risk_score": round(min(score, 100), 2),
                    "priority": priority,
                    "recommendation": recommendation,
                    "reasons": reasons,
                }
            )
        items.sort(key=lambda x: x["risk_score"], reverse=True)
        return {"stage": stage, "month": month, "requires_human_confirm": True, "items": items}
