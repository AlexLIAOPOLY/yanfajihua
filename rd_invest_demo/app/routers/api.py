from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..config import DEFAULT_REPORT_MONTH, UPLOAD_DIR
from ..db import db_session
from ..services.compliance import (
    approve_timesheet,
    compliance_checks,
    labor_growth_anomalies,
    list_pending_approvals,
    mask_name,
)
from ..services.ai_features import (
    approval_recommendations,
    ask_data,
    copilot_brief,
    scenario_simulation,
)
from ..services.data_loader import load_all_sources
from ..services.forecast import dynamic_hour_suggestions, forecast
from ..services.llm_client import LLMError, chat_summary, chat_summary_stream_events
from ..services.metrics import get_dashboard_metrics, list_import_log, list_scopes
from ..services.metrics import generate_progress_alerts
from ..services.safety import enforce_safe_user_prompt, sanitize_input_text
from ..services.upload_ingest import import_attendance, import_erp

router = APIRouter(prefix="/api", tags=["api"])


class ApprovalPayload(BaseModel):
    stage: str = Field(pattern="^(manager|dept|rd|hr|finance)$")
    actor: str = Field(min_length=2, max_length=40)
    decision: str = Field(pattern="^(approved|rejected)$")
    comment: str = Field(default="", max_length=500)
    human_confirmed: bool = False


class LlmPayload(BaseModel):
    prompt: str = Field(min_length=1, max_length=2200)
    api_key: str | None = None
    provider: str = Field(default="deepseek", pattern="^(deepseek|openai)$")
    model: str | None = Field(default=None, max_length=64)


class CopilotPayload(BaseModel):
    month: str = DEFAULT_REPORT_MONTH
    scope_type: str = Field(default="company", pattern="^(company|department|project)$")
    scope_key: str = Field(default="COMPANY", max_length=64)
    api_key: str | None = None
    provider: str = Field(default="deepseek", pattern="^(deepseek|openai)$")
    model: str | None = Field(default=None, max_length=64)


class SimulationPayload(BaseModel):
    month: str = DEFAULT_REPORT_MONTH
    scope_type: str = Field(default="company", pattern="^(company|department|project)$")
    scope_key: str = Field(default="COMPANY", max_length=64)
    cost_class: str = Field(default="TOTAL", pattern="^(TOTAL|OPEX|CAPEX)$")
    add_outsourced_hkd: float = 0.0
    add_labor_hours: float = 0.0
    labor_cost_factor: float = 1.0


class AskPayload(BaseModel):
    question: str = Field(min_length=1, max_length=600)
    month: str = DEFAULT_REPORT_MONTH
    scope_type: str = Field(default="company", pattern="^(company|department|project)$")
    scope_key: str = Field(default="COMPANY", max_length=64)
    api_key: str | None = None
    provider: str = Field(default="deepseek", pattern="^(deepseek|openai)$")
    model: str | None = Field(default=None, max_length=64)


def _actor_guard(actor: str) -> str:
    clean = sanitize_input_text(actor, label="actor", max_len=40)
    low = clean.lower()
    if any(x in low for x in ("bot", "robot", "ai", "自动")):
        raise HTTPException(status_code=400, detail="审批人必须为人工账号，不可使用自动化账号标识")
    return clean


def _stream_line(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False) + "\n"


def _chunk_text(text: str, size: int = 56) -> list[str]:
    clean = str(text or "")
    if not clean:
        return []
    chunks: list[str] = []
    start = 0
    total = len(clean)
    while start < total:
        chunks.append(clean[start : start + size])
        start += size
    return chunks


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/runtime-config")
def runtime_config() -> dict[str, Any]:
    return {
        "llm": {
            "default_provider": (os.getenv("DEFAULT_LLM_PROVIDER") or "deepseek").strip().lower(),
            "default_models": {
                "deepseek": (os.getenv("DEFAULT_DEEPSEEK_MODEL") or "deepseek-chat").strip(),
                "openai": (os.getenv("DEFAULT_OPENAI_MODEL") or "gpt-4o-mini").strip(),
            },
            "server_keys": {
                "deepseek": bool((os.getenv("DS_API_KEY") or "").strip()),
                "openai": bool((os.getenv("OPENAI_API_KEY") or "").strip()),
            },
        }
    }


@router.post("/load-initial-data")
def load_initial_data() -> dict[str, Any]:
    try:
        stats = load_all_sources()
        return {"ok": True, "stats": stats}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"初始化失败: {exc}") from exc


@router.get("/scopes")
def scopes() -> dict[str, Any]:
    return list_scopes()


@router.get("/dashboard")
def dashboard(
    scope_type: str = "company",
    scope_key: str = "COMPANY",
    month: str = DEFAULT_REPORT_MONTH,
) -> dict[str, Any]:
    try:
        return get_dashboard_metrics(month=month, scope_type=scope_type, scope_key=scope_key)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/alerts")
def alerts(
    scope_type: str = "company",
    scope_key: str = "COMPANY",
    month: str = DEFAULT_REPORT_MONTH,
) -> dict[str, Any]:
    return {"items": generate_progress_alerts(month=month, scope_type=scope_type, scope_key=scope_key)}


@router.get("/imports")
def imports() -> dict[str, Any]:
    return {"items": list_import_log()}


@router.get("/anomalies/labor")
def anomalies(month: str = DEFAULT_REPORT_MONTH) -> dict[str, Any]:
    return labor_growth_anomalies(month=month)


@router.get("/compliance")
def compliance(month: str = DEFAULT_REPORT_MONTH) -> dict[str, Any]:
    return compliance_checks(month=month)


@router.get("/approvals/pending")
def approvals_pending(stage: str = "manager", month: str | None = None) -> dict[str, Any]:
    try:
        rows = list_pending_approvals(stage=stage, month=month)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"stage": stage, "items": rows}


@router.post("/approvals/{timesheet_id}/action")
def approval_action(timesheet_id: int, payload: ApprovalPayload) -> dict[str, Any]:
    if not payload.human_confirmed:
        raise HTTPException(status_code=400, detail="审批动作必须由人工确认后提交")
    actor = _actor_guard(payload.actor)
    comment = (payload.comment or "").strip()
    if payload.decision == "rejected" and len(comment) < 2:
        raise HTTPException(status_code=400, detail="驳回时必须填写意见")
    try:
        updated = approve_timesheet(
            timesheet_id=timesheet_id,
            stage=payload.stage,
            actor=actor,
            decision=payload.decision,
            comment=comment,
        )
        return {"ok": True, "item": updated}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/forecast")
def forecast_api(
    scope_type: str = "company",
    scope_key: str = "COMPANY",
    cost_class: str = "TOTAL",
    horizon: int = 2,
) -> dict[str, Any]:
    return forecast(scope_type=scope_type, scope_key=scope_key, cost_class=cost_class, horizon=horizon)


@router.get("/suggestions/hours")
def suggestion_api(month: str = DEFAULT_REPORT_MONTH) -> dict[str, Any]:
    return dynamic_hour_suggestions(month=month)


@router.post("/llm/analyze")
def llm_analyze(payload: LlmPayload) -> dict[str, Any]:
    try:
        prompt = enforce_safe_user_prompt(payload.prompt, label="prompt", max_len=2200)
        return chat_summary(
            prompt=prompt,
            api_key=payload.api_key,
            provider=payload.provider,
            model=payload.model,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except LLMError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"LLM 调用失败: {exc}") from exc


@router.post("/llm/analyze/stream")
def llm_analyze_stream(payload: LlmPayload) -> StreamingResponse:
    try:
        prompt = enforce_safe_user_prompt(payload.prompt, label="prompt", max_len=2200)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    def iterator():
        try:
            for event in chat_summary_stream_events(
                prompt=(
                    f"{prompt}\n\n"
                    "输出要求（必须遵守）：\n"
                    "1) 使用 Markdown 输出。\n"
                    "2) 给出“### 结论 / ### 关键风险 / ### 建议动作”三个小节。\n"
                    "3) 关键结论与关键数字使用 **粗体**。\n"
                    "4) 最关键一句额外使用 __下划线__ 强调。"
                ),
                api_key=payload.api_key,
                provider=payload.provider,
                model=payload.model,
            ):
                yield _stream_line(event)
            yield _stream_line({"type": "done"})
        except LLMError as exc:
            yield _stream_line({"type": "error", "message": str(exc)})
        except ValueError as exc:
            yield _stream_line({"type": "error", "message": str(exc)})
        except Exception as exc:  # noqa: BLE001
            yield _stream_line({"type": "error", "message": f"流式分析失败: {exc}"})

    return StreamingResponse(iterator(), media_type="application/x-ndjson")


@router.post("/ai/copilot/brief")
def ai_copilot(payload: CopilotPayload) -> dict[str, Any]:
    try:
        return copilot_brief(
            month=payload.month,
            scope_type=payload.scope_type,
            scope_key=payload.scope_key,
            api_key=payload.api_key,
            provider=payload.provider,
            model=payload.model,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Copilot 生成失败: {exc}") from exc


@router.post("/ai/simulate")
def ai_simulate(payload: SimulationPayload) -> dict[str, Any]:
    try:
        return scenario_simulation(
            month=payload.month,
            scope_type=payload.scope_type,
            scope_key=payload.scope_key,
            cost_class=payload.cost_class,
            add_outsourced_hkd=payload.add_outsourced_hkd,
            add_labor_hours=payload.add_labor_hours,
            labor_cost_factor=payload.labor_cost_factor,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"情景推演失败: {exc}") from exc


@router.post("/ai/ask")
def ai_ask(payload: AskPayload) -> dict[str, Any]:
    try:
        question = enforce_safe_user_prompt(payload.question, label="question", max_len=600)
        return ask_data(
            question=question,
            month=payload.month,
            scope_type=payload.scope_type,
            scope_key=payload.scope_key,
            api_key=payload.api_key,
            provider=payload.provider,
            model=payload.model,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"智能问答失败: {exc}") from exc


@router.post("/ai/ask/stream")
def ai_ask_stream(payload: AskPayload) -> StreamingResponse:
    try:
        question = enforce_safe_user_prompt(payload.question, label="question", max_len=600)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    base = ask_data(
        question=question,
        month=payload.month,
        scope_type=payload.scope_type,
        scope_key=payload.scope_key,
        api_key=None,
    )
    base_answer = str(base.get("answer") or "")
    context = base.get("context") or {}

    def iterator():
        yield _stream_line({"type": "status", "text": "正在检索当前真实数据上下文..."})
        try:
            ask_prompt = (
                "请只基于给定数据回答，禁止编造未给出的角色、责任人、组织任命信息。\n"
                "输出要求（必须遵守）：\n"
                "1) 使用 Markdown 输出；\n"
                "2) 包含“### 结论 / ### 关键依据 / ### 建议动作”三个小节；\n"
                "3) 关键结论、关键数字使用 **粗体**；\n"
                "4) 最关键一句使用 __下划线__ 强调；\n"
                "5) 如果数据不足，明确写“数据不足：xxx”。\n\n"
                f"问题: {question}\n"
                f"规则引擎回答: {base_answer}\n"
                f"上下文: {context}\n"
                f"月份: {payload.month}\n"
                f"范围: {payload.scope_type}/{payload.scope_key}"
            )
            for event in chat_summary_stream_events(
                prompt=ask_prompt,
                api_key=payload.api_key,
                provider=payload.provider,
                model=payload.model,
                system_prompt="你是研发投入数据问答助手，只可依据提供的真实数据上下文作答。",
            ):
                if event.get("type") == "meta":
                    event["mode"] = "llm"
                yield _stream_line(event)
            yield _stream_line({"type": "done", "mode": "llm"})
        except LLMError as exc:
            yield _stream_line({"type": "status", "text": f"模型不可用，已回退规则引擎：{exc}"})
            yield _stream_line({"type": "meta", "mode": "rule"})
            for piece in _chunk_text(base_answer):
                yield _stream_line({"type": "delta", "content": piece})
            yield _stream_line({"type": "done", "mode": "rule"})
        except Exception as exc:  # noqa: BLE001
            yield _stream_line({"type": "error", "message": f"流式问答失败: {exc}"})
            yield _stream_line({"type": "meta", "mode": "rule"})
            for piece in _chunk_text(base_answer):
                yield _stream_line({"type": "delta", "content": piece})
            yield _stream_line({"type": "done", "mode": "rule"})

    return StreamingResponse(iterator(), media_type="application/x-ndjson")


@router.get("/ai/approvals/recommend")
def ai_approval_recommend(stage: str = "finance", month: str = DEFAULT_REPORT_MONTH) -> dict[str, Any]:
    try:
        return approval_recommendations(stage=stage, month=month)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"审批建议生成失败: {exc}") from exc


def _save_upload(file: UploadFile) -> Path:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target = UPLOAD_DIR / file.filename
    with target.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    return target


@router.post("/upload/erp")
def upload_erp(file: UploadFile = File(...)) -> dict[str, Any]:
    try:
        path = _save_upload(file)
        count = import_erp(path)
        return {"ok": True, "loaded_rows": count, "file": path.name}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"ERP 导入失败: {exc}") from exc


@router.post("/upload/attendance")
def upload_attendance(file: UploadFile = File(...)) -> dict[str, Any]:
    try:
        path = _save_upload(file)
        count = import_attendance(path)
        return {"ok": True, "loaded_rows": count, "file": path.name}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"考勤导入失败: {exc}") from exc


@router.get("/timesheets")
def timesheets(month: str | None = None) -> dict[str, Any]:
    with db_session() as conn:
        where = "WHERE month = ?" if month else ""
        params = [month] if month else []
        rows = conn.execute(
            f"""
            SELECT t.id, t.month, t.employee_name, t.project_code, t.project_name, t.declared_hours,
                   d.dept_name, t.manager_approved, t.dept_approved, t.rd_approved, t.hr_approved, t.finance_approved
            FROM labor_timesheets t
            LEFT JOIN departments d ON d.dept_code = t.dept_code
            {where}
            ORDER BY t.month, t.project_code, t.employee_name
            """,
            params,
        ).fetchall()
    items = []
    for r in rows:
        item = dict(r)
        item["employee_name"] = mask_name(item["employee_name"])
        items.append(item)
    return {"items": items}
