from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from typing import Any

import requests
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .safety import enforce_safe_user_prompt, redact_secrets, sanitize_output_text


class LLMError(RuntimeError):
    pass


RETRYABLE_STATUS = {408, 409, 429, 500, 502, 503, 504}
PROVIDER_SPECS: dict[str, dict[str, Any]] = {
    "deepseek": {
        "name": "DeepSeek",
        "url": "https://api.deepseek.com/chat/completions",
        "env_key": "DS_API_KEY",
        "default_models": ("deepseek-chat", "deepseek-reasoner"),
    },
    "openai": {
        "name": "OpenAI",
        "url": "https://api.openai.com/v1/chat/completions",
        "env_key": "OPENAI_API_KEY",
        "default_models": ("gpt-4o-mini", "gpt-4.1-mini"),
    },
}


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=0.6,
        allowed_methods=frozenset(["POST"]),
        status_forcelist=RETRYABLE_STATUS,
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


SESSION = _build_session()


def _normalize_provider(provider: str | None) -> str:
    p = (provider or "deepseek").strip().lower()
    if p not in PROVIDER_SPECS:
        allowed = ", ".join(sorted(PROVIDER_SPECS))
        raise LLMError(f"不支持的模型提供商: {p}. 支持: {allowed}")
    return p


def _provider_token(provider: str, api_key: str | None) -> str:
    spec = PROVIDER_SPECS[provider]
    token = (api_key or os.getenv(spec["env_key"]) or "").strip()
    if not token:
        raise LLMError(f"缺少 {spec['name']} API Key")
    return token


def _clean_error_text(text: str) -> str:
    return sanitize_output_text(text, max_len=180)


def _extract_content(data: dict[str, Any]) -> str:
    try:
        content = data["choices"][0]["message"]["content"]
    except Exception as exc:  # noqa: BLE001
        raise LLMError("LLM 返回结构异常") from exc
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text", "")))
        content = "\n".join(parts)
    return str(content or "")


def _request_chat(
    *,
    provider: str,
    token: str,
    messages: list[dict[str, str]],
    model: str,
    temperature: float = 0.2,
    response_format: dict[str, Any] | None = None,
    max_tokens: int = 1024,
) -> tuple[Response, dict[str, Any]]:
    spec = PROVIDER_SPECS[provider]
    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "stream": False,
    }
    if response_format:
        payload["response_format"] = response_format
    response = SESSION.post(
        spec["url"],
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        json=payload,
        timeout=(8, 35),
    )
    try:
        data = response.json()
    except Exception:  # noqa: BLE001
        data = {}
    return response, data


def _run_with_fallback(
    *,
    provider: str,
    token: str,
    messages: list[dict[str, str]],
    response_format: dict[str, Any] | None = None,
    temperature: float = 0.2,
    model: str | None = None,
) -> dict[str, Any]:
    spec = PROVIDER_SPECS[provider]
    if model and model.strip():
        models = (model.strip(),)
    else:
        models = tuple(spec["default_models"])
    last_error = "unknown"
    for idx, model_name in enumerate(models):
        try:
            response, data = _request_chat(
                provider=provider,
                token=token,
                messages=messages,
                model=model_name,
                temperature=temperature,
                response_format=response_format,
            )
        except requests.Timeout as exc:
            last_error = f"timeout@{model_name}"
            if idx < len(models) - 1:
                time.sleep(0.4)
                continue
            raise LLMError(f"{spec['name']} 请求超时，请稍后重试") from exc
        except requests.RequestException as exc:
            last_error = f"network@{model_name}"
            if idx < len(models) - 1:
                time.sleep(0.4)
                continue
            raise LLMError(f"{spec['name']} 网络异常，请稍后重试") from exc

        if response.status_code >= 400:
            body_message = ""
            if isinstance(data, dict):
                err = data.get("error")
                if isinstance(err, dict):
                    body_message = str(err.get("message") or "")
            clean = _clean_error_text(body_message)
            last_error = f"http{response.status_code}@{model_name}:{clean}"
            if response.status_code in RETRYABLE_STATUS and idx < len(models) - 1:
                time.sleep(0.5 + idx * 0.2)
                continue
            raise LLMError(f"{spec['name']} 请求失败: HTTP {response.status_code} {clean}".strip())

        content = _extract_content(data)
        return {
            "content": sanitize_output_text(content, max_len=10000),
            "usage": data.get("usage", {}),
            "model": model_name,
            "provider": provider,
            "request_id": response.headers.get("x-request-id", ""),
        }
    raise LLMError(f"{spec['name']} 请求失败，已尝试回退模型: {last_error}")


def _stream_chunk_text(data: dict[str, Any]) -> str:
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0] if isinstance(choices[0], dict) else {}
    delta = first.get("delta")
    if not isinstance(delta, dict):
        return ""
    content = delta.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text":
                parts.append(str(block.get("text", "")))
        return "".join(parts)
    return ""


def _request_chat_stream(
    *,
    provider: str,
    token: str,
    messages: list[dict[str, str]],
    model: str,
    temperature: float = 0.2,
    max_tokens: int = 1024,
) -> Response:
    spec = PROVIDER_SPECS[provider]
    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "stream": True,
    }
    try:
        response = SESSION.post(
            spec["url"],
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json=payload,
            timeout=(8, 70),
            stream=True,
        )
    except requests.Timeout as exc:
        raise LLMError(f"{spec['name']} 请求超时，请稍后重试") from exc
    except requests.RequestException as exc:
        raise LLMError(f"{spec['name']} 网络异常，请稍后重试") from exc

    if response.status_code >= 400:
        message = ""
        try:
            payload = response.json()
            if isinstance(payload, dict):
                err = payload.get("error")
                if isinstance(err, dict):
                    message = str(err.get("message") or "")
        except Exception:  # noqa: BLE001
            message = ""
        clean = _clean_error_text(message)
        response.close()
        raise LLMError(f"{spec['name']} 请求失败: HTTP {response.status_code} {clean}".strip())
    return response


def _iter_stream_chunks(response: Response) -> Iterator[str]:
    try:
        for raw in response.iter_lines(decode_unicode=True):
            if raw is None:
                continue
            line = str(raw).strip()
            if not line or line.startswith(":"):
                continue
            if not line.startswith("data:"):
                continue
            payload = line[5:].strip()
            if not payload:
                continue
            if payload == "[DONE]":
                break
            try:
                obj = json.loads(payload)
            except json.JSONDecodeError:
                continue
            piece = _stream_chunk_text(obj)
            if not piece:
                continue
            safe_piece = redact_secrets(str(piece).replace("\x00", "").replace("\r", ""))
            if safe_piece:
                yield safe_piece
    finally:
        response.close()


def chat_summary_stream_events(
    *,
    prompt: str,
    api_key: str | None = None,
    provider: str = "deepseek",
    model: str | None = None,
    system_prompt: str = "你是研发投入管理分析师，请基于真实指标给出结论、风险和执行建议。",
) -> Iterator[dict[str, Any]]:
    selected_provider = _normalize_provider(provider)
    token = _provider_token(selected_provider, api_key)
    clean_prompt = enforce_safe_user_prompt(prompt, label="prompt", max_len=2200)
    spec = PROVIDER_SPECS[selected_provider]
    if model and model.strip():
        models = (model.strip(),)
    else:
        models = tuple(spec["default_models"])

    yield {"type": "status", "text": f"正在连接 {spec['name']} 模型..."}
    last_error = ""
    for idx, model_name in enumerate(models):
        if len(models) > 1:
            yield {"type": "status", "text": f"尝试模型 {model_name}..."}
        try:
            response = _request_chat_stream(
                provider=selected_provider,
                token=token,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": clean_prompt},
                ],
                model=model_name,
                temperature=0.2,
            )
            emitted_meta = False
            for chunk in _iter_stream_chunks(response):
                if not emitted_meta:
                    emitted_meta = True
                    yield {
                        "type": "meta",
                        "provider": selected_provider,
                        "model": model_name,
                    }
                yield {"type": "delta", "content": chunk}
            if not emitted_meta:
                yield {
                    "type": "meta",
                    "provider": selected_provider,
                    "model": model_name,
                }
            return
        except LLMError as exc:
            last_error = str(exc)
            if idx < len(models) - 1:
                yield {"type": "status", "text": f"{model_name} 暂不可用，切换候选模型..."}
                time.sleep(0.25)
                continue
            raise
    raise LLMError(last_error or f"{spec['name']} 流式请求失败")


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start < 0 or end <= start:
            raise LLMError("LLM 未返回 JSON 对象")
        try:
            obj = json.loads(raw[start : end + 1])
        except json.JSONDecodeError as exc:
            raise LLMError("LLM JSON 解析失败") from exc
    if not isinstance(obj, dict):
        raise LLMError("LLM 输出不是 JSON 对象")
    return obj


def chat_summary(
    *,
    prompt: str,
    api_key: str | None = None,
    provider: str = "deepseek",
    model: str | None = None,
    system_prompt: str = "你是研发投入管理分析师，请基于真实指标给出结论、风险和执行建议。",
) -> dict[str, Any]:
    selected_provider = _normalize_provider(provider)
    token = _provider_token(selected_provider, api_key)
    clean_prompt = enforce_safe_user_prompt(prompt, label="prompt", max_len=2200)
    result = _run_with_fallback(
        provider=selected_provider,
        token=token,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": clean_prompt},
        ],
        temperature=0.2,
        model=model,
    )
    return {
        "content": result["content"],
        "usage": result.get("usage", {}),
        "model": result.get("model"),
        "provider": result.get("provider"),
        "request_id": result.get("request_id"),
    }


def chat_json(
    *,
    system_prompt: str,
    user_prompt: str,
    schema_hint: str,
    api_key: str | None = None,
    provider: str = "deepseek",
    model: str | None = None,
) -> dict[str, Any]:
    selected_provider = _normalize_provider(provider)
    token = _provider_token(selected_provider, api_key)
    clean_user_prompt = enforce_safe_user_prompt(user_prompt, label="prompt", max_len=2200)
    ask = (
        f"{clean_user_prompt}\n\n"
        "输出约束：\n"
        f"{schema_hint}\n"
        "必须仅输出一个 JSON 对象，不要输出其他说明文本。"
    )
    result = _run_with_fallback(
        provider=selected_provider,
        token=token,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": ask},
        ],
        response_format={"type": "json_object"},
        temperature=0.0,
        model=model,
    )
    obj = _extract_json_object(result["content"])
    return {
        "object": obj,
        "usage": result.get("usage", {}),
        "model": result.get("model"),
        "provider": result.get("provider"),
        "request_id": result.get("request_id"),
    }


# backward compatible wrappers
def deepseek_summary(prompt: str, api_key: str | None = None) -> dict[str, Any]:
    return chat_summary(prompt=prompt, api_key=api_key, provider="deepseek")


def deepseek_json(
    *,
    system_prompt: str,
    user_prompt: str,
    schema_hint: str,
    api_key: str | None = None,
) -> dict[str, Any]:
    return chat_json(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        schema_hint=schema_hint,
        api_key=api_key,
        provider="deepseek",
    )
