"""AI summary generation for delivered channel tweets."""

import asyncio
import json
from datetime import datetime, timezone, timedelta

import aiohttp
from loguru import logger

from . import config

SUMMARY_SYSTEM_PROMPT = (
    "你是一位专注加密货币、交易所动态和市场信息的中文情报分析师。"
    "用户会提供某个频道在一个时间窗口内收到的推文列表，"
    "其中 reference 表示被回复、被引用、被转推或被删除的关联原文。"
    "请只基于输入内容总结，不要编造未出现的信息。"
    "输出简体中文，结构清晰，适合直接推送到 Telegram 和飞书。"
)


async def summarize_channel_tweets(
    label: str,
    items: list[dict],
    window_start: int,
    window_end: int,
    total_count: int | None = None,
    truncated: bool = False,
) -> str | None:
    if not config.DEEPSEEK_API_KEY or not items:
        return None

    user_payload = {
        "channel": label,
        "window": {
            "start": _fmt_ts(window_start),
            "end": _fmt_ts(window_end),
        },
        "tweet_count": total_count or len(items),
        "included_tweet_count": len(items),
        "truncated": truncated,
        "tweets": [_format_item(item) for item in items],
    }
    payload_text = json.dumps(user_payload, ensure_ascii=False)
    if len(payload_text) > 18000:
        payload_text = payload_text[:18000] + "\n[输入过长，后续推文已截断]"

    prompt = (
        "请生成频道定时摘要，格式如下：\n"
        "1. 标题：包含频道名、时间窗口、推文数量。\n"
        "2. 核心要点：3-6条，按重要性排序。\n"
        "3. 重点事件/公告：列出具体事件、涉及账号和影响。\n"
        "4. 市场含义：用谨慎语气总结可能影响，不做投资建议。\n"
        "5. 值得回看：挑选最多5条推文，附作者和链接。\n\n"
        "如果 truncated=true，请在摘要开头明确说明本次只覆盖部分推文样本。\n\n"
        f"输入数据：\n{payload_text}"
    )

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.DEEPSEEK_API_KEY}",
    }
    request_payload = {
        "model": config.DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "temperature": 0.2,
        "max_tokens": 3000,
    }

    max_retries = 2
    for attempt in range(1, max_retries + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=config.SUMMARY_AI_TIMEOUT_SECONDS)
            proxy_url = getattr(config, "PROXY_SERVER", "socks5://127.0.0.1:40000")
            from aiohttp_socks import ProxyConnector

            connector = ProxyConnector.from_url(proxy_url, rdns=True) if proxy_url else None
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                async with session.post(
                    f"{config.DEEPSEEK_BASE_URL}/chat/completions",
                    headers=headers,
                    json=request_payload,
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(f"🧾 DeepSeek 频道总结失败 [{resp.status}]: {body[:200]}")
                        if resp.status >= 500 and attempt < max_retries:
                            await asyncio.sleep(2)
                            continue
                        return None

                    data = await resp.json()
                    return data["choices"][0]["message"]["content"].strip()
        except asyncio.TimeoutError:
            logger.warning(f"🧾 DeepSeek 频道总结超时 (第 {attempt} 次尝试)")
        except aiohttp.ClientError as e:
            logger.warning(f"🧾 DeepSeek 频道总结网络异常 (第 {attempt} 次尝试): {e}")
        except Exception as e:
            logger.error(f"🧾 DeepSeek 频道总结发生预期外错误: {repr(e)}")
            return None

        if attempt < max_retries:
            await asyncio.sleep(1)

    return None


def _format_item(item: dict) -> dict:
    content = item.get("content_text") or ""
    reference = item.get("reference_text") or ""
    content, reference = _limit_item_texts(
        content,
        reference,
        max(100, config.SUMMARY_TWEET_TEXT_LIMIT),
    )
    raw_message = _load_raw_message(item.get("raw_json"))
    raw_reference = raw_message.get("reference") or {}
    reference_author_handle = raw_reference.get("author_handle") or ""
    reference_author_name = raw_reference.get("author_name") or reference_author_handle
    reference_tweet_id = raw_reference.get("tweet_id") or ""

    formatted = {
        "time": _fmt_ts(int(item.get("timestamp") or item.get("delivered_at") or 0)),
        "author": f"{item.get('author_name') or item.get('author_handle')} @{item.get('author_handle')}",
        "action": item.get("action") or "",
        "content": content,
        "url": item.get("tweet_url") or "",
    }
    if reference or reference_author_handle or reference_tweet_id:
        formatted["reference"] = {
            "relation": raw_reference.get("type") or _relation_from_action(item.get("action") or ""),
            "author": (
                f"{reference_author_name} @{reference_author_handle}"
                if reference_author_handle
                else ""
            ),
            "content": reference,
            "url": _build_reference_url(reference_author_handle, reference_tweet_id),
        }
    return formatted


def _limit_item_texts(content: str, reference: str, limit: int) -> tuple[str, str]:
    if len(content) + len(reference) <= limit:
        return content, reference
    if not reference:
        return _trim_text(content, limit), ""
    if not content:
        return "", _trim_text(reference, limit)

    content_budget = min(len(content), int(limit * 0.6))
    reference_budget = limit - content_budget
    if len(reference) < reference_budget:
        content_budget = limit - len(reference)
    elif len(content) < content_budget:
        reference_budget = limit - len(content)
    return _trim_text(content, content_budget), _trim_text(reference, reference_budget)


def _trim_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    suffix = "...[截断]"
    return text[: max(0, limit - len(suffix))] + suffix


def _load_raw_message(raw_json: str | None) -> dict:
    if not raw_json:
        return {}
    try:
        data = json.loads(raw_json)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _relation_from_action(action: str) -> str:
    return {
        "repost": "retweeted",
        "reply": "replied_to",
        "quote": "quoted",
        "delete_post": "deleted",
    }.get(action, "")


def _build_reference_url(handle: str, tweet_id: str) -> str:
    if handle and tweet_id:
        return f"https://x.com/{handle}/status/{tweet_id}"
    if handle:
        return f"https://x.com/{handle}"
    return ""


def _fmt_ts(ts: int) -> str:
    if not ts:
        return "未知"
    tz_cst = timezone(timedelta(hours=8))
    return datetime.fromtimestamp(ts, tz=tz_cst).strftime("%Y-%m-%d %H:%M")
