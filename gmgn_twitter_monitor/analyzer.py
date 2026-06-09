"""推文深度分析模块 — 投资赛道分类 + 摘要 + 翻译（单次 DeepSeek 调用）。

仅对 config.AI_ANALYZE_HANDLES 中指定的 handle 生效，
其余博主仍走 translator.py 的纯翻译链路。
"""

import asyncio
import json

import aiohttp
from loguru import logger

from . import config

ANALYZE_SYSTEM_PROMPT = (
    "你是一位专业的金融推文分析助手。用户会输入一段 JSON，包含一条或多条推文文本字段。\n"
    "你需要完成三件事，并以严格的 JSON 格式返回结果：\n\n"
    "1. **赛道分类 (category)**：判断该推文所属的投资赛道。\n"
    "   - 如果明确涉及 A股（中国A股市场），必须标注「A股」，且必须在后面附带提及的股票名称和代码，格式为「A股·股票名称(股票代码)」\n"
    "     例如：「A股·比亚迪(002594)」「A股·贵州茅台(600519)、宁德时代(300750)」\n"
    "     如果推文涉及 A 股但未提及具体个股，则仅标注「A股」\n"
    "   - 如果明确涉及 美股（美国股市），必须标注「美股」\n"
    "   - 如果涉及 加密货币/Web3/区块链，标注「加密货币」\n"
    "   - 如果涉及 港股，标注「港股」\n"
    "   - 如果涉及 外汇/汇率，标注「外汇」\n"
    "   - 如果涉及 大宗商品/原油/黄金，标注「商品」\n"
    "   - 如果涉及 宏观经济/政策/央行，标注「宏观经济」\n"
    "   - 如果涉及多个赛道，用顿号连接，如「A股·比亚迪(002594)、美股」\n"
    "   - 如果属于投资领域但无法判断具体类型，标注「其他投资」\n"
    "   - 如果与投资完全无关（如日常生活），标注「非投资内容」\n\n"
    "2. **摘要 (summary)**：用简体中文浓缩推文核心观点，30-60字，抓住最关键信息。\n\n"
    "3. **翻译**：将推文中所有外语文本翻译为简体中文，保持原有键名不变。\n"
    "   - 保留原文中的 @用户名、$代币符号、URL 链接和 emoji 不翻译\n"
    "   - 如果某段文本已经是中文，则原样保留\n\n"
    "4. **A股个股提取 (stocks)**（仅当 category 包含 A股 时返回）：\n"
    "   提取推文中提及的所有 A 股个股信息，以 JSON 数组格式返回。\n"
    "   每个元素包含 name（股票中文名）和 code（股票代码，如 002594、600519）。\n"
    "   如果未提及具体个股，返回空数组 []。\n\n"
    "返回格式（严格 JSON，不要 markdown 代码块）：\n"
    '{"category": "赛道名称", "summary": "摘要内容", "content": "翻译后的正文", "reference": "翻译后的引用文本", "stocks": [{"name": "比亚迪", "code": "002594"}]}\n'
    "注意：content 和 reference 字段只有在用户输入中存在对应字段时才返回。stocks 字段仅在 category 包含 A股 时返回。"
)


async def analyze_tweet(texts_dict: dict[str, str], handle: str = "") -> dict | None:
    """对推文进行投资赛道分类 + 摘要 + 翻译，一次 DeepSeek 调用完成。

    输入: {"content": "推文正文", "reference": "引用/回复原文"} (字段可选)
    返回: {"category": "A股", "summary": "...", "content": "...", "reference": "..."}
    失败时返回 None。
    """
    if not config.DEEPSEEK_API_KEY or not texts_dict:
        return None

    valid_texts = {k: v for k, v in texts_dict.items() if v and len(v.strip()) > 0}
    if not valid_texts:
        return None

    is_aleabi = handle.lower() == "aleabitoreddit"
    truncate_limit = 3000 if is_aleabi else 1000

    # 截断超长推文，平衡分析质量与延迟
    for k, v in valid_texts.items():
        if len(v) > truncate_limit:
            valid_texts[k] = v[:truncate_limit] + "...\n[⬇️ 原文过长已截断]"

    system_prompt = ANALYZE_SYSTEM_PROMPT
    if is_aleabi:
        system_prompt += (
            "\n\n【特别指令】当前作者为 aleabitoreddit。"
            "\n在摘要和正文翻译中，请正常总结和翻译所有内容。"
            "\n但如果推文中提及了『A股股票』，请务必明确提取并标注出对应的股票名称及代码！"
        )

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.DEEPSEEK_API_KEY}",
    }
    payload = {
        "model": config.DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(valid_texts, ensure_ascii=False)},
        ],
        "stream": False,
        "temperature": 0.3,
        "max_tokens": 2048,
        "response_format": {"type": "json_object"},
    }

    MAX_RETRIES = 2
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=45)

            proxy_url = getattr(config, "PROXY_SERVER", "socks5://127.0.0.1:40000")
            from aiohttp_socks import ProxyConnector

            connector = (
                ProxyConnector.from_url(proxy_url, rdns=True) if proxy_url else None
            )

            async with aiohttp.ClientSession(
                timeout=timeout, connector=connector
            ) as session:
                async with session.post(
                    f"{config.DEEPSEEK_BASE_URL}/chat/completions",
                    headers=headers,
                    json=payload,
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(
                            f"🧠 DeepSeek 分析失败 [{resp.status}]: {body[:200]}"
                        )
                        if resp.status >= 500 and attempt < MAX_RETRIES:
                            await asyncio.sleep(2)
                            continue
                        return None

                    data = await resp.json()
                    result = data["choices"][0]["message"]["content"].strip()

                    # 容错：去除 markdown json 代码块
                    if result.startswith("```json"):
                        result = result[7:]
                    if result.startswith("```"):
                        result = result[3:]
                    if result.endswith("```"):
                        result = result[:-3]

                    result = result.strip()
                    try:
                        parsed = json.loads(result)
                        # 校验必须包含 category 和 summary
                        if "category" not in parsed or "summary" not in parsed:
                            logger.warning(
                                f"🧠 分析结果缺少 category/summary 字段: {result[:200]}"
                            )
                            return None
                        return parsed
                    except json.JSONDecodeError:
                        logger.error(
                            f"🧠 分析结果无法解析为 JSON: {result[:200]}"
                        )
                        return None

        except asyncio.TimeoutError:
            logger.warning(f"🧠 DeepSeek 分析超时 (第 {attempt} 次尝试)")
            if attempt < MAX_RETRIES:
                continue
            return None
        except aiohttp.ClientError as e:
            logger.warning(
                f"🧠 DeepSeek 分析网络异常 (第 {attempt} 次尝试): {e}"
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1)
                continue
            return None
        except Exception as e:
            logger.error(f"🧠 DeepSeek 分析发生预期外错误: {repr(e)}")
            return None

    return None
