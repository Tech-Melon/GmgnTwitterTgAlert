import json
from typing import Any

from .models import Author, AvatarChange, BannerChange, BioChange, Content, Media, Reference, StandardizedMessage, UnfollowTarget


def parse_socketio_payload(frame_data: Any) -> dict | None:
    if not isinstance(frame_data, str):
        return None

    if "twitter_user_monitor_basic" not in frame_data:
        return None

    payload_str = frame_data.lstrip("0123456789")
    if not payload_str:
        return None

    parsed = json.loads(payload_str)
    if isinstance(parsed, list) and len(parsed) >= 2:
        parsed = parsed[1]

    if isinstance(parsed, str):
        parsed = json.loads(parsed)

    if not parsed:
        return None

    if parsed.get("channel") != "twitter_user_monitor_basic":
        return None

    if not isinstance(parsed.get("data"), list):
        return None

    return parsed


def _normalize_action_type(item: dict) -> str:
    action_type = item.get("tw", "unknown")
    if action_type != "other":
        return action_type

    p_data = item.get("p")
    if isinstance(p_data, dict):
        banner_keys = {"bb", "ab", "b", "banner", "before_banner", "after_banner", "old_banner"}
        if any(key in p_data for key in banner_keys):
            return "banner"
    return action_type


def extract_triggers_map(items: list[dict]) -> dict[str, str]:
    triggers_map = {}
    for tweet_data in items:
        if not tweet_data:
            continue

        action_type = _normalize_action_type(tweet_data)
        u_data = tweet_data.get("u")
        if u_data and isinstance(u_data, dict):
            user_handle = u_data.get("s")
            if user_handle:
                triggers_map[user_handle] = action_type
    return triggers_map


def _build_media_list(raw_media: list | None) -> list[Media]:
    """从原始 m 数组构建 Media 列表。"""
    if not raw_media or not isinstance(raw_media, list):
        return []
    return [Media(type=m.get("t"), url=m.get("u")) for m in raw_media if isinstance(m, dict)]


def _build_reference(item: dict, action_type: str) -> Reference | None:
    """从 si/su/sc 字段构建引用信息。"""
    if "su" not in item:
        return None

    su = item.get("su", {}) or {}
    sc = item.get("sc", {}) or {}

    ref_type_map = {
        "repost": "retweeted",
        "reply": "replied_to",
        "quote": "quoted",
        "delete_post": "deleted",
    }
    ref_type = ref_type_map.get(action_type, "referenced")

    return Reference(
        tweet_id=item.get("si"),
        author_handle=su.get("s"),
        author_name=su.get("n"),
        author_avatar=su.get("a"),
        author_followers=su.get("f"),
        text=sc.get("t") if isinstance(sc, dict) else None,
        media=_build_media_list(sc.get("m") if isinstance(sc, dict) else None),
        type=ref_type,
    )


def _build_unfollow_target(item: dict) -> UnfollowTarget | None:
    """从 f.f 字段构建取关目标信息（仅 unfollow 动作）。"""
    f_data = item.get("f")
    if not f_data or not isinstance(f_data, dict):
        return None

    target = f_data.get("f")
    if not target or not isinstance(target, dict):
        return None

    return UnfollowTarget(
        handle=target.get("s"),
        name=target.get("n"),
        bio=target.get("d"),
        avatar=target.get("a"),
        banner=target.get("b"),
        followers=target.get("f"),
    )


def _build_avatar_change(item: dict) -> AvatarChange | None:
    """从 p 字段构建头像变更信息（仅 photo 动作）。"""
    p_data = item.get("p")
    if not p_data or not isinstance(p_data, dict):
        return None

    return AvatarChange(
        before=p_data.get("ba"),
        after=p_data.get("aa"),
    )


def _build_bio_change(item: dict) -> BioChange | None:
    """从 p 字段构建简介变更信息（仅 description 动作）。"""
    p_data = item.get("p")
    if not p_data or not isinstance(p_data, dict):
        return None

    return BioChange(
        before=p_data.get("bd"),
        after=p_data.get("d"),
    )


def _build_banner_change(item: dict) -> BannerChange | None:
    """从 p 字段构建横幅变更信息（仅 banner 动作）。"""
    p_data = item.get("p")
    if not p_data or not isinstance(p_data, dict):
        return None

    return BannerChange(
        before=p_data.get("bb") or p_data.get("before_banner") or p_data.get("old_banner"),
        after=p_data.get("ab") or p_data.get("b") or p_data.get("banner") or p_data.get("after_banner"),
    )


def build_standardized_message(item: dict) -> StandardizedMessage:
    action_type = _normalize_action_type(item)

    # 用户信息
    u = item.get("u", {}) or {}
    author = Author(
        handle=u.get("s"),
        name=u.get("n"),
        avatar=u.get("a"),
        followers=u.get("f"),
        tags=item.get("ut", []),
    )

    # 内容：tweet/quote/reply/delete_post 有 c 字段，repost/unfollow 无
    main_content = item.get("c", {}) or {}
    if not isinstance(main_content, dict):
        main_content = {}

    content = Content(
        text=main_content.get("t"),
        media=_build_media_list(main_content.get("m")),
    )

    # 引用来源
    reference = _build_reference(item, action_type)

    # 关注/取关目标（复用 unfollow_target 字段）
    unfollow_target = _build_unfollow_target(item) if action_type in ("unfollow", "follow") else None

    # 头像变更（仅 photo）
    avatar_change = _build_avatar_change(item) if action_type == "photo" else None

    # 简介变更（仅 description）
    bio_change = _build_bio_change(item) if action_type == "description" else None

    # 横幅变更（仅 banner）
    banner_change = _build_banner_change(item) if action_type == "banner" else None

    # 时间戳：gmgn 给的是毫秒级，标准化为秒级
    raw_ts = item.get("ts", 0)
    try:
        ts_ms = int(raw_ts)
        timestamp = ts_ms // 1000 if ts_ms > 9_999_999_999 else ts_ms
    except (ValueError, TypeError):
        timestamp = 0

    return StandardizedMessage(
        action=action_type,
        original_action=item.get("stw"),  # 仅 delete_post 会有此字段
        tweet_id=item.get("ti"),
        internal_id=item.get("i"),
        timestamp=timestamp,
        author=author,
        content=content,
        reference=reference,
        unfollow_target=unfollow_target,
        avatar_change=avatar_change,
        bio_change=bio_change,
        banner_change=banner_change,
    )
