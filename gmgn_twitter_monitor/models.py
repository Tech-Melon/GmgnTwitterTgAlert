from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class Author:
    handle: Optional[str]
    name: Optional[str]
    avatar: Optional[str]
    followers: Optional[int]
    tags: list[str]


@dataclass
class Media:
    type: Optional[str]
    url: Optional[str]


@dataclass
class Content:
    text: Optional[str]
    media: list[Media]


@dataclass
class Reference:
    tweet_id: Optional[str]
    author_handle: Optional[str]
    author_name: Optional[str]
    author_avatar: Optional[str]
    author_followers: Optional[int]
    text: Optional[str]
    media: list[Media]
    type: str


@dataclass
class UnfollowTarget:
    handle: Optional[str]
    name: Optional[str]
    bio: Optional[str]
    avatar: Optional[str]
    banner: Optional[str]
    followers: Optional[int]


@dataclass
class AvatarChange:
    before: Optional[str]
    after: Optional[str]


@dataclass
class BioChange:
    before: Optional[str]
    after: Optional[str]


@dataclass
class BannerChange:
    before: Optional[str]
    after: Optional[str]


@dataclass
class StandardizedMessage:
    action: str
    original_action: Optional[str]
    tweet_id: Optional[str]
    internal_id: Optional[str]
    timestamp: int
    author: Author
    content: Content
    reference: Optional[Reference]
    unfollow_target: Optional[UnfollowTarget]
    avatar_change: Optional[AvatarChange]
    bio_change: Optional['BioChange']
    banner_change: Optional['BannerChange']

    def to_dict(self) -> dict:
        return asdict(self)
