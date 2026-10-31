"""Plan contract alerts and deliver through an injected sender and clock."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Protocol

from gammad.alerts import s089_cards as cards
from gammad.scanners import contract as sc
from gammad.scanners.targets import v1_target_prices

PERSONAL = "personal"
GROUP = "group"
SENT, FAILED, NO_CHAT, MUTED = "sent", "failed", "no_chat", "muted"


@dataclass(frozen=True)
class Post:
    chat: str
    copy: str
    key: str
    text: str
    security_id: int | None = None  # the contract's (D42 mute key); None = no contract (an ops line), never muted
    is_exit: bool = False  # T-737 (D732 (2)): a plan_exit card (an EXIT_KINDS event of an open trade); see _held


@dataclass(frozen=True)
class Delivery:
    chat: str
    key: str
    status: str
    sent_ms: int | None
    delay_ms: int | None
    error: str | None


class AlertSender(Protocol):
    async def send(self, chat_id: str | int, text: str, **kwargs: Any) -> Any: ...


def _edition(edition: str) -> None:
    if edition not in ("owner", "client"):
        raise ValueError("unknown edition")


def plan_entry(signal: sc.EntrySignal, *, edition: str, own_targets: sc.TargetsConfig,
               group_targets: sc.TargetsConfig,
               v2_targets: Callable[[sc.EntrySignal], Sequence[float]] | None = None) -> tuple[Post, ...]:
    _edition(edition)
    if signal.ban_reason is not None:
        return ()
    posts = []
    if cards.client_allowed(signal.scanner, signal.list_name):
        posts.append(Post(GROUP, cards.CLIENT_COPY, signal.key, cards.entry_card(
            signal, copy=cards.CLIENT_COPY, targets=v1_target_prices(signal.signal_px, group_targets),
        ), signal.security_id))
    if edition == "owner" and signal.risk_block is None:
        prices = (v1_target_prices(signal.signal_px, own_targets) if signal.scanner == sc.V1N else
                  tuple(v2_targets(signal)) if v2_targets is not None else ())
        posts.append(Post(PERSONAL, cards.OWNER_COPY, signal.key,
                          cards.entry_card(signal, copy=cards.OWNER_COPY, targets=prices), signal.security_id))
    return tuple(posts)


def exit_key(event: sc.ExitEvent, seq: int) -> str:
    """Identify an event by its ordinal in the ladder step's events tuple."""
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
        raise ValueError("seq must be a non-negative integer")
    return "|".join((event.position_key, event.kind, event.leg or "-", str(event.ts_ms), str(seq)))


def plan_exit(event: sc.ExitEvent, position: sc.PositionState, *, edition: str, seq: int) -> tuple[Post, ...]:
    _edition(edition)
    key = exit_key(event, seq)
    posts = []
    if (cards.client_allowed(position.scanner, position.list_name)
            and event.kind in cards.CLIENT_EXIT_KINDS):
        posts.append(Post(GROUP, cards.CLIENT_COPY, key,
                          cards.exit_card(event, position, copy=cards.CLIENT_COPY), position.security_id, True))
    if edition == "owner":
        posts.append(Post(PERSONAL, cards.OWNER_COPY, key,
                          cards.exit_card(event, position, copy=cards.OWNER_COPY), position.security_id, True))
    return tuple(posts)


FailureHook = Callable[[Post, Delivery], Any]
EntryStatus = Callable[[Post], str | None]  # T-737: an exit post -> its trade's ENTRY card status on post.chat


def _held(post: Post, is_muted: Callable[[int], bool] | None, entry_status: EntryStatus | None) -> bool:
    """T-737 (D732 (2), amends D42 / D48 for exits): Mute blocks NEW entry cards only. An exit card goes out whenever
    its trade's entry card was SENT to that chat (a later mute never holds it); an exit of a trade whose entry card was
    MUTED on that chat stays held there, also after an unmute (the chat never saw the trade; nothing is replayed); any
    other or unknown entry status (no hook, paused, failed, no row) keeps the D42 rule: held while the contract is muted."""
    if post.security_id is None or is_muted is None:
        return False
    if not post.is_exit:
        return bool(is_muted(post.security_id))
    seen = entry_status(post) if entry_status is not None else None
    if seen == SENT:
        return False
    return seen == MUTED or bool(is_muted(post.security_id))


async def deliver(posts: Sequence[Post], *, sender: AlertSender,
                  chat_ids: Mapping[str, str | int | None], clock_ms: Callable[[], int],
                  start_ms: int, on_failure: FailureHook | None = None,
                  entry_status: EntryStatus | None = None) -> tuple[Delivery, ...]:
    """Audit JOBS_ALERTS-02: a FAILED send (and a GROUP post with no chat) logs at ERROR with its error text, and
    `on_failure(post, delivery)` (injected by the app's wiring: the router's failure alert / streak) runs once per
    FAILED post, awaited when it returns an awaitable (an async hook). Neither a failed send nor a raising hook stops the next post.
    Audit P2B-APP-02 (D42): an ENTRY post whose contract `sender.is_muted(security_id)` (the app's S089Sender: the
    router's mutes table) is recorded MUTED and never sent -- GROUP and PERSONAL, checked before the pause, as the
    legacy router's entry paths. T-737 (D732 (2), amends D42 / D48 for exits): Mute blocks NEW entry cards only; an
    exit post (`is_exit`: plan_exit's GROUP STOP / TARGET / TRAIL / ZONE_STOP / E1_EXIT and PERSONAL STOP_MOVE /
    OI_RETURN / CARRY / DAY_END) of an ALREADY-SENT open trade -- `entry_status(post)` (the hub's delivery record)
    is SENT on that chat -- always goes out; see `_held` for the rest. The pause still holds every post. Nothing is
    replayed: an unmute sends no entry this call did not plan."""
    log = logging.getLogger(__name__)
    is_muted = getattr(sender, "is_muted", None)
    deliveries = []
    for post in posts:
        chat_id = chat_ids.get(post.chat)
        sent_ms = delay_ms = error = None
        if _held(post, is_muted, entry_status):
            status = MUTED
        elif chat_id is None or chat_id == "":
            status = NO_CHAT
        else:
            try:
                await sender.send(chat_id, post.text)
            except Exception as exc:  # noqa: BLE001 - a failed delivery must not suppress the next post
                status = FAILED
                error = f"{type(exc).__name__}: {exc}"
            else:
                sent_ms = clock_ms()
                delay_ms = sent_ms - start_ms
                status = SENT
        delivery = Delivery(post.chat, post.key, status, sent_ms, delay_ms, error)
        deliveries.append(delivery)
        if status == FAILED or (status == NO_CHAT and post.chat == GROUP):
            log.error("s089 alert %s %s %s error=%s", post.chat, post.key, status, error)
        else:
            log.info("s089 alert %s %s %s delay_ms=%s", post.chat, post.key, status, delay_ms)
        if status == FAILED and on_failure is not None:
            try:
                res = on_failure(post, delivery)
                if isinstance(res, Awaitable):  # review F1: the router's _auto_post_failed is async
                    await res
            except Exception:  # the failure hook must not suppress the next post either
                log.exception("s089 alert %s %s on_failure hook raised", post.chat, post.key)
    return tuple(deliveries)


async def send_entry(signal: sc.EntrySignal, *, edition: str, sender: AlertSender,
                     chat_ids: Mapping[str, str | int | None], clock_ms: Callable[[], int],
                     own_targets: sc.TargetsConfig, group_targets: sc.TargetsConfig,
                     v2_targets: Callable[[sc.EntrySignal], Sequence[float]] | None = None,
                     on_failure: FailureHook | None = None
                     ) -> tuple[sc.EntrySignal, tuple[Delivery, ...]]:
    posts = plan_entry(signal, edition=edition, own_targets=own_targets, group_targets=group_targets,
                       v2_targets=v2_targets)
    deliveries = await deliver(posts, sender=sender, chat_ids=chat_ids, clock_ms=clock_ms,
                               start_ms=signal.signal_ms, on_failure=on_failure)
    first = next((d for d in deliveries if d.status == SENT), None)
    stamped = replace(signal, alert_sent_ms=first.sent_ms) if first is not None else signal
    return stamped, deliveries


async def send_exit(event: sc.ExitEvent, position: sc.PositionState, *, edition: str, sender: AlertSender,
                    chat_ids: Mapping[str, str | int | None], clock_ms: Callable[[], int], seq: int,
                    on_failure: FailureHook | None = None, entry_status: EntryStatus | None = None
                    ) -> tuple[sc.ExitEvent, tuple[Delivery, ...]]:
    deliveries = await deliver(plan_exit(event, position, edition=edition, seq=seq), sender=sender, chat_ids=chat_ids,
                               clock_ms=clock_ms, start_ms=event.decided_ms, on_failure=on_failure,
                               entry_status=entry_status)
    first = next((d for d in deliveries if d.status == SENT), None)
    stamped = replace(event, alert_sent_ms=first.sent_ms) if first is not None else event
    return stamped, deliveries
