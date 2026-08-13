"""Bounded subprocess execution with descendant containment.

The module is deliberately standard-library only so the deployment bootstrap can load
the exact immutable generation copy before importing the rest of :mod:`rquant`.
Contained acquisitions require inactive ``sys.settrace`` and ``sys.setprofile`` hooks.
"""

from __future__ import annotations

import ctypes
import errno
import os
import secrets
import select
import signal
import struct
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn, Protocol, TypeVar

_SIGNAL_STATE_ATTEMPTS = 3
_UNSAFE_SIGNAL_STATE_EXIT_CODE = 70
_CLEANUP_GROUP_NODE_BUDGET = max(16384, 8 * sys.getrecursionlimit())
_CLEANUP_GROUP_FRAME_BUDGET = max(4096, 4 * sys.getrecursionlimit())
_CLEANUP_GROUP_WORK_BUDGET = max(65536, 32 * sys.getrecursionlimit())
_T = TypeVar("_T")


class ContainedProcessError(RuntimeError):
    """A process tree could not be started, stopped, or proven contained."""


class _ContainedSignal(BaseException):
    def __init__(self, signum: int) -> None:
        self.signum = signum


class _ContainedSignalLatch:
    def __init__(self) -> None:
        self.first_signum: int | None = None

    def checkpoint(self) -> None:
        if self.first_signum is not None:
            raise _ContainedSignal(self.first_signum)

    def handle(self, signum: int, _frame: object) -> None:
        if self.first_signum is None:
            self.first_signum = signum


def _record_cleanup_error(
    errors: list[BaseException],
    error: BaseException,
) -> None:
    if not any(existing is error for existing in errors):
        errors.append(error)


def _format_cleanup_error_details(errors: Sequence[BaseException]) -> str:
    try:
        details: list[str] = []
        for error in errors:
            try:
                detail = str(error)
                if not detail:
                    detail = type(error).__name__
            except BaseException:
                try:
                    detail = type(error).__name__
                except BaseException:
                    detail = "cleanup error"
            try:
                details.append(detail)
            except BaseException:
                return "cleanup error"
        try:
            return "; ".join(details)
        except BaseException:
            return "cleanup error"
    except BaseException:
        return "cleanup error"


@dataclass
class _CleanupBudgetScope:
    group: BaseExceptionGroup
    output_start: int
    completion_start: int
    frame_start: int
    node_start: int
    work_start: int
    node_limit: int
    work_limit: int
    signatures: dict[tuple[object, ...], int]


@dataclass
class _CleanupGroupFrame:
    group: BaseExceptionGroup
    nested_errors: tuple[BaseException, ...]
    next_index: int
    output_start: int
    completion_start: int
    scope: _CleanupBudgetScope


def _collect_cleanup_error(
    cleanup_errors: list[BaseException],
    error: BaseException,
    *,
    primary_exception: BaseException,
    seen: set[int],
    preserve_root_evidence: bool = False,
) -> None:
    if error is primary_exception or id(error) in seen:
        return
    if not isinstance(error, BaseExceptionGroup):
        cleanup_errors.append(error)
        seen.add(id(error))
        return

    output: list[BaseException] = []
    emitted: set[int] = set()
    completed_groups: set[int] = set()
    completion_log: list[int] = []
    active_groups: dict[int, int] = {}
    node_count = 1
    work_count = 1
    descent_locked = False
    frames: list[_CleanupGroupFrame] = []

    def make_scope(
        group: BaseExceptionGroup,
        *,
        node_start: int,
        work_start: int,
        node_limit: int,
        work_limit: int,
    ) -> _CleanupBudgetScope:
        return _CleanupBudgetScope(
            group=group,
            output_start=len(output),
            completion_start=len(completion_log),
            frame_start=len(frames),
            node_start=node_start,
            work_start=work_start,
            node_limit=max(1, node_limit),
            work_limit=max(1, work_limit),
            signatures={},
        )

    def sibling_limits(
        parent_scope: _CleanupBudgetScope,
        *,
        node_start: int,
        work_start: int,
        remaining_siblings: int,
    ) -> tuple[int, int]:
        node_remaining = parent_scope.node_limit - (node_start - parent_scope.node_start)
        work_remaining = parent_scope.work_limit - (work_start - parent_scope.work_start)
        return (
            node_remaining // remaining_siblings,
            work_remaining // remaining_siblings,
        )

    def inspect_group(group: BaseExceptionGroup) -> tuple[BaseException, ...] | None:
        try:
            nested = group.exceptions
            if not isinstance(nested, tuple):
                return None
        except BaseException:
            return None
        return nested

    def rollback_output(start: int) -> None:
        while len(output) > start:
            emitted.discard(id(output.pop()))

    def rollback_completions(start: int) -> None:
        while len(completion_log) > start:
            completed_groups.discard(completion_log.pop())

    def preserve_branch(
        group: BaseExceptionGroup,
        *,
        output_start: int,
        completion_start: int,
        frame_start: int,
    ) -> None:
        for aborted in frames[frame_start:]:
            active_groups.pop(id(aborted.group), None)
        del frames[frame_start:]
        rollback_output(output_start)
        rollback_completions(completion_start)
        group_id = id(group)
        if group is not primary_exception and group_id not in seen and group_id not in emitted:
            output.append(group)
            emitted.add(group_id)

    def preserve_scope(scope: _CleanupBudgetScope) -> None:
        preserve_branch(
            scope.group,
            output_start=scope.output_start,
            completion_start=scope.completion_start,
            frame_start=scope.frame_start,
        )

    def expansion_signature(
        group: BaseExceptionGroup,
        nested: tuple[BaseException, ...],
    ) -> tuple[object, ...] | None:
        try:
            marker = group.args[0]
        except BaseException:
            return None
        if type(marker) not in {str, bytes, int}:
            return None
        return (type(group), len(nested), type(marker), marker)

    def repeated_expansion(
        scope: _CleanupBudgetScope,
        group: BaseExceptionGroup,
        nested: tuple[BaseException, ...],
    ) -> bool:
        signature = expansion_signature(group, nested)
        if signature is None:
            return False
        count = scope.signatures.get(signature, 0) + 1
        scope.signatures[signature] = count
        repeat_budget = max(2, min(64, _CLEANUP_GROUP_NODE_BUDGET // 4))
        return count > repeat_budget

    root_scope = _CleanupBudgetScope(
        group=error,
        output_start=0,
        completion_start=0,
        frame_start=0,
        node_start=0,
        work_start=0,
        node_limit=_CLEANUP_GROUP_NODE_BUDGET,
        work_limit=_CLEANUP_GROUP_WORK_BUDGET,
        signatures={},
    )
    root_nested = inspect_group(error)
    if root_nested is None:
        cleanup_errors.append(error)
        seen.add(id(error))
        return
    root_width = len(root_nested)
    if (
        1 + root_width > _CLEANUP_GROUP_NODE_BUDGET
        or 1 + 2 * root_width > _CLEANUP_GROUP_WORK_BUDGET
    ):
        cleanup_errors.append(error)
        seen.add(id(error))
        return

    frames.append(_CleanupGroupFrame(error, root_nested, 0, 0, 0, root_scope))
    active_groups[id(error)] = 0

    while frames:
        frame = frames[-1]
        if frame.next_index >= len(frame.nested_errors):
            group_id = id(frame.group)
            active_groups.pop(group_id, None)
            completed_groups.add(group_id)
            completion_log.append(group_id)
            frames.pop()
            continue

        current = frame.nested_errors[frame.next_index]
        frame.next_index += 1
        if not isinstance(current, BaseException):
            frame_index = len(frames) - 1
            preserve_branch(
                frame.group,
                output_start=frame.output_start,
                completion_start=frame.completion_start,
                frame_start=frame_index,
            )
            continue

        node_count += 1
        work_count += 2
        scope = frame.scope
        if isinstance(current, BaseExceptionGroup) and (
            (frame is frames[0] and preserve_root_evidence) or len(frame.nested_errors) > 1
        ):
            remaining_siblings = len(frame.nested_errors) - frame.next_index + 1
            node_limit, work_limit = sibling_limits(
                frame.scope,
                node_start=node_count - 1,
                work_start=work_count - 2,
                remaining_siblings=remaining_siblings,
            )
            scope = make_scope(
                current,
                node_start=node_count - 1,
                work_start=work_count - 2,
                node_limit=node_limit,
                work_limit=work_limit,
            )

        current_id = id(current)
        if current is primary_exception or current_id in seen:
            continue
        cycle_start = active_groups.get(current_id)
        if cycle_start is not None:
            cycle_frame = frames[cycle_start]
            preserve_branch(
                cycle_frame.group,
                output_start=cycle_frame.output_start,
                completion_start=cycle_frame.completion_start,
                frame_start=cycle_start,
            )
            continue
        if current_id in emitted or current_id in completed_groups:
            continue

        global_exhausted = (
            node_count > _CLEANUP_GROUP_NODE_BUDGET or work_count > _CLEANUP_GROUP_WORK_BUDGET
        )
        local_exhausted = (
            node_count - scope.node_start > scope.node_limit
            or work_count - scope.work_start > scope.work_limit
        )
        if global_exhausted or local_exhausted:
            if scope is root_scope and preserve_root_evidence:
                descent_locked = True
            else:
                preserve_scope(scope)
                if global_exhausted:
                    descent_locked = True
                continue

        if not isinstance(current, BaseExceptionGroup):
            output.append(current)
            emitted.add(current_id)
            continue
        if descent_locked:
            output.append(current)
            emitted.add(current_id)
            continue

        work_count += 1
        global_exhausted = work_count > _CLEANUP_GROUP_WORK_BUDGET
        local_exhausted = work_count - scope.work_start > scope.work_limit
        if global_exhausted or local_exhausted:
            if scope is root_scope and preserve_root_evidence:
                descent_locked = True
                output.append(current)
                emitted.add(current_id)
            else:
                preserve_scope(scope)
                if global_exhausted:
                    descent_locked = True
            continue

        nested_errors = inspect_group(current)
        if nested_errors is None:
            output.append(current)
            emitted.add(current_id)
            continue
        if scope is frame.scope and len(nested_errors) > 1:
            remaining_siblings = len(frame.nested_errors) - frame.next_index + 1
            node_limit, work_limit = sibling_limits(
                frame.scope,
                node_start=node_count - 1,
                work_start=work_count - 3,
                remaining_siblings=remaining_siblings,
            )
            scope = make_scope(
                current,
                node_start=node_count - 1,
                work_start=work_count - 3,
                node_limit=node_limit,
                work_limit=work_limit,
            )
        if repeated_expansion(scope, current, nested_errors):
            preserve_scope(scope)
            continue
        if len(frames) - scope.frame_start >= _CLEANUP_GROUP_FRAME_BUDGET:
            preserve_scope(scope)
            continue
        active_groups[current_id] = len(frames)
        frames.append(
            _CleanupGroupFrame(
                current,
                nested_errors,
                0,
                len(output),
                len(completion_log),
                scope,
            )
        )

    cleanup_errors.extend(output)
    seen.update(emitted)
    seen.update(completed_groups)


def _merge_cleanup_error_group(
    primary_exception: BaseException,
    errors: Sequence[BaseException],
    *,
    error_label: str,
) -> bool:
    cleanup_errors: list[BaseException] = []
    seen: set[int] = set()
    try:
        existing_group = getattr(primary_exception, "cleanup_error_group", None)
    except BaseException:
        existing_group = None
    if isinstance(existing_group, BaseException):
        _collect_cleanup_error(
            cleanup_errors,
            existing_group,
            primary_exception=primary_exception,
            seen=seen,
            preserve_root_evidence=True,
        )
    for error in errors:
        _collect_cleanup_error(
            cleanup_errors,
            error,
            primary_exception=primary_exception,
            seen=seen,
        )
    if not cleanup_errors:
        return False
    with suppress(BaseException):
        primary_exception.cleanup_error_group = BaseExceptionGroup(  # type: ignore[attr-defined]
            error_label,
            cleanup_errors,
        )
    return True


def _attach_cleanup_error_group(
    primary_exception: BaseException,
    errors: Sequence[BaseException],
    *,
    error_label: str,
    note: str,
) -> None:
    try:
        has_cleanup_errors = _merge_cleanup_error_group(
            primary_exception,
            errors,
            error_label=error_label,
        )
    except BaseException:
        has_cleanup_errors = True
    if not has_cleanup_errors:
        return
    try:
        notes = getattr(primary_exception, "__notes__", ())
        if note not in notes:
            primary_exception.add_note(note)
    except BaseException:
        pass


def _require_no_execution_hooks() -> None:
    if sys.gettrace() is not None or sys.getprofile() is not None:
        raise ContainedProcessError("contained acquisition does not support active execution hooks")


def _call_with_execution_hook_guard(
    operation: Callable[..., _T],
    /,
    *args: object,
    **kwargs: object,
) -> _T:
    _require_no_execution_hooks()
    result = operation(*args, **kwargs)
    _require_no_execution_hooks()
    return result


def _terminate_unsafe_signal_state(
    message: str,
    errors: Sequence[BaseException],
) -> NoReturn:
    try:
        details = _format_cleanup_error_details(errors)
        diagnostic = f"rquant: {message}"
        if details:
            diagnostic = f"{diagnostic}: {details}"
        os.write(2, f"{diagnostic}\n".encode())
    except BaseException:
        pass
    finally:
        os._exit(_UNSAFE_SIGNAL_STATE_EXIT_CODE)


def _ensure_signal_mask_bounded(
    expected_mask: set[signal.Signals],
    errors: list[BaseException],
) -> bool:
    mismatch: ContainedProcessError | None = None
    for _attempt in range(_SIGNAL_STATE_ATTEMPTS):
        try:
            if signal.pthread_sigmask(signal.SIG_BLOCK, set()) == expected_mask:
                return True
        except BaseException as exc:
            _record_cleanup_error(errors, exc)
        try:
            signal.pthread_sigmask(signal.SIG_SETMASK, expected_mask)
        except BaseException as exc:
            _record_cleanup_error(errors, exc)
        try:
            if signal.pthread_sigmask(signal.SIG_BLOCK, set()) == expected_mask:
                return True
        except BaseException as exc:
            _record_cleanup_error(errors, exc)
        if mismatch is None:
            mismatch = ContainedProcessError("signal mask restoration could not be verified")
            _record_cleanup_error(errors, mismatch)
    return False


def _restore_signal_mask_or_terminate(
    blocked_mask: set[signal.Signals],
    errors: list[BaseException],
    *,
    context: str,
) -> None:
    if _ensure_signal_mask_bounded(blocked_mask, errors):
        return
    _terminate_unsafe_signal_state(context, errors)


def _signal_mask_matches_once(
    expected_mask: set[signal.Signals],
    errors: list[BaseException],
) -> bool:
    try:
        return signal.pthread_sigmask(signal.SIG_BLOCK, set()) == expected_mask
    except BaseException as exc:
        _record_cleanup_error(errors, exc)
        return False


def _release_signal_mask_once(
    target_mask: set[signal.Signals],
    blocked_mask: set[signal.Signals],
    errors: list[BaseException],
) -> tuple[BaseException | None, bool]:
    try:
        signal.pthread_sigmask(signal.SIG_SETMASK, target_mask)
    except BaseException as exc:
        _record_cleanup_error(errors, exc)
        reached_target = _signal_mask_matches_once(target_mask, errors)
        _restore_signal_mask_or_terminate(
            blocked_mask,
            errors,
            context="signal release failed and the blocked mask could not be restored",
        )
        return exc, reached_target
    try:
        observed_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    except BaseException as exc:
        _record_cleanup_error(errors, exc)
        reached_target = _signal_mask_matches_once(target_mask, errors)
        _restore_signal_mask_or_terminate(
            blocked_mask,
            errors,
            context="signal release verification failed and blocking could not be restored",
        )
        return exc, reached_target
    if observed_mask == target_mask:
        return None, True
    error = ContainedProcessError("signal mask release could not be verified")
    _record_cleanup_error(errors, error)
    _restore_signal_mask_or_terminate(
        blocked_mask,
        errors,
        context="signal release mismatch could not be returned to a blocked state",
    )
    return error, False


def _release_signal_mask_bounded(
    target_mask: set[signal.Signals],
    blocked_mask: set[signal.Signals],
    errors: list[BaseException],
) -> bool:
    for _attempt in range(_SIGNAL_STATE_ATTEMPTS):
        release_error, _reached_target = _release_signal_mask_once(
            target_mask,
            blocked_mask,
            errors,
        )
        if release_error is None:
            return True
    return False


def _returning_signal_replay_error(signum: int) -> InterruptedError:
    return InterruptedError(f"process runner interrupted by signal {signum}")


def _is_nested_signal_latch_handler(handler: object) -> bool:
    return (
        isinstance(getattr(handler, "__self__", None), _ContainedSignalLatch)
        and getattr(handler, "__func__", None) is _ContainedSignalLatch.handle
    )


@dataclass
class _SignalHandlerInvocation:
    signum: int
    replay_error: BaseException | None = None
    authority_transferred: bool = False


class _SignalHandlerInvocationTracker:
    def __init__(
        self,
        previous_handlers: Mapping[int, object],
    ) -> None:
        self._previous_handlers = previous_handlers
        self._invocations: list[_SignalHandlerInvocation] = []
        trampoline = self._invoke
        self.handlers = {
            signum: trampoline for signum, handler in previous_handlers.items() if callable(handler)
        }

    def _invoke(self, signum: int, frame: object) -> None:
        handler = self._previous_handlers[signum]
        assert callable(handler)
        authority_transferred = _is_nested_signal_latch_handler(handler) and not self._invocations
        invocation = _SignalHandlerInvocation(
            signum,
            replay_error=(
                None if authority_transferred else _returning_signal_replay_error(signum)
            ),
            authority_transferred=authority_transferred,
        )
        self._invocations.append(invocation)
        try:
            handler(signum, frame)
        except BaseException as exc:
            invocation.replay_error = exc
            invocation.authority_transferred = False
            raise
        if invocation.authority_transferred:
            return
        assert invocation.replay_error is not None
        raise invocation.replay_error

    def raised(self, error: BaseException) -> bool:
        return any(invocation.replay_error is error for invocation in self._invocations)

    @property
    def replay_errors(self) -> tuple[BaseException, ...]:
        return tuple(
            invocation.replay_error
            for invocation in self._invocations
            if invocation.replay_error is not None
        )

    @property
    def authority_transferred(self) -> bool:
        return bool(self._invocations and self._invocations[0].authority_transferred)


def _consume_signal_handler_outcomes(
    tracker: _SignalHandlerInvocationTracker,
    protected_replay_error: BaseException | None,
    cleanup_errors: list[BaseException],
) -> BaseException | None:
    replay_errors = tracker.replay_errors
    if protected_replay_error is None and replay_errors and not tracker.authority_transferred:
        protected_replay_error = replay_errors[0]
    for replay_error in replay_errors:
        if replay_error is not protected_replay_error:
            _record_cleanup_error(cleanup_errors, replay_error)
    return protected_replay_error


def _restore_signal_handlers_collecting_errors(
    previous_handlers: Mapping[int, object],
    cleanup_errors: list[BaseException],
) -> bool:
    restoration_errors: list[BaseException] = []
    restored = _restore_signal_handlers_verified(
        previous_handlers,
        restoration_errors,
    )
    for error in restoration_errors:
        _record_cleanup_error(cleanup_errors, error)
    return restored


class _SignalRestoration(list[BaseException]):
    def __init__(
        self,
        errors: Sequence[BaseException],
        *,
        previous_mask: set[signal.Signals] | None,
        blocked_mask: set[signal.Signals] | None,
        handlers_restored: bool = True,
    ) -> None:
        super().__init__(errors)
        self._previous_mask = previous_mask
        self._blocked_mask = blocked_mask
        self._handlers_restored = handlers_restored
        self._released = False
        self._last_release_transition_exception: BaseException | None = None

    def _fail_closed(self, errors: list[BaseException], *, context: str) -> None:
        if self._blocked_mask is None:
            _terminate_unsafe_signal_state(context, errors)
        _restore_signal_mask_or_terminate(
            self._blocked_mask,
            errors,
            context=context,
        )
        self._released = False

    def release(self) -> None:
        self._last_release_transition_exception = None
        if self._released:
            return
        if not self._handlers_restored:
            raise ContainedProcessError(
                "signal mask release refused because latch handlers remain installed"
            )
        if self._previous_mask is None:
            self._released = True
            return
        if self._blocked_mask is None:
            _terminate_unsafe_signal_state(
                "signal release has no verified blocked-mask checkpoint",
                self,
            )
        release_errors: list[BaseException] = []
        release_error, reached_target = _release_signal_mask_once(
            self._previous_mask,
            self._blocked_mask,
            release_errors,
        )
        if release_error is not None:
            if reached_target:
                self._last_release_transition_exception = release_error
            _attach_cleanup_error_group(
                release_error,
                release_errors,
                error_label="signal mask release recovery failures",
                note="signal mask release recovery also failed",
            )
            raise release_error
        self._released = True

    def release_and_replay(
        self,
        latch: _ContainedSignalLatch,
        previous_handlers: Mapping[int, object],
        cleanup_errors: list[BaseException],
        *,
        error_label: str,
        primary_exception: BaseException | None = None,
    ) -> None:
        protected_replay_error = (
            _latched_signal_replay_error(latch.first_signum, previous_handlers)
            if latch.first_signum is not None
            else None
        )
        invocation_tracker: _SignalHandlerInvocationTracker | None = None
        release_attempts = _SIGNAL_STATE_ATTEMPTS
        candidate_tracker = _SignalHandlerInvocationTracker(previous_handlers)
        if candidate_tracker.handlers and self._handlers_restored:
            self._fail_closed(
                cleanup_errors,
                context="signal handler tracking could not establish a blocked state",
            )
            if _restore_signal_handlers_collecting_errors(
                candidate_tracker.handlers,
                cleanup_errors,
            ):
                invocation_tracker = candidate_tracker
            else:
                self._handlers_restored = _restore_signal_handlers_collecting_errors(
                    previous_handlers,
                    cleanup_errors,
                )
                if protected_replay_error is not None:
                    release_attempts = 0
                else:
                    return
        for _attempt in range(release_attempts):
            try:
                if not self._released:
                    self.release()
                if invocation_tracker is not None:
                    # Remove forwarding trampolines only after the verified unmask so
                    # handler exceptions remain attributable through the handoff.
                    self._handlers_restored = _restore_signal_handlers_collecting_errors(
                        previous_handlers,
                        cleanup_errors,
                    )
                    if not self._handlers_restored:
                        self._fail_closed(
                            cleanup_errors,
                            context=("signal handler handoff could not return to a blocked state"),
                        )
                        self._handlers_restored = _restore_signal_handlers_collecting_errors(
                            previous_handlers,
                            cleanup_errors,
                        )
                    protected_replay_error = _consume_signal_handler_outcomes(
                        invocation_tracker,
                        protected_replay_error,
                        cleanup_errors,
                    )
                    invocation_tracker = None
                    if not self._handlers_restored:
                        break
                if protected_replay_error is None:
                    return
                _attach_cleanup_error_group(
                    protected_replay_error,
                    cleanup_errors,
                    error_label=error_label,
                    note="contained subprocess cleanup also failed",
                )
                raise protected_replay_error
            except BaseException as exc:
                if exc is protected_replay_error and self._released:
                    raise
                if protected_replay_error is None:
                    boundary_signal = (
                        invocation_tracker is not None
                        and invocation_tracker.raised(exc)
                        and (self._released or exc is self._last_release_transition_exception)
                    )
                    if boundary_signal:
                        assert invocation_tracker is not None
                        protected_replay_error = _consume_signal_handler_outcomes(
                            invocation_tracker,
                            protected_replay_error,
                            cleanup_errors,
                        )
                    elif primary_exception is None:
                        protected_replay_error = exc
                    else:
                        _record_cleanup_error(cleanup_errors, exc)
                elif exc is not protected_replay_error:
                    _record_cleanup_error(cleanup_errors, exc)

        if invocation_tracker is not None:
            self._fail_closed(
                cleanup_errors,
                context="signal handler tracking could not finish in a blocked state",
            )
            self._handlers_restored = _restore_signal_handlers_collecting_errors(
                previous_handlers,
                cleanup_errors,
            )
            protected_replay_error = _consume_signal_handler_outcomes(
                invocation_tracker,
                protected_replay_error,
                cleanup_errors,
            )

        if protected_replay_error is None:
            return
        self._fail_closed(
            cleanup_errors,
            context="terminal signal replay could not establish a blocked state",
        )
        try:
            _attach_cleanup_error_group(
                protected_replay_error,
                cleanup_errors,
                error_label=error_label,
                note="contained subprocess cleanup also failed",
            )
        except BaseException as exc:
            if exc is not protected_replay_error:
                _record_cleanup_error(cleanup_errors, exc)
        raise protected_replay_error


def _signal_handlers_match(observed: object, expected: object) -> bool:
    return observed is expected or observed == expected


def _set_signal_handler_verified(
    signum: int,
    handler: object,
    errors: list[BaseException],
) -> bool:
    for _attempt in range(_SIGNAL_STATE_ATTEMPTS):
        transition_failed = False
        try:
            signal.signal(signum, handler)
        except BaseException as exc:
            transition_failed = True
            _record_cleanup_error(errors, exc)
        try:
            observed = signal.getsignal(signum)
            if not _signal_handlers_match(observed, handler):
                if not transition_failed:
                    _record_cleanup_error(
                        errors,
                        ContainedProcessError(
                            f"signal handler restoration could not be verified for {signum}"
                        ),
                    )
                continue
        except BaseException as exc:
            _record_cleanup_error(errors, exc)
            continue
        return True
    return False


def _read_signal_mask_bounded(
    errors: list[BaseException],
) -> set[signal.Signals] | None:
    for _attempt in range(_SIGNAL_STATE_ATTEMPTS):
        try:
            return signal.pthread_sigmask(signal.SIG_BLOCK, set())
        except BaseException as exc:
            _record_cleanup_error(errors, exc)
    return None


def _set_signal_mask_bounded(
    how: int,
    mask: set[signal.Signals] | frozenset[int],
    errors: list[BaseException],
    *,
    initial_mask: set[signal.Signals] | None = None,
) -> set[signal.Signals] | None:
    previous_mask = initial_mask
    if previous_mask is None:
        previous_mask = _read_signal_mask_bounded(errors)
    if previous_mask is None:
        return None
    expected_mask = {*previous_mask, *mask} if how == signal.SIG_BLOCK else set(mask)
    for _attempt in range(_SIGNAL_STATE_ATTEMPTS):
        transition_failed = False
        try:
            signal.pthread_sigmask(how, mask)
        except BaseException as exc:
            transition_failed = True
            _record_cleanup_error(errors, exc)
        try:
            observed_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        except BaseException as exc:
            _record_cleanup_error(errors, exc)
            continue
        if observed_mask == expected_mask:
            return previous_mask
        if not transition_failed:
            _record_cleanup_error(
                errors,
                ContainedProcessError("signal mask transition could not be verified"),
            )
    return None


def _restore_signal_handlers_verified(
    previous_handlers: Mapping[int, object],
    errors: list[BaseException],
) -> bool:
    restored = True
    for signum, previous in previous_handlers.items():
        if not _set_signal_handler_verified(signum, previous, errors):
            restored = False
    return restored


def _install_signal_latch(
    latch: _ContainedSignalLatch,
) -> tuple[dict[int, object], frozenset[int]]:
    candidates = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    active = frozenset(
        signum for signum, previous in candidates.items() if previous is not signal.SIG_IGN
    )
    if not active:
        return {}, frozenset()
    if not hasattr(signal, "pthread_sigmask"):
        raise ContainedProcessError("atomic signal arbitration is unavailable")

    mask_errors: list[BaseException] = []
    previous_mask = _set_signal_mask_bounded(signal.SIG_BLOCK, active, mask_errors)
    if previous_mask is None:
        primary_exception = mask_errors[0]
        if len(mask_errors) > 1:
            _attach_cleanup_error_group(
                primary_exception,
                mask_errors[1:],
                error_label="signal latch installation mask failures",
                note="signal latch installation mask retries also failed",
            )
        raise primary_exception

    installed: dict[int, object] = {}
    touched: dict[int, object] = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            if signum not in active:
                continue
            touched[signum] = candidates[signum]
            signal.signal(signum, latch.handle)
            if not _signal_handlers_match(signal.getsignal(signum), latch.handle):
                raise ContainedProcessError(
                    f"signal latch installation could not be verified for {signum}"
                )
            installed[signum] = candidates[signum]
    except BaseException as primary_exception:
        rollback_errors: list[BaseException] = []
        rollback_complete = _restore_signal_handlers_verified(touched, rollback_errors)
        if rollback_complete:
            release_errors: list[BaseException] = []
            _release_signal_mask_bounded(
                previous_mask,
                {*previous_mask, *active},
                release_errors,
            )
            for error in release_errors:
                _record_cleanup_error(rollback_errors, error)
        _attach_cleanup_error_group(
            primary_exception,
            rollback_errors,
            error_label="signal latch installation rollback failures",
            note="signal latch installation rollback also failed",
        )
        raise

    blocked_mask = {*previous_mask, *active}
    release_errors: list[BaseException] = []
    if not _release_signal_mask_bounded(previous_mask, blocked_mask, release_errors):
        primary_exception = release_errors[0]
        rollback_errors: list[BaseException] = []
        for error in release_errors[1:]:
            _record_cleanup_error(rollback_errors, error)
        handlers_restored = _restore_signal_handlers_verified(
            installed,
            rollback_errors,
        )
        if handlers_restored:
            recovery_cleanup = [primary_exception, *rollback_errors]
            recovery = _SignalRestoration(
                (),
                previous_mask=previous_mask,
                blocked_mask=blocked_mask,
            )
            recovery.release_and_replay(
                latch,
                candidates,
                recovery_cleanup,
                error_label="signal latch installation release failures",
                primary_exception=primary_exception,
            )
            rollback_errors = []
            for error in recovery_cleanup:
                if error is not primary_exception:
                    _record_cleanup_error(rollback_errors, error)
        _attach_cleanup_error_group(
            primary_exception,
            rollback_errors,
            error_label="signal latch installation release failures",
            note="signal latch installation release also failed",
        )
        raise primary_exception
    return installed, active


def _restore_signal_handlers_atomically(
    previous_handlers: Mapping[int, object],
    active_signals: frozenset[int],
    latch: _ContainedSignalLatch,
) -> _SignalRestoration:
    if not previous_handlers:
        return _SignalRestoration((), previous_mask=None, blocked_mask=None)

    errors: list[BaseException] = []
    previous_mask = _read_signal_mask_bounded(errors)
    if previous_mask is None:
        fallback_snapshot: set[signal.Signals] | None = None
        snapshot_trustworthy = True
        verified_blocked_mask: set[signal.Signals] | None = None
        for _attempt in range(_SIGNAL_STATE_ATTEMPTS):
            try:
                transition_mask = signal.pthread_sigmask(
                    signal.SIG_BLOCK,
                    active_signals,
                )
                if fallback_snapshot is None and snapshot_trustworthy:
                    fallback_snapshot = transition_mask
            except BaseException as exc:
                snapshot_trustworthy = False
                _record_cleanup_error(errors, exc)
            try:
                observed_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
            except BaseException as exc:
                _record_cleanup_error(errors, exc)
                continue
            if set(active_signals) <= observed_mask:
                verified_blocked_mask = observed_mask
            if fallback_snapshot is not None and observed_mask == {
                *fallback_snapshot,
                *active_signals,
            }:
                previous_mask = fallback_snapshot
                break
        if previous_mask is None:
            if verified_blocked_mask is not None:
                return _SignalRestoration(
                    errors,
                    previous_mask=None,
                    blocked_mask=verified_blocked_mask,
                    handlers_restored=False,
                )
            _terminate_unsafe_signal_state(
                "signal mask snapshot failed and blocking could not be verified",
                errors,
            )
    blocked_mask = {*previous_mask, *active_signals}
    if _set_signal_mask_bounded(
        signal.SIG_BLOCK,
        active_signals,
        errors,
        initial_mask=previous_mask,
    ) is None and not _ensure_signal_mask_bounded(blocked_mask, errors):
        _terminate_unsafe_signal_state(
            "signal handlers cannot be restored because blocking is unverified",
            errors,
        )

    handlers_restored = _restore_signal_handlers_verified(previous_handlers, errors)

    if not handlers_restored:
        return _SignalRestoration(
            errors,
            previous_mask=previous_mask,
            blocked_mask=blocked_mask,
            handlers_restored=False,
        )

    # Signals delivered while the handlers were being restored are pending because
    # the whole set is blocked. Setting a pending signal to SIG_IGN discards that
    # kernel delivery; the prior handler is restored before leaving this helper.
    for _attempt in range(8):
        try:
            pending = signal.sigpending()
        except BaseException as exc:
            errors.append(exc)
            break
        drainable = [
            signum
            for signum in (signal.SIGINT, signal.SIGTERM)
            if signum in active_signals and signum not in previous_mask and signum in pending
        ]
        if not drainable:
            break
        for signum in drainable:
            latch.handle(signum, None)
            previous = previous_handlers[signum]
            ignored = _set_signal_handler_verified(signum, signal.SIG_IGN, errors)
            restored = _set_signal_handler_verified(signum, previous, errors)
            handlers_restored = ignored and restored and handlers_restored
    else:
        errors.append(ContainedProcessError("signal arbitration did not quiesce"))
    for signum, previous in previous_handlers.items():
        try:
            observed = signal.getsignal(signum)
            if not _signal_handlers_match(observed, previous):
                handlers_restored = False
                errors.append(
                    ContainedProcessError(
                        f"signal handler restoration could not be verified for {signum}"
                    )
                )
        except BaseException as exc:
            handlers_restored = False
            errors.append(exc)
    return _SignalRestoration(
        errors,
        previous_mask=previous_mask,
        blocked_mask=blocked_mask,
        handlers_restored=handlers_restored,
    )


def _latched_signal_replay_error(
    signum: int,
    previous_handlers: Mapping[int, object],
) -> BaseException | None:
    previous = previous_handlers[signum]
    if callable(previous):
        try:
            previous(signum, None)
        except BaseException as exc:
            return exc
        if _is_nested_signal_latch_handler(previous):
            return None
        return _returning_signal_replay_error(signum)
    if previous is signal.SIG_IGN:
        return None
    try:
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)
    except BaseException as exc:
        return exc
    return SystemExit(128 + signum)


def _finish_signal_restoration(
    previous_handlers: Mapping[int, object],
    active_signals: frozenset[int],
    latch: _ContainedSignalLatch,
    cleanup_errors: list[BaseException],
    *,
    primary_exception: BaseException | None,
    error_label: str,
    replay_ready: bool = True,
) -> None:
    restoration = _restore_signal_handlers_atomically(
        previous_handlers,
        active_signals,
        latch,
    )
    cleanup_errors.extend(restoration)
    if not replay_ready:
        cleanup_group = BaseExceptionGroup(error_label, cleanup_errors)
        if latch.first_signum is not None:
            deferred_signal = _ContainedSignal(latch.first_signum)
            _attach_cleanup_error_group(
                deferred_signal,
                cleanup_errors,
                error_label=error_label,
                note="contained subprocess signal replay deferred until anchors are closed",
            )
            raise deferred_signal
        if primary_exception is not None:
            _attach_cleanup_error_group(
                primary_exception,
                cleanup_errors,
                error_label=error_label,
                note="contained subprocess cleanup failed closed with managed signals blocked",
            )
            return
        raise ContainedProcessError(
            "contained subprocess cleanup failed closed because anchor closure is unproven"
        ) from cleanup_group
    restoration.release_and_replay(
        latch,
        previous_handlers,
        cleanup_errors,
        error_label=error_label,
        primary_exception=primary_exception,
    )

    if not cleanup_errors:
        return
    details = _format_cleanup_error_details(cleanup_errors)
    if primary_exception is not None:
        _attach_cleanup_error_group(
            primary_exception,
            cleanup_errors,
            error_label=error_label,
            note=f"contained subprocess cleanup also failed: {details}",
        )
        return
    cleanup_group = BaseExceptionGroup(error_label, cleanup_errors)
    raise ContainedProcessError(
        f"contained subprocess cleanup failed: {details}"
    ) from cleanup_group


@dataclass(frozen=True, order=True)
class ProcessIdentity:
    pid: int
    started: tuple[int, int]
    kernel_unique_id: int = 0


@dataclass(frozen=True)
class _ProcessObservation:
    identity: ProcessIdentity
    parent_pid: int
    containment_token: bool = False
    parent_kernel_unique_id: int = 0


Clock = Callable[[], float]
Sleep = Callable[[float], None]
Inventory = Callable[[float], dict[int, _ProcessObservation]]

_CONTAINMENT_ENVIRONMENT_KEY = "RQUANT_CONTAINMENT_TOKEN"
_MAX_PROCESS_ARGUMENT_BYTES = 4 * 1024 * 1024
_LINUX_SUBREAPER_LOCK = threading.Lock()
_LINUX_SUBREAPER_OWNERSHIP = threading.local()
_DARWIN_UNIQUE_IDENTITY_FLAVOR = 17
_DARWIN_UNIQUE_IDENTITY_SIZE = 56
_DARWIN_LIST_FDS_FLAVOR = 1
_DARWIN_PIPE_FD_TYPE = 6
_DARWIN_PIPE_INFO_FLAVOR = 6
_DARWIN_PIPE_INFO_SIZE = 184
_DARWIN_PIPE_HANDLE_OFFSET = 160
_DARWIN_FD_ENTRY_SIZE = 8
_MAX_DARWIN_FD_LIST_BYTES = 4 * 1024 * 1024
DarwinPipeMarker = tuple[int, int]


def _file_descriptor_is_closed(
    descriptor: int,
    cleanup_errors: list[BaseException],
) -> bool:
    try:
        os.fstat(descriptor)
    except OSError as exc:
        if exc.errno == errno.EBADF:
            return True
        _record_cleanup_error(cleanup_errors, exc)
    except BaseException as exc:
        _record_cleanup_error(cleanup_errors, exc)
    return False


def _close_file_descriptors(
    descriptors: list[int],
    cleanup_errors: list[BaseException],
) -> bool:
    for descriptor in reversed(tuple(descriptors)):
        closed = False
        for _attempt in range(_SIGNAL_STATE_ATTEMPTS):
            try:
                os.close(descriptor)
            except OSError as exc:
                if exc.errno == errno.EBADF:
                    closed = True
                    break
                _record_cleanup_error(cleanup_errors, exc)
                if _file_descriptor_is_closed(descriptor, cleanup_errors):
                    closed = True
                    break
            except BaseException as exc:
                _record_cleanup_error(cleanup_errors, exc)
                if _file_descriptor_is_closed(descriptor, cleanup_errors):
                    closed = True
                    break
            else:
                closed = True
                break
        if closed:
            descriptors.remove(descriptor)
    return not descriptors


def _close_file_descriptor_inventories(
    inventories: Sequence[list[int]],
    cleanup_errors: list[BaseException],
) -> bool:
    descriptors: list[int] = []
    for inventory in inventories:
        for descriptor in inventory:
            if descriptor not in descriptors:
                descriptors.append(descriptor)
    closed = _close_file_descriptors(descriptors, cleanup_errors)
    unresolved = set(descriptors)
    for inventory in inventories:
        inventory[:] = [descriptor for descriptor in inventory if descriptor in unresolved]
    return closed


def _close_resource_bounded(
    resource: object,
    cleanup_errors: list[BaseException],
) -> bool:
    descriptor: int | None = None
    try:
        descriptor = resource.fileno()  # type: ignore[attr-defined]
    except (AttributeError, ValueError):
        pass
    except OSError as exc:
        if exc.errno == errno.EBADF:
            return True
        _record_cleanup_error(cleanup_errors, exc)
    except BaseException as exc:
        _record_cleanup_error(cleanup_errors, exc)
    for _attempt in range(_SIGNAL_STATE_ATTEMPTS):
        try:
            resource.close()  # type: ignore[attr-defined]
        except BaseException as exc:
            _record_cleanup_error(cleanup_errors, exc)
        else:
            if descriptor is None:
                return True
        if descriptor is not None and _file_descriptor_is_closed(descriptor, cleanup_errors):
            return True
    return False


def _raise_tracker_cleanup_error(
    message: str,
    cleanup_errors: Sequence[BaseException],
) -> NoReturn:
    details = _format_cleanup_error_details(cleanup_errors)
    primary = ContainedProcessError(f"{message}: {details}")
    _attach_cleanup_error_group(
        primary,
        cleanup_errors,
        error_label="kernel process tracker cleanup failures",
        note="kernel process tracker cleanup also failed",
    )
    raise primary


def _cleanup_reserve_seconds(remaining: float) -> float:
    fraction = 0.75 if remaining <= 0.25 else 0.5
    return min(1.0, max(0.1, remaining * fraction))


class _KernelProcessTracker(Protocol):
    def register_root(self, pid: int, *, deadline: float) -> ProcessIdentity: ...

    def poll(self, *, deadline: float) -> dict[int, ProcessIdentity]: ...

    def close(self) -> None: ...


KernelTrackerFactory = Callable[[], _KernelProcessTracker]


def _darwin_unique_process_identity(
    libproc: ctypes.CDLL,
    pid: int,
) -> tuple[int, int] | None:
    buffer = ctypes.create_string_buffer(_DARWIN_UNIQUE_IDENTITY_SIZE)
    size = libproc.proc_pidinfo(
        pid,
        _DARWIN_UNIQUE_IDENTITY_FLAVOR,
        0,
        buffer,
        len(buffer),
    )
    if size != _DARWIN_UNIQUE_IDENTITY_SIZE:
        return None
    unique_id, parent_unique_id = struct.unpack_from("=QQ", buffer.raw, 16)
    if unique_id <= 0:
        return None
    return unique_id, parent_unique_id


def _darwin_process_observation(pid: int) -> _ProcessObservation | None:
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    libproc.proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    libproc.proc_pidinfo.restype = ctypes.c_int
    buffer = ctypes.create_string_buffer(256)
    size = libproc.proc_pidinfo(pid, 3, 0, buffer, len(buffer))
    if size < 136:
        return None
    _flags, _status, _xstatus, observed_pid, parent, effective_uid = struct.unpack_from(
        "=IIIIII", buffer.raw
    )
    start_seconds, start_microseconds = struct.unpack_from("=QQ", buffer.raw, 120)
    if observed_pid != pid or start_seconds <= 0 or effective_uid != os.getuid():
        return None
    unique_identity = _darwin_unique_process_identity(libproc, pid)
    if unique_identity is None:
        return None
    unique_id, parent_unique_id = unique_identity
    return _ProcessObservation(
        identity=ProcessIdentity(
            pid,
            (start_seconds, start_microseconds),
            kernel_unique_id=unique_id,
        ),
        parent_pid=parent,
        parent_kernel_unique_id=parent_unique_id,
    )


def _darwin_libproc() -> ctypes.CDLL:
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    libproc.proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    libproc.proc_pidinfo.restype = ctypes.c_int
    libproc.proc_pidfdinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    libproc.proc_pidfdinfo.restype = ctypes.c_int
    return libproc


def _darwin_pipe_marker_for_fd(pid: int, fd: int) -> DarwinPipeMarker:
    libproc = _darwin_libproc()
    buffer = ctypes.create_string_buffer(_DARWIN_PIPE_INFO_SIZE)
    size = libproc.proc_pidfdinfo(
        pid,
        fd,
        _DARWIN_PIPE_INFO_FLAVOR,
        buffer,
        len(buffer),
    )
    if size != _DARWIN_PIPE_INFO_SIZE:
        raise ContainedProcessError("Darwin containment pipe identity is unavailable")
    handle, peer_handle = struct.unpack_from(
        "=QQ",
        buffer.raw,
        _DARWIN_PIPE_HANDLE_OFFSET,
    )
    if handle <= 0 or peer_handle <= 0 or handle == peer_handle:
        raise ContainedProcessError("Darwin containment pipe identity is invalid")
    return tuple(sorted((handle, peer_handle)))


def _darwin_process_has_pipe_marker(
    pid: int,
    markers: frozenset[DarwinPipeMarker],
    *,
    deadline: float,
) -> bool:
    if not markers:
        return False
    if time.monotonic() >= deadline:
        raise TimeoutError("process pipe inventory timed out")
    libproc = _darwin_libproc()
    required = libproc.proc_pidinfo(pid, _DARWIN_LIST_FDS_FLAVOR, 0, None, 0)
    if required <= 0:
        return False
    if required > _MAX_DARWIN_FD_LIST_BYTES:
        raise ContainedProcessError("process file descriptor inventory exceeds budget")
    buffer = ctypes.create_string_buffer(required)
    size = libproc.proc_pidinfo(
        pid,
        _DARWIN_LIST_FDS_FLAVOR,
        0,
        buffer,
        len(buffer),
    )
    if size <= 0:
        return False
    if size % _DARWIN_FD_ENTRY_SIZE != 0:
        raise ContainedProcessError("process file descriptor inventory is malformed")
    pipe_buffer = ctypes.create_string_buffer(_DARWIN_PIPE_INFO_SIZE)
    for offset in range(0, size, _DARWIN_FD_ENTRY_SIZE):
        if time.monotonic() >= deadline:
            raise TimeoutError("process pipe inventory timed out")
        fd, fd_type = struct.unpack_from("=iI", buffer.raw, offset)
        if fd_type != _DARWIN_PIPE_FD_TYPE:
            continue
        pipe_size = libproc.proc_pidfdinfo(
            pid,
            fd,
            _DARWIN_PIPE_INFO_FLAVOR,
            pipe_buffer,
            len(pipe_buffer),
        )
        if pipe_size != _DARWIN_PIPE_INFO_SIZE:
            continue
        handle, peer_handle = struct.unpack_from(
            "=QQ",
            pipe_buffer.raw,
            _DARWIN_PIPE_HANDLE_OFFSET,
        )
        if tuple(sorted((handle, peer_handle))) in markers:
            return True
    return False


def _linux_process_observation(pid: int) -> _ProcessObservation | None:
    try:
        raw = (Path("/proc") / str(pid) / "stat").read_text(encoding="ascii")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    close = raw.rfind(")")
    fields = raw[close + 2 :].split()
    if close < 0 or len(fields) < 20:
        return None
    return _ProcessObservation(
        identity=ProcessIdentity(pid, (int(fields[19]), 0)),
        parent_pid=int(fields[1]),
    )


def _process_observation(pid: int) -> _ProcessObservation | None:
    if sys.platform == "darwin":
        return _darwin_process_observation(pid)
    if sys.platform.startswith("linux"):
        return _linux_process_observation(pid)
    return None


def _require_tracker_registration_deadline(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise TimeoutError("kernel tracker registration deadline expired")


class _DarwinKqueueProcessTracker:
    def __init__(self) -> None:
        self._known: dict[int, ProcessIdentity] = {}
        self._lifecycle_lock = threading.Lock()
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None
        self._root_pid: int | None = None
        self._root_started: tuple[int, int] | None = None
        self._deadline = 0.0
        self._registered: set[int] = set()
        self._poll_generation = 0
        self._queue: object | None = None
        self._owns_queue = False
        self._queue_tainted = False
        self._cleanup_pending = False
        self._construction_error: BaseException | None = None

    def _initialize_queue(self) -> None:
        with self._lifecycle_lock:
            self._initialize_queue_locked()

    def _initialize_queue_locked(self, *, deadline: float | None = None) -> None:
        _require_no_execution_hooks()
        if deadline is not None:
            _require_tracker_registration_deadline(deadline)
        if self._cleanup_pending:
            raise ContainedProcessError("kernel process tracker cleanup is pending")
        if self._construction_error is not None:
            raise self._construction_error
        if self._queue is not None:
            return
        try:
            queue = select.kqueue()
        except BaseException as exc:
            self._construction_error = exc
            raise
        self._queue = queue
        self._owns_queue = True
        _require_no_execution_hooks()
        if deadline is not None:
            _require_tracker_registration_deadline(deadline)

    def _register_process(self, identity: ProcessIdentity, *, deadline: float) -> bool:
        _require_no_execution_hooks()
        _require_tracker_registration_deadline(deadline)
        if identity.pid in self._registered and self._known.get(identity.pid) == identity:
            return True
        before = _call_with_execution_hook_guard(
            _darwin_process_observation,
            identity.pid,
        )
        _require_tracker_registration_deadline(deadline)
        if before is None:
            return False
        if before.identity != identity:
            raise ContainedProcessError("kernel child registration identity changed")
        event = _call_with_execution_hook_guard(
            select.kevent,
            identity.pid,
            filter=select.KQ_FILTER_PROC,
            flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_CLEAR,
            fflags=select.KQ_NOTE_FORK | select.KQ_NOTE_EXIT,
        )
        _require_tracker_registration_deadline(deadline)
        queue = self._queue
        if queue is None:
            raise ContainedProcessError("kernel process tracker queue is not initialized")
        _require_tracker_registration_deadline(deadline)
        self._queue_tainted = True
        try:
            _call_with_execution_hook_guard(
                queue.control,  # type: ignore[attr-defined]
                [event],
                0,
                0,
            )
        except OSError as exc:
            _require_tracker_registration_deadline(deadline)
            if exc.errno == errno.ESRCH:
                return False
            raise ContainedProcessError("kernel child registration failed") from exc
        _require_tracker_registration_deadline(deadline)
        after = _call_with_execution_hook_guard(
            _darwin_process_observation,
            identity.pid,
        )
        _require_tracker_registration_deadline(deadline)
        if after is not None and after.identity != identity:
            raise ContainedProcessError("kernel child identity changed during registration")
        self._registered.add(identity.pid)
        return True

    def _is_pristine(self) -> bool:
        queue_is_pristine = (self._queue is None and not self._owns_queue) or (
            self._queue is not None and self._owns_queue
        )
        return (
            not self._known
            and not self._registered
            and self._root_pid is None
            and self._root_started is None
            and self._thread is None
            and self._error is None
            and self._poll_generation == 0
            and self._deadline == 0.0
            and not self._stop.is_set()
            and self._construction_error is None
            and not self._queue_tainted
            and not self._cleanup_pending
            and queue_is_pristine
        )

    def _clear_state(self) -> None:
        with self._condition:
            self._known.clear()
            self._registered.clear()
            self._root_pid = None
            self._root_started = None
            self._deadline = 0.0
            self._thread = None
            self._error = None
            self._poll_generation = 0
            self._queue = None
            self._owns_queue = False
            self._queue_tainted = False
            self._cleanup_pending = False
            self._construction_error = None
            self._stop.clear()
            self._condition.notify_all()

    def _shutdown_locked(
        self,
        cleanup_errors: list[BaseException],
        *,
        deadline: float,
    ) -> bool:
        if self._thread is not None or self._owns_queue or self._queue is not None:
            self._cleanup_pending = True
        thread_stopped = True
        stop_unverifiable = False
        thread = self._thread
        if thread is not None:
            self._stop.set()
            try:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
            except BaseException as exc:
                _record_cleanup_error(cleanup_errors, exc)
            try:
                thread_stopped = not thread.is_alive()
            except BaseException as exc:
                thread_stopped = False
                stop_unverifiable = True
                _record_cleanup_error(cleanup_errors, exc)
                _record_cleanup_error(
                    cleanup_errors,
                    ContainedProcessError("kernel process tracker stop is unverifiable"),
                )
            if not thread_stopped and not stop_unverifiable:
                _record_cleanup_error(
                    cleanup_errors,
                    ContainedProcessError("kernel process tracker did not stop"),
                )

        if not thread_stopped:
            if self._owns_queue and self._queue is not None:
                _record_cleanup_error(
                    cleanup_errors,
                    ContainedProcessError("kernel process tracker queue remains open"),
                )
            return False

        if self._owns_queue:
            if self._queue is None:
                _record_cleanup_error(
                    cleanup_errors,
                    ContainedProcessError("kernel process tracker queue ownership is invalid"),
                )
                return False
            if not _close_resource_bounded(self._queue, cleanup_errors):
                _record_cleanup_error(
                    cleanup_errors,
                    ContainedProcessError("kernel process tracker queue remains open"),
                )
                return False
        self._clear_state()
        return True

    def register_root(self, pid: int, *, deadline: float) -> ProcessIdentity:
        _require_no_execution_hooks()
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not self._lifecycle_lock.acquire(timeout=remaining):
            raise TimeoutError("kernel tracker registration deadline expired")
        try:
            _require_no_execution_hooks()
            _require_tracker_registration_deadline(deadline)
            if not self._is_pristine():
                raise ContainedProcessError("kernel process tracker is not pristine")
            try:
                self._initialize_queue_locked(deadline=deadline)
                before = _call_with_execution_hook_guard(_darwin_process_observation, pid)
                _require_tracker_registration_deadline(deadline)
                if before is None:
                    raise ContainedProcessError("kernel root registration failed")
                # Darwin exposes NOTE_TRACK constants through Python but rejects that
                # FreeBSD extension with ENOTSUP. NOTE_FORK is the supported kernel edge;
                # every discovered child is registered before it becomes trusted.
                if not self._register_process(before.identity, deadline=deadline):
                    raise ContainedProcessError("kernel root registration failed")
                _require_tracker_registration_deadline(deadline)
                thread = _call_with_execution_hook_guard(
                    threading.Thread,
                    target=self._track,
                    name=f"rquant-kqueue-{pid}",
                    daemon=True,
                )
                _require_tracker_registration_deadline(deadline)
                with self._condition:
                    self._root_pid = pid
                    self._root_started = before.identity.started
                    self._known[pid] = before.identity
                    self._deadline = deadline
                    self._thread = thread
                _require_tracker_registration_deadline(deadline)
                _call_with_execution_hook_guard(thread.start)
                _require_tracker_registration_deadline(deadline)
            except BaseException as primary_exception:
                cleanup_errors: list[BaseException] = []
                self._shutdown_locked(cleanup_errors, deadline=deadline)
                _attach_cleanup_error_group(
                    primary_exception,
                    cleanup_errors,
                    error_label="kernel process tracker startup cleanup failures",
                    note="kernel process tracker startup cleanup also failed",
                )
                raise
            return before.identity
        finally:
            self._lifecycle_lock.release()

    def _track(self) -> None:
        try:
            _require_no_execution_hooks()
        except BaseException as exc:
            if threading.current_thread() is not self._thread:
                raise
            with self._condition:
                if self._error is None:
                    self._error = exc
                self._condition.notify_all()
            return
        try:
            queue = self._queue
            if queue is None:
                raise ContainedProcessError("kernel process tracker queue is not initialized")
            while not self._stop.is_set():
                with self._condition:
                    if self._error is not None:
                        return
                if time.monotonic() >= self._deadline:
                    raise TimeoutError("kernel tracker deadline expired")
                events = _call_with_execution_hook_guard(
                    queue.control,  # type: ignore[attr-defined]
                    None,
                    256,
                    0.01,
                )
                fork_observed = False
                for event in events:
                    if event.fflags & select.KQ_NOTE_TRACKERR:
                        raise ContainedProcessError("kernel process tracker NOTE_TRACKERR")
                    fork_observed = fork_observed or bool(event.fflags & select.KQ_NOTE_FORK)
                if fork_observed:
                    if self._root_pid is None or self._root_started is None:
                        raise ContainedProcessError("kernel root is not registered")
                    inventory = _call_with_execution_hook_guard(
                        _darwin_process_inventory,
                        self._deadline,
                        started_at_or_after=self._root_started,
                    )
                with self._condition:
                    if fork_observed:
                        assert self._root_pid is not None
                        descendants = _discover_descendants(
                            self._root_pid,
                            inventory,
                            self._known,
                        )
                        for pid, identity in descendants.items():
                            prior = self._known.get(pid)
                            if prior is not None and prior != identity:
                                raise ContainedProcessError(
                                    "kernel tracker observed PID identity reuse"
                                )
                            self._register_process(identity, deadline=self._deadline)
                            self._known[pid] = identity
                    self._poll_generation += 1
                    self._condition.notify_all()
        except BaseException as exc:
            if self._stop.is_set() and isinstance(exc, OSError) and exc.errno == errno.EBADF:
                return
            with self._condition:
                if self._error is None:
                    self._error = exc
                self._condition.notify_all()

    def poll(self, *, deadline: float) -> dict[int, ProcessIdentity]:
        try:
            _require_no_execution_hooks()
        except BaseException:
            with self._condition:
                first_error = self._error
            if first_error is not None:
                raise ContainedProcessError("kernel process tracking failed") from first_error
            raise
        with self._condition:
            self._require_poll_hooks_locked()
            observed_generation = self._poll_generation
            while self._error is None and self._poll_generation == observed_generation:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("kernel process tracker poll timed out")
                self._condition.wait(timeout=remaining)
                self._require_poll_hooks_locked()
            self._require_poll_hooks_locked()
            if self._error is not None:
                raise ContainedProcessError("kernel process tracking failed") from self._error
            known = dict(self._known)
            self._require_poll_hooks_locked()
            return known

    def _require_poll_hooks_locked(self) -> None:
        try:
            _require_no_execution_hooks()
        except BaseException:
            if self._error is not None:
                raise ContainedProcessError("kernel process tracking failed") from self._error
            raise

    def close(self) -> None:
        if not self._lifecycle_lock.acquire(blocking=False):
            raise ContainedProcessError("kernel process tracker cleanup lifecycle is busy")
        try:
            cleanup_errors: list[BaseException] = []
            self._shutdown_locked(
                cleanup_errors,
                deadline=self._deadline,
            )
            if cleanup_errors:
                _raise_tracker_cleanup_error(
                    "kernel process tracker cleanup failed",
                    cleanup_errors,
                )
        finally:
            self._lifecycle_lock.release()


class _LinuxSubreaperProcessTracker:
    _PR_SET_CHILD_SUBREAPER = 36
    _PR_GET_CHILD_SUBREAPER = 37

    def __init__(self) -> None:
        self._known: dict[int, ProcessIdentity] = {}
        self._pidfds: dict[int, int] = {}
        self._pending_pidfds: list[int] = []
        self._root_pid: int | None = None
        self._root_started: tuple[int, int] | None = None
        self._previous_subreaper = 0
        self._owns_subreaper_lock = False
        self._subreaper_changed = False
        self._subreaper_owner: _LinuxSubreaperProcessTracker | None = None
        self._subreaper_depth = 0

    def _enable_subreaper(self, deadline: float) -> None:
        _require_no_execution_hooks()
        owner = getattr(_LINUX_SUBREAPER_OWNERSHIP, "owner", None)
        if owner is not None:
            owner._subreaper_depth += 1
            self._subreaper_owner = owner
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not _LINUX_SUBREAPER_LOCK.acquire(timeout=remaining):
            raise TimeoutError("subreaper registration deadline expired")
        self._owns_subreaper_lock = True
        self._subreaper_owner = self
        self._subreaper_depth = 1
        _LINUX_SUBREAPER_OWNERSHIP.owner = self
        libc = ctypes.CDLL(None, use_errno=True)
        current = ctypes.c_int()
        if libc.prctl(self._PR_GET_CHILD_SUBREAPER, ctypes.byref(current), 0, 0, 0) != 0:
            raise ContainedProcessError("could not read child subreaper state")
        self._previous_subreaper = int(current.value)
        if self._previous_subreaper != 1:
            if libc.prctl(self._PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
                raise ContainedProcessError("could not enable child subreaper")
            self._subreaper_changed = True

    def _bind_pid(self, identity: ProcessIdentity) -> None:
        _require_no_execution_hooks()
        needs_pidfd = identity.pid not in self._pidfds and hasattr(os, "pidfd_open")
        prior = self._known.get(identity.pid)
        if prior is not None and prior != identity:
            raise ContainedProcessError("kernel tracker observed PID identity reuse")
        self._known[identity.pid] = identity
        if needs_pidfd:
            descriptor = -1
            try:
                try:
                    descriptor = os.pidfd_open(identity.pid, 0)
                    self._pending_pidfds.append(descriptor)
                except BaseException:
                    if descriptor >= 0 and descriptor not in self._pending_pidfds:
                        self._pending_pidfds.append(descriptor)
                    raise
                self._pidfds[identity.pid] = descriptor
                self._pending_pidfds.remove(descriptor)
            except BaseException as primary_exception:
                if descriptor >= 0:
                    if (
                        descriptor not in self._pending_pidfds
                        and descriptor not in self._pidfds.values()
                    ):
                        self._pending_pidfds.append(descriptor)
                    cleanup_errors: list[BaseException] = []
                    pending_close = [descriptor]
                    if _close_file_descriptors(pending_close, cleanup_errors):
                        self._pending_pidfds[:] = [
                            owned for owned in self._pending_pidfds if owned != descriptor
                        ]
                        try:
                            for pid, owned in tuple(self._pidfds.items()):
                                if owned == descriptor:
                                    self._pidfds.pop(pid, None)
                        except BaseException as exc:
                            _record_cleanup_error(cleanup_errors, exc)
                    _attach_cleanup_error_group(
                        primary_exception,
                        cleanup_errors,
                        error_label="pidfd binding cleanup failures",
                        note="pidfd binding cleanup also failed",
                    )
                if isinstance(primary_exception, ProcessLookupError):
                    return
                raise

    def register_root(self, pid: int, *, deadline: float) -> ProcessIdentity:
        _require_no_execution_hooks()
        self._enable_subreaper(deadline)
        observed = _linux_process_observation(pid)
        if observed is None:
            raise ContainedProcessError("kernel root registration failed")
        self._root_pid = pid
        self._root_started = observed.identity.started
        self._bind_pid(observed.identity)
        return observed.identity

    def poll(self, *, deadline: float) -> dict[int, ProcessIdentity]:
        _require_no_execution_hooks()
        if self._root_pid is None or self._root_started is None:
            raise ContainedProcessError("kernel root is not registered")
        inventory = _linux_process_inventory(deadline)
        descendants = _discover_descendants(self._root_pid, inventory, self._known)
        for observation in inventory.values():
            if (
                observation.parent_pid == os.getpid()
                and observation.identity.started >= self._root_started
            ):
                descendants[observation.identity.pid] = observation.identity
        for identity in descendants.values():
            self._bind_pid(identity)
        for pid, identity in sorted(self._known.items()):
            if pid == self._root_pid:
                continue
            observation = inventory.get(pid)
            if (
                observation is None
                or observation.identity != identity
                or observation.parent_pid != os.getpid()
            ):
                continue
            try:
                reaped_pid, _status = os.waitpid(pid, os.WNOHANG)
            except (ChildProcessError, ProcessLookupError):
                continue
            if reaped_pid not in {0, pid}:
                raise ContainedProcessError("subreaper reaped an unexpected process identity")
        return dict(self._known)

    def close(self) -> None:
        cleanup_errors: list[BaseException] = []
        remaining_descriptors: list[int] = []
        for descriptor in (*self._pending_pidfds, *self._pidfds.values()):
            if descriptor not in remaining_descriptors:
                remaining_descriptors.append(descriptor)
        _close_file_descriptors(remaining_descriptors, cleanup_errors)
        unresolved_descriptors = set(remaining_descriptors)
        self._pending_pidfds[:] = [
            descriptor
            for descriptor in self._pending_pidfds
            if descriptor in unresolved_descriptors
        ]
        for pid, descriptor in tuple(self._pidfds.items()):
            if descriptor not in unresolved_descriptors:
                self._pidfds.pop(pid, None)
        if unresolved_descriptors:
            _record_cleanup_error(
                cleanup_errors,
                ContainedProcessError("kernel process tracker descriptors remain open"),
            )
        restore_failed = False
        owner = self._subreaper_owner
        if owner is not None and owner is not self:
            if getattr(_LINUX_SUBREAPER_OWNERSHIP, "owner", None) is not owner:
                _record_cleanup_error(
                    cleanup_errors,
                    ContainedProcessError("subreaper ownership belongs to a different thread"),
                )
            elif owner._subreaper_depth <= 1:
                _record_cleanup_error(
                    cleanup_errors,
                    ContainedProcessError("subreaper ownership depth is invalid"),
                )
            else:
                owner._subreaper_depth -= 1
                self._subreaper_owner = None
        elif self._owns_subreaper_lock:
            if owner is self and self._subreaper_depth != 1:
                _record_cleanup_error(
                    cleanup_errors,
                    ContainedProcessError("nested subreaper tracker remains active during cleanup"),
                )
            else:
                if owner is self:
                    del _LINUX_SUBREAPER_OWNERSHIP.owner
                    self._subreaper_owner = None
                    self._subreaper_depth = 0
                try:
                    if self._subreaper_changed:
                        libc = ctypes.CDLL(None, use_errno=True)
                        restore_failed = (
                            libc.prctl(
                                self._PR_SET_CHILD_SUBREAPER,
                                self._previous_subreaper,
                                0,
                                0,
                                0,
                            )
                            != 0
                        )
                finally:
                    self._subreaper_changed = False
                    self._owns_subreaper_lock = False
                    _LINUX_SUBREAPER_LOCK.release()
        if restore_failed:
            _record_cleanup_error(
                cleanup_errors,
                ContainedProcessError("could not restore child subreaper state"),
            )
        if cleanup_errors:
            _raise_tracker_cleanup_error(
                "kernel process tracker cleanup failed",
                cleanup_errors,
            )


def _create_kernel_tracker() -> _KernelProcessTracker:
    if sys.platform == "darwin":
        return _DarwinKqueueProcessTracker()
    if sys.platform.startswith("linux"):
        return _LinuxSubreaperProcessTracker()
    raise ContainedProcessError("kernel process tracking is unsupported on this platform")


def _darwin_process_has_token(pid: int, token: str, *, deadline: float) -> bool:
    if time.monotonic() >= deadline:
        raise TimeoutError("process environment inventory timed out")
    libc = ctypes.CDLL(None, use_errno=True)
    libc.sysctl.argtypes = [
        ctypes.POINTER(ctypes.c_int),
        ctypes.c_uint,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    libc.sysctl.restype = ctypes.c_int
    mib = (ctypes.c_int * 3)(1, 49, pid)  # CTL_KERN, KERN_PROCARGS2, pid
    size = ctypes.c_size_t()
    if libc.sysctl(mib, 3, None, ctypes.byref(size), None, 0) != 0:
        error = ctypes.get_errno()
        if error in {errno.ESRCH, errno.EPERM, errno.EACCES, errno.EIO, errno.EINVAL}:
            return False
        raise OSError(error, "sysctl KERN_PROCARGS2 size")
    if size.value <= 0 or size.value > _MAX_PROCESS_ARGUMENT_BYTES:
        return False
    buffer = ctypes.create_string_buffer(size.value)
    if libc.sysctl(mib, 3, buffer, ctypes.byref(size), None, 0) != 0:
        error = ctypes.get_errno()
        if error in {errno.ESRCH, errno.EPERM, errno.EACCES, errno.EIO, errno.EINVAL}:
            return False
        raise OSError(error, "sysctl KERN_PROCARGS2 payload")
    expected = f"{_CONTAINMENT_ENVIRONMENT_KEY}={token}".encode()
    return expected in buffer.raw[: size.value].split(b"\0")


def _linux_process_has_token(pid: int, token: str, *, deadline: float) -> bool:
    if time.monotonic() >= deadline:
        raise TimeoutError("process environment inventory timed out")
    expected = f"{_CONTAINMENT_ENVIRONMENT_KEY}={token}".encode()
    try:
        payload = (Path("/proc") / str(pid) / "environ").read_bytes()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return False
    if len(payload) > _MAX_PROCESS_ARGUMENT_BYTES:
        raise ContainedProcessError("process environment exceeds containment budget")
    return expected in payload.split(b"\0")


def _darwin_process_inventory(
    deadline: float,
    *,
    containment_token: str | None = None,
    started_at_or_after: tuple[int, int] | None = None,
    pipe_markers: frozenset[DarwinPipeMarker] = frozenset(),
) -> dict[int, _ProcessObservation]:
    try:
        libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        libproc.proc_listallpids.argtypes = [ctypes.c_void_p, ctypes.c_int]
        libproc.proc_listallpids.restype = ctypes.c_int
        libproc.proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        libproc.proc_pidinfo.restype = ctypes.c_int
        capacity = max(256, libproc.proc_listallpids(None, 0) * 2)
        pids = (ctypes.c_int * capacity)()
        count = libproc.proc_listallpids(pids, ctypes.sizeof(pids))
        if count < 0:
            raise OSError(ctypes.get_errno(), "proc_listallpids")
        result: dict[int, _ProcessObservation] = {}
        for pid in pids[:count]:
            if time.monotonic() >= deadline:
                raise TimeoutError("process inventory timed out")
            buffer = ctypes.create_string_buffer(256)
            size = libproc.proc_pidinfo(pid, 3, 0, buffer, len(buffer))
            if size < 136:
                continue
            _flags, _status, _xstatus, observed_pid, parent, effective_uid = struct.unpack_from(
                "=IIIIII", buffer.raw
            )
            start_seconds, start_microseconds = struct.unpack_from("=QQ", buffer.raw, 120)
            if observed_pid != pid or start_seconds <= 0 or effective_uid != os.getuid():
                continue
            unique_identity = _darwin_unique_process_identity(libproc, pid)
            if unique_identity is None:
                continue
            unique_id, parent_unique_id = unique_identity
            result[pid] = _ProcessObservation(
                identity=ProcessIdentity(
                    pid,
                    (start_seconds, start_microseconds),
                    kernel_unique_id=unique_id,
                ),
                parent_pid=parent,
                containment_token=(
                    (
                        started_at_or_after is None
                        or (start_seconds, start_microseconds) >= started_at_or_after
                    )
                    and (
                        (
                            containment_token is not None
                            and _darwin_process_has_token(
                                pid,
                                containment_token,
                                deadline=deadline,
                            )
                        )
                        or _darwin_process_has_pipe_marker(
                            pid,
                            pipe_markers,
                            deadline=deadline,
                        )
                    )
                ),
                parent_kernel_unique_id=parent_unique_id,
            )
        return result
    except TimeoutError:
        raise
    except (OSError, ValueError) as exc:
        raise ContainedProcessError("process inventory failed") from exc


def _linux_process_inventory(
    deadline: float,
    *,
    containment_token: str | None = None,
    started_at_or_after: tuple[int, int] | None = None,
) -> dict[int, _ProcessObservation]:
    result: dict[int, _ProcessObservation] = {}
    try:
        for entry in Path("/proc").iterdir():
            if time.monotonic() >= deadline:
                raise TimeoutError("process inventory timed out")
            if not entry.name.isdigit():
                continue
            try:
                raw = (entry / "stat").read_text(encoding="ascii")
            except OSError as exc:
                if isinstance(exc, (FileNotFoundError, ProcessLookupError)) or exc.errno in {
                    errno.ENOENT,
                    errno.ESRCH,
                }:
                    continue
                raise
            close = raw.rfind(")")
            fields = raw[close + 2 :].split()
            if close < 0 or len(fields) < 20:
                raise ValueError("process stat is malformed")
            pid = int(entry.name)
            identity = ProcessIdentity(pid, (int(fields[19]), 0))
            result[pid] = _ProcessObservation(
                identity=identity,
                parent_pid=int(fields[1]),
                containment_token=(
                    containment_token is not None
                    and (started_at_or_after is None or identity.started >= started_at_or_after)
                    and _linux_process_has_token(pid, containment_token, deadline=deadline)
                ),
            )
    except TimeoutError:
        raise
    except (OSError, ValueError) as exc:
        raise ContainedProcessError("process inventory failed") from exc
    return result


def process_inventory(
    deadline: float,
    *,
    containment_token: str | None = None,
    started_at_or_after: tuple[int, int] | None = None,
    darwin_pipe_markers: frozenset[DarwinPipeMarker] = frozenset(),
) -> dict[int, _ProcessObservation]:
    if time.monotonic() >= deadline:
        raise TimeoutError("process inventory deadline expired")
    if sys.platform == "darwin":
        return _darwin_process_inventory(
            deadline,
            containment_token=containment_token,
            started_at_or_after=started_at_or_after,
            pipe_markers=darwin_pipe_markers,
        )
    if sys.platform.startswith("linux"):
        return _linux_process_inventory(
            deadline,
            containment_token=containment_token,
            started_at_or_after=started_at_or_after,
        )
    raise ContainedProcessError("process inventory is unsupported on this platform")


def _discover_descendants(
    root_pid: int,
    inventory: Mapping[int, _ProcessObservation],
    known: Mapping[int, ProcessIdentity],
) -> dict[int, ProcessIdentity]:
    parents: dict[int, set[int]] = {}
    birth_parents: dict[int, set[int]] = {}
    for observation in inventory.values():
        parents.setdefault(observation.parent_pid, set()).add(observation.identity.pid)
        if observation.parent_kernel_unique_id:
            birth_parents.setdefault(observation.parent_kernel_unique_id, set()).add(
                observation.identity.pid
            )
    pending = [root_pid, *known]
    descendants = dict(known)
    for observation in inventory.values():
        if not observation.containment_token:
            continue
        prior = descendants.get(observation.identity.pid)
        if prior is None or prior == observation.identity:
            descendants[observation.identity.pid] = observation.identity
    visited: set[int] = set()
    while pending:
        parent = pending.pop()
        if parent in visited:
            continue
        visited.add(parent)
        parent_observation = inventory.get(parent)
        parent_identity = (
            parent_observation.identity if parent_observation is not None else known.get(parent)
        )
        child_pids = set(parents.get(parent, ()))
        if parent_identity is not None and parent_identity.kernel_unique_id:
            child_pids.update(birth_parents.get(parent_identity.kernel_unique_id, ()))
        for pid in child_pids:
            observation = inventory.get(pid)
            if observation is None:
                continue
            prior = descendants.get(pid)
            if prior is not None and prior != observation.identity:
                # A reused PID is not evidence that the replacement process belongs
                # to the original tree. Do not signal it or traverse through it.
                continue
            descendants[pid] = observation.identity
            pending.append(pid)
    descendants.pop(root_pid, None)
    return descendants


def _signal_identity(
    identity: ProcessIdentity,
    signum: int,
    inventory: Mapping[int, _ProcessObservation],
) -> None:
    observation = inventory.get(identity.pid)
    if observation is None or observation.identity != identity:
        return
    try:
        os.kill(identity.pid, signum)
    except ProcessLookupError:
        return
    except PermissionError as exc:
        raise ContainedProcessError("process descendant containment is unverifiable") from exc


def _signal_bound_identity(identity: ProcessIdentity, signum: int) -> None:
    observed = _process_observation(identity.pid)
    if observed is None or observed.identity != identity:
        return
    try:
        os.kill(identity.pid, signum)
    except ProcessLookupError:
        return
    except PermissionError as exc:
        raise ContainedProcessError("kernel-tracked process cannot be signalled") from exc


def _merge_kernel_identities(
    known: dict[int, ProcessIdentity],
    tracker: _KernelProcessTracker,
    *,
    root_pid: int,
    deadline: float,
) -> None:
    for pid, identity in tracker.poll(deadline=deadline).items():
        if pid == root_pid:
            continue
        prior = known.get(pid)
        if prior is not None and prior != identity:
            raise ContainedProcessError("kernel tracker observed PID identity reuse")
        known[pid] = identity


def _terminate_blocked_root(process: subprocess.Popen[str], *, deadline: float) -> None:
    signal_error: PermissionError | None = None
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError as exc:
        signal_error = exc
        with suppress(ProcessLookupError):
            process.kill()
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ContainedProcessError("blocked subprocess cleanup deadline expired")
    try:
        process.communicate(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        raise ContainedProcessError("blocked subprocess could not be reaped") from exc
    if signal_error is not None:
        raise ContainedProcessError(
            "blocked process group could not be signalled"
        ) from signal_error


def _cleanup_process_tree(
    process: subprocess.Popen[str],
    known: dict[int, ProcessIdentity],
    *,
    root_identity: ProcessIdentity | None = None,
    deadline: float,
    inventory_provider: Inventory,
    clock: Clock,
    sleep: Sleep,
    kernel_tracker: _KernelProcessTracker | None = None,
    initial_inventory: Mapping[int, _ProcessObservation] | None = None,
) -> None:
    kernel_error: BaseException | None = None
    process_group_errors: list[PermissionError] = []

    def signal_process_group(signum: int) -> None:
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            return
        except PermissionError as exc:
            process_group_errors.append(exc)

    def merge_kernel() -> None:
        nonlocal kernel_error
        if kernel_tracker is None or kernel_error is not None:
            return
        try:
            _merge_kernel_identities(
                known,
                kernel_tracker,
                root_pid=process.pid,
                deadline=deadline,
            )
        except BaseException as exc:
            kernel_error = exc

    signal_process_group(signal.SIGSTOP)
    merge_kernel()
    if initial_inventory is not None:
        if root_identity is not None:
            _signal_identity(root_identity, signal.SIGSTOP, initial_inventory)
            root = initial_inventory.get(process.pid)
            if root is not None and root.identity == root_identity:
                signal_process_group(signal.SIGSTOP)
        for identity in tuple(known.values()):
            _signal_identity(identity, signal.SIGSTOP, initial_inventory)
    stable = 0
    prior: frozenset[ProcessIdentity] = frozenset()
    while stable < 2:
        remaining = deadline - clock()
        if remaining <= 0:
            raise ContainedProcessError("process containment deadline expired")
        inventory = inventory_provider(deadline)
        merge_kernel()
        known.update(_discover_descendants(process.pid, inventory, known))
        if root_identity is not None:
            _signal_identity(root_identity, signal.SIGSTOP, inventory)
            root = inventory.get(process.pid)
            if root is not None and root.identity == root_identity:
                signal_process_group(signal.SIGSTOP)
        for identity in tuple(known.values()):
            _signal_identity(identity, signal.SIGSTOP, inventory)
            _signal_bound_identity(identity, signal.SIGSTOP)
        current = frozenset(known.values())
        stable = stable + 1 if current == prior else 0
        prior = current
        if stable < 2:
            sleep(min(0.01, max(0.0, deadline - clock())))

    inventory = inventory_provider(deadline)
    merge_kernel()
    for identity in sorted(known.values(), reverse=True):
        _signal_identity(identity, signal.SIGKILL, inventory)
        _signal_bound_identity(identity, signal.SIGKILL)
    if root_identity is not None:
        _signal_identity(root_identity, signal.SIGKILL, inventory)
    signal_process_group(signal.SIGKILL)

    remaining = deadline - clock()
    if remaining <= 0:
        raise ContainedProcessError("process containment deadline expired before reap")
    try:
        process.communicate(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        raise ContainedProcessError("process group could not be reaped") from exc

    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            raise ContainedProcessError("detached process descendants survived cleanup")
        inventory = inventory_provider(deadline)
        merge_kernel()
        alive = {
            pid: identity
            for pid, identity in known.items()
            if (
                (pid in inventory and inventory[pid].identity == identity)
                or (
                    (observed := _process_observation(pid)) is not None
                    and observed.identity == identity
                )
            )
        }
        if not alive:
            if kernel_error is not None:
                raise ContainedProcessError(
                    "kernel process tracking failed during cleanup"
                ) from kernel_error
            if process_group_errors:
                raise ContainedProcessError(
                    "process group signalling failed during containment cleanup"
                ) from ExceptionGroup(
                    "process group signal failures",
                    process_group_errors,
                )
            return
        for identity in alive.values():
            _signal_identity(identity, signal.SIGKILL, inventory)
            _signal_bound_identity(identity, signal.SIGKILL)
        sleep(min(0.01, remaining))


def run_contained(
    args: Sequence[str],
    *,
    cwd: Path,
    deadline_monotonic: float,
    check: bool = False,
    pass_fds: tuple[int, ...] = (),
    env: Mapping[str, str] | None = None,
    text: bool = True,
    inventory_provider: Inventory = process_inventory,
    clock: Clock = time.monotonic,
    sleep: Sleep = time.sleep,
    cancellation_check: Callable[[], bool] | None = None,
    kernel_tracker_factory: KernelTrackerFactory = _create_kernel_tracker,
    may_spawn_background_descendants: bool,
) -> subprocess.CompletedProcess[str]:
    """Run one command within one absolute deadline and clean up its process tree.

    ``may_spawn_background_descendants`` is a required launch capability declaration.
    Darwin refuses ``True`` before spawning because its available process APIs cannot
    prove containment after a descendant reparents and discards inherited evidence.
    Passing ``False`` is therefore a caller guarantee that the command does not
    intentionally daemonize; it is not a stronger Darwin kernel guarantee.
    Active trace or profile hooks are rejected before containment acquires resources.
    """

    _require_no_execution_hooks()
    remaining = deadline_monotonic - clock()
    if remaining <= 0:
        raise subprocess.TimeoutExpired(list(args), 0)
    cleanup_reserve = _cleanup_reserve_seconds(remaining)
    execution_deadline = deadline_monotonic - cleanup_reserve
    if execution_deadline <= clock():
        raise subprocess.TimeoutExpired(list(args), 0)
    if sys.platform == "darwin" and may_spawn_background_descendants:
        raise ContainedProcessError(
            "Darwin cannot prove containment for background-capable commands; startup refused"
        )
    signal_latch = _ContainedSignalLatch()
    empty_ownership = frozenset()
    previous_handlers: dict[int, object] = {}
    active_signals: frozenset[int] = empty_ownership
    kernel_tracker: _KernelProcessTracker | None = None
    gate_read = gate_write = -1
    process: subprocess.Popen[str] | None = None
    darwin_pipe_markers: frozenset[DarwinPipeMarker] = empty_ownership
    darwin_pipe_anchor_fds: list[int] = []
    darwin_pending_anchor_fds: list[int] = []

    def close_darwin_pipe_anchors(cleanup_errors: list[BaseException]) -> bool:
        return _close_file_descriptor_inventories(
            (darwin_pending_anchor_fds, darwin_pipe_anchor_fds),
            cleanup_errors,
        )

    try:
        previous_handlers, active_signals = _install_signal_latch(signal_latch)
        containment_token = secrets.token_hex(32)
        process_environment = dict(os.environ if env is None else env)
        process_environment[_CONTAINMENT_ENVIRONMENT_KEY] = containment_token
        _require_no_execution_hooks()
        kernel_tracker = kernel_tracker_factory()
        if isinstance(kernel_tracker, _DarwinKqueueProcessTracker):
            kernel_tracker._initialize_queue()
        _require_no_execution_hooks()
        gate_read, gate_write = os.pipe()
        helper_command = [
            sys.executable,
            "-I",
            "-S",
            str(Path(__file__).resolve(strict=True)),
            "--contained-child",
            str(gate_read),
            "--",
            *args,
        ]
        _require_no_execution_hooks()
        process = subprocess.Popen(
            helper_command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=text,
            start_new_session=True,
            pass_fds=(*pass_fds, gate_read),
            env=process_environment,
        )
        os.close(gate_read)
        gate_read = -1
        if sys.platform == "darwin":
            if process.stdout is None or process.stderr is None:
                raise ContainedProcessError("Darwin containment pipes are unavailable")
            for stream in (process.stdout, process.stderr):
                anchor = -1
                try:
                    _require_no_execution_hooks()
                    try:
                        anchor = os.dup(stream.fileno())
                        darwin_pending_anchor_fds.append(anchor)
                    except BaseException:
                        if anchor >= 0 and anchor not in darwin_pending_anchor_fds:
                            darwin_pending_anchor_fds.append(anchor)
                        raise
                    darwin_pipe_anchor_fds.append(anchor)
                    darwin_pending_anchor_fds.remove(anchor)
                    os.set_inheritable(anchor, False)
                except BaseException:
                    if (
                        anchor >= 0
                        and anchor not in darwin_pending_anchor_fds
                        and anchor not in darwin_pipe_anchor_fds
                    ):
                        darwin_pending_anchor_fds.append(anchor)
                    raise
            darwin_pipe_markers = frozenset(
                {_darwin_pipe_marker_for_fd(os.getpid(), fd) for fd in darwin_pipe_anchor_fds}
            )
        root_identity = kernel_tracker.register_root(
            process.pid,
            deadline=deadline_monotonic,
        )
        if root_identity.pid != process.pid:
            raise ContainedProcessError("kernel tracker root identity does not match subprocess")
        root_started: tuple[int, int] | None = root_identity.started
        if cancellation_check is not None and cancellation_check():
            raise ContainedProcessError("contained process was cancelled before startup")
        kernel_tracker.poll(deadline=deadline_monotonic)
    except BaseException as primary_exception:
        startup_cleanup_errors: list[BaseException] = []
        if process is not None:
            try:
                _terminate_blocked_root(process, deadline=deadline_monotonic)
            except BaseException as exc:
                startup_cleanup_errors.append(exc)
        for descriptor in (gate_read, gate_write):
            if descriptor < 0:
                continue
            try:
                os.close(descriptor)
            except BaseException as exc:
                startup_cleanup_errors.append(exc)
        if kernel_tracker is not None:
            try:
                kernel_tracker.close()
            except BaseException as exc:
                startup_cleanup_errors.append(exc)
        anchors_closed = close_darwin_pipe_anchors(startup_cleanup_errors)
        _finish_signal_restoration(
            previous_handlers,
            active_signals,
            signal_latch,
            startup_cleanup_errors,
            primary_exception=primary_exception,
            error_label="contained subprocess startup cleanup failures",
            replay_ready=anchors_closed,
        )
        raise

    assert process is not None
    assert kernel_tracker is not None

    def observe(deadline: float) -> dict[int, _ProcessObservation]:
        if inventory_provider is process_inventory:
            return process_inventory(
                deadline,
                containment_token=containment_token,
                started_at_or_after=root_started,
                darwin_pipe_markers=darwin_pipe_markers,
            )
        return inventory_provider(deadline)

    initial_inventory: Mapping[int, _ProcessObservation] | None = None
    known: dict[int, ProcessIdentity] = {}
    known_lock = threading.Lock()
    tracker_stop = threading.Event()
    tracker_thread: threading.Thread | None = None
    tracker_errors: list[BaseException] = []
    cleanup_errors: list[BaseException] = []
    tracker_last_inventory: Mapping[int, _ProcessObservation] | None = None
    root_exit_observed_at: float | None = None
    last_inventory: Mapping[int, _ProcessObservation] | None = None
    stdout = stderr = ""

    def stop_tracker() -> None:
        nonlocal tracker_thread
        tracker_stop.set()
        if tracker_thread is not None:
            tracker_thread.join(timeout=max(0.0, deadline_monotonic - clock()))
            if tracker_thread.is_alive():
                cleanup_errors.append(
                    ContainedProcessError("process containment tracker did not stop")
                )
                return
            tracker_thread = None

    def track_process_tree() -> None:
        nonlocal tracker_last_inventory
        try:
            while not tracker_stop.is_set():
                tracker_deadline = min(execution_deadline, time.monotonic() + 0.05)
                try:
                    inventory = observe(tracker_deadline)
                except TimeoutError:
                    if time.monotonic() >= execution_deadline:
                        return
                    continue
                with known_lock:
                    known.update(_discover_descendants(process.pid, inventory, known))
                    tracked = tuple(known.values())
                    tracker_last_inventory = inventory
                if process.poll() is not None:
                    for identity in tracked:
                        _signal_identity(identity, signal.SIGSTOP, inventory)
                tracker_stop.wait(0.001)
        except BaseException as exc:
            tracker_errors.append(exc)

    try:
        signal_latch.checkpoint()
        initial_inventory = observe(execution_deadline)
        last_inventory = initial_inventory
        observed_root = initial_inventory.get(process.pid)
        if observed_root is None:
            raise ContainedProcessError("startup inventory cannot prove root identity")
        if observed_root.identity != root_identity:
            raise ContainedProcessError("inventory root identity differs from kernel tracker")
        known.update(_discover_descendants(process.pid, initial_inventory, known))
        _merge_kernel_identities(
            known,
            kernel_tracker,
            root_pid=process.pid,
            deadline=deadline_monotonic,
        )
        if inventory_provider is process_inventory:
            tracker_thread = threading.Thread(
                target=track_process_tree,
                name=f"rquant-containment-{process.pid}",
                daemon=True,
            )
            tracker_thread.start()
        os.write(gate_write, b"1")
        os.close(gate_write)
        gate_write = -1
        while True:
            signal_latch.checkpoint()
            if tracker_errors:
                raise ContainedProcessError(
                    "process containment tracker failed"
                ) from tracker_errors[0]
            if cancellation_check is not None and cancellation_check():
                raise ContainedProcessError("contained process was cancelled")
            signal_latch.checkpoint()
            _merge_kernel_identities(
                known,
                kernel_tracker,
                root_pid=process.pid,
                deadline=deadline_monotonic,
            )
            remaining = execution_deadline - clock()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(list(args), 0)
            try:
                inventory = observe(execution_deadline)
            except (ContainedProcessError, TimeoutError):
                if clock() >= execution_deadline:
                    raise subprocess.TimeoutExpired(list(args), 0) from None
                raise
            last_inventory = inventory
            observed_root = inventory.get(process.pid)
            if observed_root is not None:
                if root_identity is not None and observed_root.identity != root_identity:
                    raise ContainedProcessError("subprocess PID identity changed while running")
                root_identity = observed_root.identity
            with known_lock:
                known.update(_discover_descendants(process.pid, inventory, known))
            if process.poll() is not None:
                alive_descendants = {
                    pid: identity
                    for pid, identity in known.items()
                    if (
                        (pid in inventory and inventory[pid].identity == identity)
                        or (
                            (observed := _process_observation(pid)) is not None
                            and observed.identity == identity
                        )
                    )
                }
                if alive_descendants:
                    root_exit_observed_at = root_exit_observed_at or clock()
                    if clock() - root_exit_observed_at >= 0.05:
                        raise ContainedProcessError(
                            "subprocess root exited while descendants were still running"
                        )
                    sleep(min(0.005, max(0.0, execution_deadline - clock())))
                    continue
            try:
                stdout, stderr = process.communicate(timeout=min(0.02, remaining))
                _merge_kernel_identities(
                    known,
                    kernel_tracker,
                    root_pid=process.pid,
                    deadline=deadline_monotonic,
                )
                signal_latch.checkpoint()
                break
            except subprocess.TimeoutExpired:
                continue
    except subprocess.TimeoutExpired:
        tracker_stop.set()
        if tracker_last_inventory is not None:
            last_inventory = tracker_last_inventory
        try:
            _cleanup_process_tree(
                process,
                known,
                root_identity=root_identity,
                deadline=deadline_monotonic,
                inventory_provider=observe,
                clock=clock,
                sleep=sleep,
                kernel_tracker=kernel_tracker,
                initial_inventory=last_inventory,
            )
        except BaseException as exc:
            cleanup_errors.append(exc)
        raise
    except _ContainedSignal:
        tracker_stop.set()
        if tracker_last_inventory is not None:
            last_inventory = tracker_last_inventory
        try:
            _cleanup_process_tree(
                process,
                known,
                root_identity=root_identity,
                deadline=deadline_monotonic,
                inventory_provider=observe,
                clock=clock,
                sleep=sleep,
                kernel_tracker=kernel_tracker,
                initial_inventory=last_inventory,
            )
        except BaseException as cleanup_exc:
            cleanup_errors.append(cleanup_exc)
    except BaseException:
        tracker_stop.set()
        if tracker_last_inventory is not None:
            last_inventory = tracker_last_inventory
        try:
            _cleanup_process_tree(
                process,
                known,
                root_identity=root_identity,
                deadline=deadline_monotonic,
                inventory_provider=observe,
                clock=clock,
                sleep=sleep,
                kernel_tracker=kernel_tracker,
                initial_inventory=last_inventory,
            )
        except BaseException as exc:
            cleanup_errors.append(exc)
        raise
    finally:
        primary_exception = sys.exception()
        if gate_write >= 0:
            try:
                os.close(gate_write)
            except BaseException as exc:
                cleanup_errors.append(exc)
        if tracker_thread is not None:
            try:
                stop_tracker()
            except BaseException as exc:
                cleanup_errors.append(exc)
        try:
            kernel_tracker.close()
        except BaseException as exc:
            cleanup_errors.append(exc)
        if primary_exception is not None:
            anchors_closed = close_darwin_pipe_anchors(cleanup_errors)
            _finish_signal_restoration(
                previous_handlers,
                active_signals,
                signal_latch,
                cleanup_errors,
                primary_exception=primary_exception,
                error_label="contained subprocess cleanup failures",
                replay_ready=anchors_closed,
            )
    try:
        remaining = deadline_monotonic - clock()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(list(args), 0)
        inventory = observe(deadline_monotonic)
        known.update(_discover_descendants(process.pid, inventory, known))
        alive = {
            pid: identity
            for pid, identity in known.items()
            if (
                (pid in inventory and inventory[pid].identity == identity)
                or (
                    (observed := _process_observation(pid)) is not None
                    and observed.identity == identity
                )
            )
        }
        if alive:
            _cleanup_process_tree(
                process,
                known,
                root_identity=root_identity,
                deadline=deadline_monotonic,
                inventory_provider=observe,
                clock=clock,
                sleep=sleep,
            )
            raise ContainedProcessError("subprocess descendants outlived their root")
        completed = subprocess.CompletedProcess(list(args), process.returncode, stdout, stderr)
        if check and completed.returncode != 0:
            raise subprocess.CalledProcessError(
                completed.returncode,
                list(args),
                output=stdout,
                stderr=stderr,
            )
        return completed
    finally:
        anchors_closed = close_darwin_pipe_anchors(cleanup_errors)
        _finish_signal_restoration(
            previous_handlers,
            active_signals,
            signal_latch,
            cleanup_errors,
            primary_exception=sys.exception(),
            error_label="contained subprocess cleanup failures",
            replay_ready=anchors_closed,
        )


def _contained_child_main(arguments: list[str]) -> int:
    if len(arguments) < 3 or arguments[1] != "--":
        return 127
    gate_fd = int(arguments[0])
    command = arguments[2:]
    if not command:
        return 127
    signal_byte = os.read(gate_fd, 1)
    os.close(gate_fd)
    if signal_byte != b"1":
        return 127
    os.execvpe(command[0], command, os.environ)
    return 127


if __name__ == "__main__" and len(sys.argv) >= 2 and sys.argv[1] == "--contained-child":
    raise SystemExit(_contained_child_main(sys.argv[2:]))
