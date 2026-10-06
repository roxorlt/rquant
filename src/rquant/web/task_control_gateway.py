"""Only the original private task admission can cross the Web write boundary."""

from typing import Protocol, runtime_checkable

from rquant.task_control_commands import TaskControlIdentity, TaskControlRequest
from rquant.task_control_admission import TaskControlAdmissionResult, TaskControlCapabilities


@runtime_checkable
class TaskControlGateway(Protocol):
    def capabilities(self, *, authenticated_actor_id: str, generation_id: str) -> TaskControlCapabilities: ...
    def lookup(self, request: TaskControlRequest, *, authenticated_actor_id: str) -> TaskControlAdmissionResult | None: ...
    def resume(self, request: TaskControlRequest, *, authenticated_actor_id: str) -> TaskControlAdmissionResult: ...
    def submit(self, request: TaskControlRequest, *, authenticated_actor_id: str, verified_metadata_identity: TaskControlIdentity) -> TaskControlAdmissionResult: ...
