"""Confirmed operator publication and persistent BUY admission in the original role."""

from __future__ import annotations

import os
import stat
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from rquant.paper_operator_commands import OwnedSavePaperPortfolioConfiguration, PaperOperatorApplication, PaperOperatorControl, SavePaperPortfolioConfiguration, SetPaperAccountPaused
from rquant.paper_portfolio_state import PaperPortfolioStateStore
from rquant.runtime_contracts import normalize_aware_utc


class PaperOperatorAdmissionClosed(ValueError):
    pass


class PaperOperatorControlStore:
    def __init__(self, state: PaperPortfolioStateStore, *, root: Path, clock: Callable[[], datetime]) -> None:
        self.state = state
        self.root = Path(root).absolute()
        self.clock = clock
        self.path = self.root / f"{state.configuration.binding.role_id}.json"
        self.root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.root.mkdir(mode=0o700, exist_ok=True)
        self._directory_identity = self._directory_stat()
        with state._connection(write=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS operator_root(singleton INTEGER PRIMARY KEY CHECK(singleton=1),parent_dev INTEGER NOT NULL,parent_ino INTEGER NOT NULL,root_dev INTEGER NOT NULL,root_ino INTEGER NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS operator_command_refs(command_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,request_body TEXT NOT NULL,control_body TEXT NOT NULL,sequence INTEGER UNIQUE NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS operator_publication(sequence INTEGER PRIMARY KEY,digest TEXT NOT NULL,file_dev INTEGER NOT NULL,file_ino INTEGER NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS operator_application(singleton INTEGER PRIMARY KEY CHECK(singleton=1),body TEXT NOT NULL)")
            old = connection.execute("SELECT parent_dev,parent_ino,root_dev,root_ino FROM operator_root WHERE singleton=1").fetchone()
            if old is None:
                connection.execute("INSERT INTO operator_root VALUES(1,?,?,?,?)", self._directory_identity)
            elif tuple(old) != self._directory_identity:
                raise ValueError("operator control directory was replaced")

    def _directory_stat(self) -> tuple[int, int, int, int]:
        identities = []
        for path in (self.root.parent, self.root):
            item = path.lstat()
            if not stat.S_ISDIR(item.st_mode) or item.st_uid != os.getuid() or item.st_mode & 0o077:
                raise ValueError("operator source requires private owned directories")
            identities.extend((item.st_dev, item.st_ino))
        return tuple(identities)

    def _require_directory(self) -> None:
        if self._directory_stat() != self._directory_identity:
            raise ValueError("operator control source directory was replaced")

    def _require_actor(self, actor_id: str) -> None:
        if actor_id != self.state.configuration.binding.owner_id:
            raise PermissionError("paper account belongs to a different user")

    def _lookup(self, connection: sqlite3.Connection, request: SetPaperAccountPaused, actor_id: str) -> PaperOperatorControl | None:
        row = connection.execute("SELECT * FROM operator_command_refs WHERE command_id=?", (request.command_id,)).fetchone()
        if row is None:
            return None
        if row["owner_id"] != actor_id:
            raise PermissionError("original paper command belongs to a different user")
        if SetPaperAccountPaused.model_validate_json(row["request_body"]) != request:
            raise ValueError("original paper command body differs")
        return PaperOperatorControl.model_validate_json(row["control_body"])

    def lookup(self, request: SetPaperAccountPaused, *, authenticated_actor_id: str) -> PaperOperatorControl | None:
        request = SetPaperAccountPaused.model_validate(request.model_dump(mode="python"))
        self._require_actor(authenticated_actor_id)
        with self.state._connection() as connection:
            return self._lookup(connection, request, authenticated_actor_id)

    def commit_confirmed_control(self, request: SetPaperAccountPaused, *, authenticated_actor_id: str,
                                 original_command_id: str) -> PaperOperatorControl:
        request = SetPaperAccountPaused.model_validate(request.model_dump(mode="python"))
        self._require_actor(authenticated_actor_id)
        if original_command_id != request.command_id:
            raise ValueError("operator effect must reference the original PageControl command")
        with self.state._connection(write=True) as connection:
            old = self._lookup(connection, request, authenticated_actor_id)
            if old is not None:
                return old
            configuration = self.state.configuration
            head = connection.execute("SELECT current_config FROM metadata WHERE singleton=1").fetchone()[0]
            if request.account_id != configuration.binding.account_id or request.configuration_fingerprint != head or head != configuration.fingerprint:
                raise ValueError("paper operator account or configuration changed")
            previous = self._head(connection)
            sequence = previous.sequence if previous else 0
            paused = previous.paused if previous else True
            if (request.expected_sequence, request.expected_paused) != (sequence, paused):
                raise ValueError("paper operator control predecessor changed")
            control = PaperOperatorControl(binding=configuration.binding, configuration_fingerprint=head,
                                           configuration_version=configuration.version, state_instance_id=self.state.instance_id,
                                           sequence=sequence + 1, original_command_id=original_command_id,
                                           original_request_fingerprint=request.fingerprint,
                                           issued_at=normalize_aware_utc(self.clock()), paused=request.paused)
            connection.execute("INSERT INTO operator_command_refs VALUES(?,?,?,?,?)",
                               (request.command_id, authenticated_actor_id, request.model_dump_json(), control.model_dump_json(), control.sequence))
            return control

    def _commit_configuration_control(self, connection: sqlite3.Connection,
                                      command: OwnedSavePaperPortfolioConfiguration) -> PaperOperatorControl:
        # This row and the new configuration are committed by the original save effect in one transaction.
        self._require_actor(command.owner_id)
        previous = self._head(connection)
        paused = True
        try:
            verified = self._verified(connection)
            paused = verified.paused
        except (OSError, ValueError):
            pass
        configuration = command.configuration
        control = PaperOperatorControl(binding=configuration.binding, configuration_fingerprint=configuration.fingerprint,
                                       configuration_version=configuration.version, state_instance_id=self.state.instance_id,
                                       sequence=(previous.sequence if previous else 0) + 1, original_command_id=command.command_id,
                                       original_request_fingerprint=command.original().fingerprint,
                                       issued_at=command.accepted_at, paused=paused)
        connection.execute("INSERT INTO operator_command_refs VALUES(?,?,?,?,?)",
                           (command.command_id, command.owner_id, command.original().model_dump_json(), control.model_dump_json(), control.sequence))
        return control

    def configuration_control(self, command: OwnedSavePaperPortfolioConfiguration) -> PaperOperatorControl:
        self._require_actor(command.owner_id)
        with self.state._connection() as connection:
            row = connection.execute("SELECT owner_id,request_body,control_body FROM operator_command_refs WHERE command_id=?", (command.command_id,)).fetchone()
            if (row is None or row["owner_id"] != command.owner_id
                    or SavePaperPortfolioConfiguration.model_validate_json(row["request_body"]) != command.original()):
                raise ValueError("saved configuration lacks its original control continuation")
            control = PaperOperatorControl.model_validate_json(row["control_body"])
            if (control.configuration_fingerprint != command.configuration.fingerprint
                    or control.original_request_fingerprint != command.original().fingerprint):
                raise ValueError("saved configuration control differs from its accepted original")
            return control

    @staticmethod
    def _head(connection: sqlite3.Connection) -> PaperOperatorControl | None:
        row = connection.execute("SELECT control_body FROM operator_command_refs ORDER BY sequence DESC LIMIT 1").fetchone()
        return PaperOperatorControl.model_validate_json(row[0]) if row else None

    def _read_file(self) -> tuple[PaperOperatorControl, tuple[int, int]]:
        self._require_directory()
        descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            item = os.fstat(descriptor)
            if not stat.S_ISREG(item.st_mode) or item.st_uid != os.getuid() or item.st_mode & 0o077 or item.st_nlink != 1:
                raise ValueError("operator file is not a private regular source")
            if item.st_size > 16384:
                raise ValueError("operator file exceeds its fixed byte budget")
            payload = os.read(descriptor, 16385)
            after = os.fstat(descriptor)
            if len(payload) > 16384 or (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) != (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_ctime_ns):
                raise ValueError("operator file changed during read")
            control = PaperOperatorControl.model_validate_json(payload)
            named = self.path.lstat()
            if (named.st_dev, named.st_ino) != (item.st_dev, item.st_ino):
                raise ValueError("operator file was replaced during read")
            self._require_directory()
            return control, (item.st_dev, item.st_ino)
        finally:
            os.close(descriptor)

    def _after_file_replace(self) -> None:
        """Interruption boundary before recording a verified publication."""

    def publish(self, control: PaperOperatorControl) -> None:
        control = PaperOperatorControl.model_validate(control.model_dump(mode="python"))
        self._require_directory()
        with self.state._connection(write=True) as connection:
            row = connection.execute("SELECT control_body FROM operator_command_refs WHERE command_id=?", (control.original_command_id,)).fetchone()
            if row is None or PaperOperatorControl.model_validate_json(row[0]) != control:
                raise ValueError("operator body lacks its original confirmed command")
            head = self._head(connection)
            if head != control:
                # A completed old retry cannot roll back a newer control.
                return
            temporary = self.root / f".{self.path.name}.{uuid4().hex}.tmp"
            descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            try:
                payload = control.model_dump_json().encode()
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                self._require_directory()
                os.replace(temporary, self.path)
                directory_descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    os.fsync(directory_descriptor)
                finally:
                    os.close(directory_descriptor)
                self._after_file_replace()
                actual, identity = self._read_file()
                if actual != control:
                    raise ValueError("operator publication differs from the confirmed body")
                connection.execute("INSERT OR REPLACE INTO operator_publication VALUES(?,?,?,?)", (control.sequence, control.fingerprint, *identity))
            finally:
                temporary.unlink(missing_ok=True)

    def _application(self, connection: sqlite3.Connection) -> PaperOperatorApplication:
        row = connection.execute("SELECT body FROM operator_application WHERE singleton=1").fetchone()
        return PaperOperatorApplication.model_validate_json(row[0]) if row else PaperOperatorApplication(
            account_id=self.state.configuration.binding.account_id,
            configuration_fingerprint=self.state.configuration.fingerprint, sequence=0, status="unavailable", reason="尚未应用控制")

    def _verified(self, connection: sqlite3.Connection) -> PaperOperatorControl:
        control, identity = self._read_file()
        configuration = self.state.configuration
        if (control != self._head(connection) or control.binding != configuration.binding
                or control.configuration_fingerprint != configuration.fingerprint
                or control.configuration_version != configuration.version or control.state_instance_id != self.state.instance_id):
            raise ValueError("operator control does not bind the current role/account/configuration")
        row = connection.execute("SELECT digest,file_dev,file_ino FROM operator_publication WHERE sequence=?", (control.sequence,)).fetchone()
        if row is None or tuple(row) != (control.fingerprint, *identity):
            raise ValueError("operator publication has no durable verified source receipt")
        previous = self._application(connection)
        if control.sequence < previous.sequence or (control.sequence == previous.sequence and previous.control_fingerprint != control.fingerprint):
            raise ValueError("operator sequence cannot roll back or change its body")
        return control

    def current(self) -> PaperOperatorApplication:
        with self.state._connection() as connection:
            previous = self._application(connection)
            head = self._head(connection)
            if head is not None and head.sequence > previous.sequence:
                return previous.model_copy(update={"paused": True, "status": "waiting", "reason": "等待应用"})
            try:
                self._verified(connection)
            except (OSError, ValueError):
                return previous.model_copy(update={"paused": True, "status": "unavailable", "reason": "控制不可用"})
            return previous

    def _apply(self, connection: sqlite3.Connection, observed_at: datetime) -> PaperOperatorApplication:
        previous = self._application(connection)
        try:
            control = self._verified(connection)
        except (OSError, ValueError):
            result = previous.model_copy(update={"paused": True, "status": "unavailable", "observed_at": observed_at, "reason": "控制不可用"})
        else:
            result = PaperOperatorApplication(account_id=control.binding.account_id,
                                              configuration_fingerprint=control.configuration_fingerprint,
                                              sequence=control.sequence, control_fingerprint=control.fingerprint,
                                              paused=control.paused, status="applied", observed_at=observed_at)
        connection.execute("INSERT OR REPLACE INTO operator_application VALUES(1,?)", (result.model_dump_json(),))
        return result

    def apply(self, *, observed_at: datetime) -> PaperOperatorApplication:
        with self.state._connection(write=True) as connection:
            return self._apply(connection, normalize_aware_utc(observed_at))

    @contextmanager
    def buy_admission(self, *, observed_at: datetime) -> Iterator[PaperOperatorApplication]:
        # The durable state transaction serializes gate application and final BUY submit.
        with self.state._connection(write=True) as connection:
            result = self._apply(connection, normalize_aware_utc(observed_at))
            if result.status != "applied" or result.paused:
                connection.commit()
                raise PaperOperatorAdmissionClosed("已暂停开新仓，信号不补买")
            yield result
