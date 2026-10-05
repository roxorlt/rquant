"""Read verified portfolio bundles and export their exact sealed HTML bytes."""

from __future__ import annotations

import hashlib
import io
import os
import stat
import zipfile
from contextlib import suppress
from pathlib import Path
from uuid import UUID, uuid4
from typing import TYPE_CHECKING

from pydantic import Field

from rquant.lab_artifact_export import LabJobZipExportFacade, LabJobZipExportReceipt
from rquant.lab_artifact_preview import (
    ArtifactPreviewIntegrityError,
    ArtifactPreviewReader,
    ArtifactPreviewUnavailableError,
)
from rquant.lab_artifacts import LabArtifactIntegrityError, _rename_noreplace, _sha256_descriptor
from rquant.lab_jobs import LabJobReader
from rquant.portfolio_backtest_adapter import PortfolioBacktestParameters
from rquant.portfolio_backtest_models import (
    MAX_BUNDLE_BYTES,
    MAX_ZIP_BYTES,
    PORTFOLIO_TABLE_NAMES,
    PortfolioBundle,
)
from rquant.runtime_contracts import RuntimeContractModel

if TYPE_CHECKING:
    from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority


def is_private_portfolio_job(job: object) -> bool:
    spec = getattr(job, "spec", None)
    experiment = getattr(spec, "experiment", None)
    identity = getattr(experiment, "spec", None)
    return isinstance(
        getattr(identity, "hypothesis_family", None), str
    ) and identity.hypothesis_family.startswith(("experiment-search:", "experiment-outer:"))


class PortfolioReadResult(RuntimeContractModel):
    job_id: UUID
    spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    bundle: PortfolioBundle

    def html_bytes(self) -> bytes:
        if self.bundle.html is None:
            raise ArtifactPreviewUnavailableError("complete sealed HTML is unavailable")
        return self.bundle.html.encode("utf-8")


class PortfolioResultReader:
    def __init__(self, *, reader: LabJobReader, artifact_root: Path) -> None:
        self.reader = reader
        self.previews = ArtifactPreviewReader(
            reader=reader,
            artifact_root=artifact_root,
            max_preview_rows=1,
            max_preview_columns=1,
            max_preview_cell_bytes=MAX_BUNDLE_BYTES,
            max_preview_arrow_bytes=MAX_BUNDLE_BYTES + 1024,
            max_preview_serialized_bytes=2 * MAX_BUNDLE_BYTES + 1024 * 1024,
        )
        self.views = ArtifactPreviewReader(
            reader=reader,
            artifact_root=artifact_root,
            max_preview_rows=80_000,
            max_preview_columns=1,
            max_preview_cell_bytes=MAX_BUNDLE_BYTES,
            max_preview_arrow_bytes=MAX_BUNDLE_BYTES,
            max_preview_serialized_bytes=2 * MAX_BUNDLE_BYTES + 1024 * 1024,
        )

    @staticmethod
    def _private_guard(
        job: object, *, owner: str | None, authority: ExperimentPrivateResultAuthority | None
    ) -> None:
        if not is_private_portfolio_job(job):
            return
        from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority

        if owner is None or not isinstance(authority, ExperimentPrivateResultAuthority):
            raise PermissionError("private portfolio result requires exact owner authority")
        authority.authorize(job, owner)

    def read(
        self,
        job_id: UUID,
        *,
        expected_result_hash: str | None = None,
        private_owner: str | None = None,
        private_authority: ExperimentPrivateResultAuthority | None = None,
    ) -> PortfolioReadResult:
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None or authority.job.spec.parameters.strategy_name != "portfolio_backtest":
            raise ArtifactPreviewUnavailableError("portfolio sealed result is unavailable")
        self._private_guard(authority.job, owner=private_owner, authority=private_authority)
        if (
            expected_result_hash is not None
            and authority.evidence.complete_result_hash != expected_result_hash
        ):
            raise ValueError("portfolio result identity changed")
        preview = self.previews.preview(
            job_id, table_name="portfolio_bundle", row_limit=1, column_limit=1
        )
        table = preview.table
        if (
            set(preview.available_tables) != set(PORTFOLIO_TABLE_NAMES)
            or table is None
            or (
                table.total_rows != 1
                or table.columns != ("payload",)
                or table.total_columns != 1
                or table.rows_truncated
                or table.columns_truncated
                or len(table.rows) != 1
                or not isinstance(table.rows[0][0], str)
            )
        ):
            raise ArtifactPreviewIntegrityError("portfolio bundle schema or inventory differs")
        payload = table.rows[0][0]
        if len(payload.encode()) > MAX_BUNDLE_BYTES:
            raise ArtifactPreviewIntegrityError("portfolio bundle exceeds its byte budget")
        bundle = PortfolioBundle.model_validate_json(payload)
        spec = authority.job.spec
        parameters = PortfolioBacktestParameters.model_validate(
            {item.name: item.value for item in spec.parameters.arguments}
        )
        if parameters != PortfolioBacktestParameters.from_frozen(bundle.frozen) or (
            bundle.frozen.request.producer_commit,
            bundle.frozen.config.start_date,
            bundle.frozen.config.end_date,
            bundle.frozen.request.execution_cost_spec,
        ) != (
            spec.code_sha,
            spec.parameters.start_date,
            spec.parameters.end_date,
            spec.execution_costs,
        ):
            raise ArtifactPreviewIntegrityError("portfolio bundle conflicts with the admitted task")
        after = self.reader.get_artifact_preview_authority(job_id)
        if after is not None:
            self._private_guard(after.job, owner=private_owner, authority=private_authority)
        if (
            after != authority
            or preview.complete_result_hash != authority.evidence.complete_result_hash
            or preview.manifest_hash != authority.evidence.manifest_hash
        ):
            raise ValueError("portfolio result identity changed while reading")
        return PortfolioReadResult(
            job_id=job_id,
            spec_hash=preview.spec_hash,
            manifest_hash=preview.manifest_hash,
            result_hash=preview.complete_result_hash,
            bundle=bundle,
        )

    def read_view(
        self, job_id: UUID, *, table_name: str, expected_result_hash: str, offset: int, limit: int
    ) -> PortfolioViewReadResult:
        if (
            table_name not in PORTFOLIO_TABLE_NAMES[1:]
            or not 0 <= offset < 80_000
            or not 1 <= limit <= 1830
            or offset + limit > 80_000
        ):
            raise ValueError("portfolio view bounds are invalid")
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None:
            raise ArtifactPreviewUnavailableError("portfolio sealed result is unavailable")
        self._private_guard(authority.job, owner=None, authority=None)
        preview = self.views.preview(
            job_id, table_name=table_name, row_limit=offset + limit, column_limit=1
        )
        table = preview.table
        if preview.complete_result_hash != expected_result_hash:
            raise ValueError("portfolio result identity changed")
        if (
            table is None
            or table.columns != ("payload",)
            or table.total_columns != 1
            or table.columns_truncated
        ):
            raise ArtifactPreviewIntegrityError("portfolio view schema differs")
        if any(len(row) != 1 or not isinstance(row[0], str) for row in table.rows):
            raise ArtifactPreviewIntegrityError("portfolio view cells differ")
        return PortfolioViewReadResult(
            result_hash=preview.complete_result_hash,
            payloads=tuple(row[0] for row in table.rows[offset : offset + limit]),
            total_rows=table.total_rows,
        )


class PortfolioZipReceipt(LabJobZipExportReceipt):
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    bundle_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    html_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class PortfolioViewReadResult(RuntimeContractModel):
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    payloads: tuple[str, ...]
    total_rows: int = Field(ge=0)


class _BudgetedZipWriter:
    def __init__(self, file: io.BufferedRandom, limit: int) -> None:
        self.file, self.limit = file, limit

    def write(self, data: bytes) -> int:
        if self.file.tell() + len(data) > self.limit:
            raise ValueError("portfolio ZIP exceeds byte budget")
        return self.file.write(data)

    def tell(self) -> int:
        return self.file.tell()

    def seek(self, offset: int, whence: int = 0) -> int:
        return self.file.seek(offset, whence)

    def flush(self) -> None:
        self.file.flush()


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


class PortfolioZipExportFacade(LabJobZipExportFacade):
    def __init__(
        self,
        *,
        result_reader: PortfolioResultReader,
        original_exports: LabJobZipExportFacade,
        max_zip_bytes: int = MAX_ZIP_BYTES,
        **kwargs: object,
    ) -> None:
        if type(max_zip_bytes) is not int or not 1 <= max_zip_bytes <= MAX_ZIP_BYTES:
            raise ValueError("portfolio ZIP budget is invalid")
        super().__init__(**kwargs)
        self.result_reader, self.original_exports, self.max_zip_bytes = (
            result_reader,
            original_exports,
            max_zip_bytes,
        )

    def export_portfolio(
        self, job_id: UUID, *, expected_result_hash: str, request_id: UUID | None = None
    ) -> PortfolioZipReceipt:
        if request_id is not None:
            recovered = self.recover_portfolio(
                job_id, request_id=request_id, expected_result_hash=expected_result_hash
            )
            if recovered is not None:
                return recovered
        result = self.result_reader.read(job_id, expected_result_hash=expected_result_hash)
        html = result.html_bytes()
        original = self.original_exports.export(job_id)
        if original.byte_size > MAX_ZIP_BYTES:
            raise ValueError("portfolio original ZIP exceeds byte budget")
        request_id = request_id or uuid4()
        descriptors: list[int] = []
        request_fd: int | None = None
        created_temporary: tuple[int, int] | None = None
        try:
            original_root = self.original_exports._open_bound_export_root()
            descriptors.append(original_root)
            original_job = self.original_exports._open_private_child(
                original_root, original.job_id.hex, label="source job directory"
            )
            descriptors.append(original_job)
            original_request = self.original_exports._open_private_child(
                original_job, original.request_id.hex, label="source request directory"
            )
            descriptors.append(original_request)
            source_fd = os.open("result.zip", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=original_request)
            descriptors.append(source_fd)
            source_before = os.fstat(source_fd)
            if (
                not stat.S_ISREG(source_before.st_mode)
                or source_before.st_nlink != 1
                or stat.S_IMODE(source_before.st_mode) != 0o600
                or source_before.st_size != original.byte_size
                or _sha256_descriptor(source_fd) != original.sha256
            ):
                raise LabArtifactIntegrityError("source ZIP identity differs from receipt")
            with self._locked_export_root() as root_fd:
                self._enforce_record_budget(root_fd)
                job_fd = self._open_private_child(
                    root_fd, job_id.hex, label="portfolio job directory", create=True
                )
                descriptors.append(job_fd)
                request_fd = self._open_private_child(
                    job_fd, request_id.hex, label="portfolio request directory", create=True
                )
                descriptors.append(request_fd)
                self._discard_interrupted_temporary(request_fd)
                output_fd = os.open(
                    "result.tmp",
                    os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=request_fd,
                )
                output_identity = os.fstat(output_fd)
                created_temporary = (output_identity.st_dev, output_identity.st_ino)
                with (
                    os.fdopen(output_fd, "w+b") as output,
                    os.fdopen(os.dup(source_fd), "rb") as source,
                ):
                    source.seek(0)
                    with (
                        zipfile.ZipFile(source) as old,
                        zipfile.ZipFile(
                            _BudgetedZipWriter(output, self.max_zip_bytes),
                            "w",
                            compression=zipfile.ZIP_DEFLATED,
                            compresslevel=9,
                        ) as new,
                    ):
                        entries = old.infolist()
                        names = [entry.filename for entry in entries]
                        allowed = {
                            "manifest.json",
                            "SHA256SUMS",
                            "spec.json",
                            "metrics.json",
                            "report.md",
                            *(f"tables/{name}.parquet" for name in PORTFOLIO_TABLE_NAMES),
                        }
                        if (
                            set(names) != allowed
                            or len(names) != len(allowed)
                            or any(entry.flag_bits & 1 for entry in entries)
                        ):
                            raise LabArtifactIntegrityError("source ZIP inventory differs")
                        if sum(entry.file_size for entry in entries) > 256 * 1024 * 1024:
                            raise ValueError("portfolio ZIP uncompressed byte budget exceeded")
                        by_name = {entry.filename: entry for entry in entries}
                        for name in sorted([*names, "report.html"]):
                            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
                            info.compress_type = zipfile.ZIP_DEFLATED
                            info.external_attr = 0o100400 << 16
                            if name == "report.html":
                                new.writestr(info, html, compresslevel=9)
                            else:
                                with (
                                    old.open(by_name[name]) as entry,
                                    new.open(info, "w") as destination,
                                ):
                                    copied = 0
                                    while chunk := entry.read(64 * 1024):
                                        copied += len(chunk)
                                        if copied > by_name[name].file_size:
                                            raise LabArtifactIntegrityError(
                                                "source ZIP entry length changed"
                                            )
                                        destination.write(chunk)
                                    if copied != by_name[name].file_size:
                                        raise LabArtifactIntegrityError(
                                            "source ZIP entry is truncated"
                                        )
                    output.flush()
                    os.fsync(output.fileno())
                source_after = os.fstat(source_fd)
                at_path = os.stat("result.zip", dir_fd=original_request, follow_symlinks=False)
                if (
                    _identity(source_after) != _identity(source_before)
                    or _identity(at_path) != _identity(source_before)
                    or _sha256_descriptor(source_fd) != original.sha256
                ):
                    raise LabArtifactIntegrityError("source ZIP changed during export")
                if (
                    self.result_reader.read(job_id, expected_result_hash=expected_result_hash)
                    != result
                ):
                    raise LabArtifactIntegrityError("sealed portfolio changed during export")
                _rename_noreplace(request_fd, "result.tmp", request_fd, "result.zip")
                os.fsync(request_fd)
                path = self.export_root / job_id.hex / request_id.hex / "result.zip"
                receipt = self._build_receipt(request_id=request_id, job_id=job_id, path=path)
                return PortfolioZipReceipt(
                    **receipt.model_dump(mode="python"),
                    result_hash=result.result_hash,
                    bundle_hash=result.bundle.bundle_hash,
                    html_sha256=result.bundle.html_sha256,
                )
        except BaseException:
            if request_fd is not None and created_temporary is not None:
                with suppress(FileNotFoundError):
                    current = os.stat("result.tmp", dir_fd=request_fd, follow_symlinks=False)
                    if (
                        (current.st_dev, current.st_ino) == created_temporary
                        and stat.S_ISREG(current.st_mode)
                        and current.st_nlink == 1
                    ):
                        os.unlink("result.tmp", dir_fd=request_fd)
            raise
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    def _discard_interrupted_temporary(self, request_fd: int) -> None:
        # The original journal fixes this request slot before publication. Under
        # the export lock only its private regular temporary may be rebuilt.
        try:
            descriptor = os.open("result.tmp", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=request_fd)
        except FileNotFoundError:
            return
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or before.st_uid != os.geteuid()
                or stat.S_IMODE(before.st_mode) != 0o600
                or before.st_size > self.max_zip_bytes
            ):
                raise LabArtifactIntegrityError("interrupted portfolio temporary is unsafe")
            current = os.stat("result.tmp", dir_fd=request_fd, follow_symlinks=False)
            if _identity(current) != _identity(before):
                raise LabArtifactIntegrityError("interrupted portfolio temporary changed")
            os.unlink("result.tmp", dir_fd=request_fd)
            os.fsync(request_fd)
        finally:
            os.close(descriptor)

    def recover_portfolio(
        self, job_id: UUID, *, request_id: UUID, expected_result_hash: str
    ) -> PortfolioZipReceipt | None:
        result = self.result_reader.read(job_id, expected_result_hash=expected_result_hash)
        html = result.html_bytes()
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None or authority.evidence.complete_result_hash != result.result_hash:
            raise LabArtifactIntegrityError("portfolio recovery result changed")
        sealed = self.artifact_store.verify_sealed(authority.evidence.sealed_path)
        if (sealed.manifest_hash, sealed.manifest.complete_result_hash, sealed.file_identities) != (
            authority.evidence.manifest_hash,
            authority.evidence.complete_result_hash,
            authority.evidence.file_identities,
        ):
            raise LabArtifactIntegrityError("portfolio recovery artifact identity changed")
        expected_hashes = self.artifact_store._expected_bound_hashes(sealed.manifest) | {
            "report.html": hashlib.sha256(html).hexdigest()
        }
        descriptors: list[int] = []
        try:
            root_fd = self._open_bound_export_root()
            descriptors.append(root_fd)
            try:
                job_fd = self._open_private_child(
                    root_fd, job_id.hex, label="portfolio recovery job"
                )
                descriptors.append(job_fd)
                request_fd = self._open_private_child(
                    job_fd, request_id.hex, label="portfolio recovery request"
                )
                descriptors.append(request_fd)
                fd = os.open("result.zip", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=request_fd)
                descriptors.append(fd)
            except FileNotFoundError:
                return None
            before = os.fstat(fd)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or stat.S_IMODE(before.st_mode) != 0o600
                or before.st_size > self.max_zip_bytes
            ):
                raise LabArtifactIntegrityError("portfolio recovery ZIP is invalid")
            with os.fdopen(os.dup(fd), "rb") as source, zipfile.ZipFile(source) as archive:
                names = archive.namelist()
                if len(names) != len(expected_hashes) or set(names) != set(expected_hashes):
                    raise LabArtifactIntegrityError("portfolio recovery ZIP inventory differs")
                if sum(info.file_size for info in archive.infolist()) > 256 * 1024 * 1024 + len(
                    html
                ):
                    raise ValueError("portfolio recovery ZIP uncompressed budget exceeded")
                for name in names:
                    digest = hashlib.sha256()
                    with archive.open(name) as entry:
                        while chunk := entry.read(64 * 1024):
                            digest.update(chunk)
                    if digest.hexdigest() != expected_hashes[name]:
                        raise LabArtifactIntegrityError("portfolio recovery ZIP content differs")
            if _identity(os.fstat(fd)) != _identity(before) or _identity(
                os.stat("result.zip", dir_fd=request_fd, follow_symlinks=False)
            ) != _identity(before):
                raise LabArtifactIntegrityError("portfolio recovery ZIP changed")
            if self.result_reader.read(job_id, expected_result_hash=expected_result_hash) != result:
                raise LabArtifactIntegrityError("portfolio recovery result changed")
            path = self.export_root / job_id.hex / request_id.hex / "result.zip"
            receipt = self._build_receipt(job_id=job_id, request_id=request_id, path=path)
            return PortfolioZipReceipt(
                **receipt.model_dump(mode="python"),
                result_hash=result.result_hash,
                bundle_hash=result.bundle.bundle_hash,
                html_sha256=result.bundle.html_sha256,
            )
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    def read_bytes(self, receipt: PortfolioZipReceipt) -> bytes:
        checked = self._build_receipt(
            request_id=receipt.request_id, job_id=receipt.job_id, path=receipt.path
        )
        if (checked.sha256, checked.byte_size) != (
            receipt.sha256,
            receipt.byte_size,
        ) or receipt.byte_size > self.max_zip_bytes:
            raise LabArtifactIntegrityError("portfolio ZIP receipt differs")
        result = self.result_reader.read(receipt.job_id, expected_result_hash=receipt.result_hash)
        if (result.bundle.bundle_hash, result.bundle.html_sha256) != (
            receipt.bundle_hash,
            receipt.html_sha256,
        ):
            raise LabArtifactIntegrityError("portfolio ZIP result binding differs")
        descriptors: list[int] = []
        try:
            root_fd = self._open_bound_export_root()
            descriptors.append(root_fd)
            job_fd = self._open_private_child(
                root_fd, receipt.job_id.hex, label="portfolio download job"
            )
            descriptors.append(job_fd)
            request_fd = self._open_private_child(
                job_fd, receipt.request_id.hex, label="portfolio download request"
            )
            descriptors.append(request_fd)
            file_fd = os.open("result.zip", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=request_fd)
            descriptors.append(file_fd)
            before = os.fstat(file_fd)
            chunks: list[bytes] = []
            length = 0
            while chunk := os.read(file_fd, 64 * 1024):
                length += len(chunk)
                if length > self.max_zip_bytes:
                    raise ValueError("portfolio ZIP download exceeds byte budget")
                chunks.append(chunk)
            payload = b"".join(chunks)
            if (
                _identity(os.fstat(file_fd)) != _identity(before)
                or _identity(os.stat("result.zip", dir_fd=request_fd, follow_symlinks=False))
                != _identity(before)
                or len(payload) != receipt.byte_size
                or hashlib.sha256(payload).hexdigest() != receipt.sha256
            ):
                raise LabArtifactIntegrityError("portfolio ZIP changed while downloading")
            return payload
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)
