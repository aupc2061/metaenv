from pathlib import Path

import pytest

import schemaopt_env.tasks as tasks


def test_resolve_runtime_asset_path_returns_local_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(tasks, "_REPO_ROOT", tmp_path)
    asset_path = tmp_path / "schemaopt_env" / "task_assets" / "databases" / "sample.duckdb"
    asset_path.parent.mkdir(parents=True, exist_ok=True)
    asset_path.write_bytes(b"DUCKDBDATA")

    resolved = tasks.resolve_runtime_asset_path("schemaopt_env/task_assets/databases/sample.duckdb")

    assert resolved == str(asset_path.resolve())


def test_resolve_runtime_asset_path_rejects_lfs_pointer_without_space_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(tasks, "_REPO_ROOT", tmp_path)
    monkeypatch.delenv("HF_SPACE_REPO_ID", raising=False)
    monkeypatch.delenv("SPACE_ID", raising=False)
    asset_path = tmp_path / "schemaopt_env" / "task_assets" / "databases" / "sample.duckdb"
    asset_path.parent.mkdir(parents=True, exist_ok=True)
    asset_path.write_text(
        "version https://git-lfs.github.com/spec/v1\n"
        "oid sha256:deadbeef\n"
        "size 123456\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Git LFS pointer"):
        tasks.resolve_runtime_asset_path("schemaopt_env/task_assets/databases/sample.duckdb")
