from __future__ import annotations

import os
import re
from pathlib import Path


PLACEHOLDER = re.compile(rb"__[A-Z0-9_]+__")
REPLACEMENTS = (b"__APP_IMAGE__", b"__APP_IMAGE_ID__", b"__BUILD_REVISION__")


def rendered_content(
    content: bytes,
    *,
    expected_identity: str,
    app_image: str,
    app_image_id: str,
) -> tuple[bytes, set[bytes]]:
    values = {
        b"__APP_IMAGE__": app_image.encode("ascii"),
        b"__APP_IMAGE_ID__": app_image_id.encode("ascii"),
        b"__BUILD_REVISION__": expected_identity.encode("ascii"),
    }
    found = set(PLACEHOLDER.findall(content))
    if found - set(values):
        raise RuntimeError("manifest contains an unrecognized placeholder")
    rendered = content
    for placeholder, value in values.items():
        rendered = rendered.replace(placeholder, value)
    if PLACEHOLDER.search(rendered):
        raise RuntimeError("rendered manifest still contains a placeholder")
    return rendered, found


def render_manifests(
    release: Path,
    *,
    expected_identity: str,
    app_image: str,
    app_image_id: str,
) -> None:
    source = release / "deploy/k3s"
    target = release / "deploy/k3s-rendered"
    if target.exists() or target.is_symlink():
        raise RuntimeError("rendered manifest target already exists")
    target.mkdir(mode=0o755)
    found: set[bytes] = set()
    source_files = sorted(source.iterdir(), key=lambda path: path.name)
    if not source_files or any(not path.is_file() or path.is_symlink() for path in source_files):
        raise RuntimeError("manifest template tree must contain only regular files")
    for path in source_files:
        rendered, placeholders = rendered_content(
            path.read_bytes(),
            expected_identity=expected_identity,
            app_image=app_image,
            app_image_id=app_image_id,
        )
        found.update(placeholders)
        destination = target / path.name
        with destination.open("xb") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
    if found != set(REPLACEMENTS):
        raise RuntimeError("manifest templates do not contain the exact required placeholder set")


def validate_rendered_manifests(
    release: Path,
    *,
    expected_identity: str,
    app_image: str,
    app_image_id: str,
) -> None:
    templates = release / "deploy/k3s"
    rendered = release / "deploy/k3s-rendered"
    if not rendered.is_dir() or rendered.is_symlink():
        raise RuntimeError("rendered manifest directory is missing or unsafe")
    template_names = sorted(path.name for path in templates.iterdir())
    rendered_names = sorted(path.name for path in rendered.iterdir())
    if template_names != rendered_names:
        raise RuntimeError("template and rendered manifest file sets differ")
    found: set[bytes] = set()
    for name in template_names:
        template = templates / name
        actual = rendered / name
        if (
            not template.is_file()
            or template.is_symlink()
            or not actual.is_file()
            or actual.is_symlink()
        ):
            raise RuntimeError("manifest trees must contain only regular files")
        expected, placeholders = rendered_content(
            template.read_bytes(),
            expected_identity=expected_identity,
            app_image=app_image,
            app_image_id=app_image_id,
        )
        found.update(placeholders)
        if actual.read_bytes() != expected:
            raise RuntimeError("rendered manifest bytes do not match their templates")
    if found != set(REPLACEMENTS):
        raise RuntimeError("manifest templates do not contain the exact required placeholder set")
