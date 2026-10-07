"""Point documentation links that leave the docs tree at the Git repository.

GitHub Pages publishes only the built site. A relative link such as
``../../grafana/kafka-throughput-lag.json`` would otherwise open on
``*.github.io``, where that file does not exist. Links that stay inside
``docs/`` (other guide pages, and files MkDocs copies into the site) are
left unchanged so the book still navigates in-site.
"""

from __future__ import annotations

import os
import re
from urllib.parse import quote

_LINK = re.compile(r"(\]\()([^)\s]+)(\))")
_FENCE = re.compile(r"(^```.*?^```\s*$)", re.M | re.S)


def _branch(config) -> str:
    edit_uri = (getattr(config, "edit_uri", None) or "").strip("/")
    parts = edit_uri.split("/")
    if len(parts) >= 2 and parts[0] == "edit" and parts[1]:
        return parts[1]
    return "main"


def _inside(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([path, root]) == root
    except ValueError:
        return False


def _rewrite(markdown: str, src_path: str, docs_root: str, project_root: str, repo: str, branch: str) -> str:
    src_dir = os.path.dirname(os.path.join(docs_root, src_path))

    def replace(match: re.Match[str]) -> str:
        target = match.group(2)
        if target.startswith(("#", "mailto:", "http://", "https://")):
            return match.group(0)
        path, sep, frag = target.partition("#")
        if not path:
            return match.group(0)
        absolute = os.path.normpath(os.path.join(src_dir, path))
        if _inside(absolute, docs_root):
            return match.group(0)
        if not _inside(absolute, project_root):
            return match.group(0)
        rel = os.path.relpath(absolute, project_root).replace(os.sep, "/")
        kind = "tree" if os.path.isdir(absolute) else "blob"
        url = f"{repo}/{kind}/{branch}/{quote(rel, safe='/')}"
        if sep and frag:
            url = f"{url}#{frag}"
        return f"{match.group(1)}{url}{match.group(3)}"

    pieces = []
    for part in _FENCE.split(markdown):
        if part.startswith("```"):
            pieces.append(part)
        else:
            pieces.append(_LINK.sub(replace, part))
    return "".join(pieces)


def on_page_markdown(markdown, page, config, files):  # noqa: ARG001
    repo = (getattr(config, "repo_url", None) or "").rstrip("/")
    if not repo:
        return markdown
    docs_root = os.path.abspath(config.docs_dir)
    project_root = os.path.dirname(docs_root)
    return _rewrite(
        markdown,
        page.file.src_path,
        docs_root,
        project_root,
        repo,
        _branch(config),
    )
