#!/usr/bin/env python3
#
# Copyright 2026 The OpenNMS Group, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Created by Ronny Trommer <ronny@opennms.com>
#
# verify-docs-attributes.py — fail when a documentation listing block
# references a document attribute without enabling substitution.
#
# AsciiDoc applies only `specialcharacters` and `callouts` inside a delimited
# listing block, so `{project-repo-url}` survives to the rendered page
# verbatim. A published command that renders an unresolved attribute is not
# documentation of a command; it is a template the reader cannot complete.
# The install snippet shipped that way and produced
# `curl: (3) URL rejected: Bad hostname` on copy-paste (#152).
#
# The correct idiom was already used in two other blocks in the same source
# tree. Review did not catch the one that missed it, so this does.
#
# Two deliberate narrowings keep the check usable:
#
#   - Only KNOWN attributes count. The docs legitimately contain {datname},
#     {name} and {spcname} in PostgreSQL examples. Matching every {word}
#     would flag those and the check would be switched off within a week.
#   - Admonition blocks ([NOTE], [TIP], ...) are exempt: they substitute
#     attributes by default and are not listing blocks.
#
# Read-only: parses .adoc sources, writes nothing.
#
# Usage:
#   verify-docs-attributes.py [repo-root]

import pathlib
import re
import sys

# Block delimiters that disable attribute substitution by default.
LISTING_DELIM = re.compile(r"^(-{4,}|\.{4,}|/{4,})\s*$")
# Any delimiter, so an admonition/example block is consumed rather than
# leaving the scanner mid-block.
ANY_DELIM = re.compile(r"^(-{4,}|\.{4,}|/{4,}|={4,}|\*{4,}|_{4,}|\+{4,})\s*$")
ATTR_DECL = re.compile(r"^:([a-zA-Z][\w-]*):", re.MULTILINE)
ATTR_REF = re.compile(r"(?<!\\)\{([a-zA-Z][\w-]*)\}")
SUBS_OK = re.compile(r"subs\s*=\s*[\"']?[^\]]*\b(attributes|normal)\b")

# Supplied by the asciidoctor-maven-plugin <attributes> block in docs/pom.xml
# rather than declared in the .adoc sources.
POM_SUPPLIED = {"revnumber", "revdate", "project-version", "toc-title"}


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else
                        pathlib.Path(__file__).resolve().parents[2])
    docs = root / "docs" / "src" / "docs" / "asciidoc"
    if not docs.is_dir():
        print(f"verify-docs-attributes: no such directory: {docs}", file=sys.stderr)
        return 2

    sources = sorted(docs.rglob("*.adoc"))
    if not sources:
        print(f"verify-docs-attributes: no .adoc sources under {docs}", file=sys.stderr)
        return 2

    known = set(POM_SUPPLIED)
    for path in sources:
        known |= set(ATTR_DECL.findall(path.read_text(encoding="utf-8")))

    findings = []
    for path in sources:
        lines = path.read_text(encoding="utf-8").split("\n")
        idx = 0
        while idx < len(lines):
            line = lines[idx]
            if not ANY_DELIM.match(line):
                idx += 1
                continue

            delim = line.strip()
            header = lines[idx - 1] if idx else ""
            start = idx
            body = []
            idx += 1
            while idx < len(lines) and lines[idx].strip() != delim:
                body.append(lines[idx])
                idx += 1
            idx += 1  # consume the closing delimiter

            # Only listing/literal blocks suppress substitution.
            if not LISTING_DELIM.match(delim):
                continue
            if SUBS_OK.search(header):
                continue

            refs = sorted({m for m in ATTR_REF.findall("\n".join(body)) if m in known})
            if refs:
                findings.append((path.relative_to(root), start + 1, header.strip(), refs))

    if findings:
        print("verify-docs-attributes: listing blocks reference attributes that will NOT resolve\n",
              file=sys.stderr)
        for path, line, header, refs in findings:
            print(f"  {path}:{line}", file=sys.stderr)
            print(f"    header     : {header or '(none)'}", file=sys.stderr)
            print(f"    unresolved : {', '.join('{' + r + '}' for r in refs)}", file=sys.stderr)
            print(file=sys.stderr)
        print("Add attribute substitution to the block header, e.g.\n"
              "    [source,bash,subs=\"attributes+\"]\n"
              "or, if the braces are meant literally, escape them (\\{name}).",
              file=sys.stderr)
        return 1

    print(f"verify-docs-attributes: OK — {len(sources)} sources, "
          f"{len(known)} known attributes, no unresolved references in listing blocks")
    return 0


if __name__ == "__main__":
    sys.exit(main())
