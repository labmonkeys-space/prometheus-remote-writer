# Contributing

Thanks for considering a contribution to `prometheus-remote-writer`.

## Start from an issue

Work starts from a [GitHub issue](https://github.com/labmonkeys-space/prometheus-remote-writer/issues), not a drive-by pull request.
Open a bug report or enhancement request first, then reference it from your PR with a closing keyword (`Closes #123`).

## Review and merge rules

`main` is protected. A change reaches it through a pull request, never a direct push, and the pull request must carry:

- **A signed commit.** Unsigned commits are rejected by the ruleset, not by a reviewer.
- **Green CI.** The build, the workflow linters and the version-declaration check are required status checks, and no one can merge through a red one.
- **A branch that is up to date with `main`.** If `main` moved while your pull request was open, update the branch (`gh pr update-branch`) and let CI run again.
- **Review by the maintainer.** `.github/CODEOWNERS` names them, so opening a pull request requests their review automatically. A review that predates your last push is dismissed when you push again.

Approval is requested but not enforced by the ruleset, and that is deliberate. The project has one maintainer, GitHub does not let anyone approve their own pull request, and the only way to merge past a required approval is the administrator override, which bypasses the required status checks as well. Trading "no merge without an approval" for "a merge that can skip CI" makes the repository less safe, not more, so the approval stays a convention until there is a second maintainer to make it a rule.

## Building and testing

The build is fronted by a `Makefile` over the Maven Wrapper.
CI invokes `make` targets, never raw Maven — do the same locally.

```bash
make build     # compile, run unit tests, install all modules locally
make test      # unit tests only
make verify    # unit + integration tests (needs Docker)
make kar       # build the KAR
make smoke     # e2e smoke tests against all backends (Docker compose)
```

## Fuzzing

Three targets live in `plugin/src/test/java/.../fuzz/`, on the inputs the plugin does not control: the WAL frame decoder, which reads segments a crash may have torn mid-write; the sanitizer, which turns arbitrary OpenNMS strings into Prometheus label names and values; and the WAL entry codec, which parses whatever a frame whose checksum passed happens to contain.

They are plain classes with a `fuzzerTestOneInput(byte[])` method and no fuzzing dependency, so they compile with the rest of the tests. Two things run them:

- `make test` drives each one over a seed corpus and a deterministic sweep (`FuzzTargetsSeedTest`). This is regression coverage: it is fast, and it runs on every pull request.
- A nightly workflow builds them with [ClusterFuzzLite](https://google.github.io/clusterfuzzlite/) and mutates inputs looking for new crashes.

When the nightly run finds a crash it uploads the input that caused it. Add those bytes to the seed corpus in `FuzzTargetsSeedTest` and the crash becomes an ordinary failing test, which is where it should be fixed.

To run a fuzzer locally the way CI does, with Docker:

```bash
docker build -f .clusterfuzzlite/Dockerfile -t prw-fuzz .
docker run --rm -e FUZZING_LANGUAGE=jvm -e SANITIZER=address -v /tmp/fuzz-out:/out prw-fuzz compile
docker run --rm -v /tmp/fuzz-out:/out -w /out prw-fuzz ./FrameFuzzer -runs=100000
```

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/): `<type>[scope]: <description>` with types `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `ci`, `build`, `revert`.
Breaking changes append `!` or add a `BREAKING CHANGE:` footer.

## Developer Certificate of Origin

All commits must be signed off (`git commit -s`), certifying the [DCO](https://developercertificate.org/).
The `Signed-off-by` trailer must name a human identity — the person responsible for the contribution.

## AI-assisted contributions

AI assistance is welcome.
Commits produced with an AI agent additionally carry an `Assisted-by: <Agent>:<model>` trailer (e.g. `Assisted-by: ClaudeCode:claude-fable-5`).
The human signer reviews all AI-generated code and remains responsible for its correctness and license compliance.

## License hygiene (load-bearing)

This project is a **clean-room implementation** licensed Apache 2.0.
Code MUST NOT be derived from the AGPL-3.0 [`opennms-cortex-tss-plugin`](https://github.com/OpenNMS/opennms-cortex-tss-plugin).
Acceptable implementation references are the [Prometheus Remote Write spec](https://prometheus.io/docs/specs/prw/remote_write_spec/), the [Prometheus HTTP query API docs](https://prometheus.io/docs/prometheus/latest/querying/api/), the upstream Prometheus protobuf definitions, and the sanitization rules from `prometheus/common` (all Apache 2.0).
If you suspect a snippet could be derivative, flag it explicitly in your PR instead of importing the pattern.

Every `.java` file begins with the project's Apache 2.0 header.
The year is the file's creation year; the author line stays stable across edits.
