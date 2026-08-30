# Contributing

Thanks for considering a contribution to `prometheus-remote-writer`.

## Start from an issue

Work starts from a [GitHub issue](https://github.com/labmonkeys-space/prometheus-remote-writer/issues), not a drive-by pull request.
Open a bug report or enhancement request first, then reference it from your PR with a closing keyword (`Closes #123`).

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
