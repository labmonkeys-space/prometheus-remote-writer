# Releasing

This document describes the release procedure for `prometheus-remote-writer`.
It assumes the `main` branch is in a releasable state (CI green, `make verify`
passing locally, the gateway image building and running against a Kafka +
Remote Write target).

## Summary

Releases are driven by **pushing a `v*.*.*` git tag**. The
[`release.yml`](.github/workflows/release.yml) workflow picks up the tag push,
verifies the tag's signature against the maintainer's GitHub-registered
GPG keys, builds and pushes a multi-arch (`linux/amd64,linux/arm64`) OCI
container image to GitHub Container Registry, generates a CycloneDX SBOM over
the pushed image, attests both via Sigstore, and creates a GitHub Release whose
notes pin the image by its immutable `sha256` digest and which attaches the SBOM.

A release consists of **the container image (in the registry, referenced by
digest) plus the CycloneDX SBOM**. There is no KAR.

Tags follow [SemVer](https://semver.org): `vMAJOR.MINOR.PATCH` (`v0.5.0`,
`v0.6.0`, `v1.0.0-rc1`).

## Maintainer signing identity

Starting with **v0.4.4**, the release pipeline does **not** use a dedicated
project GPG key. Two distinct trust paths cover the release:

1. **The git tag** is signed locally on the maintainer's workstation with
   a personal GPG key that is **registered on their GitHub profile**.
   The canonical trust path for verifiers is `https://github.com/<maintainer>.gpg`,
   which returns every key the maintainer has ever registered on the
   profile (active + historical).
2. **The release artifacts (image, SBOM)** are signed in CI via Sigstore
   using GitHub Artifact Attestations. The signature is bound to the
   workflow run's OIDC token — no long-lived key, nothing for a repo-admin
   compromise to ex-filtrate.

Current maintainer (single-maintainer model; this section grows when
co-maintainers join):

| Field | Value |
|---|---|
| GitHub user | [`indigo423`](https://github.com/indigo423) |
| Public keys (canonical) | <https://github.com/indigo423.gpg> |
| Configured in workflow | `.github/workflows/release.yml` → `env.RELEASE_MAINTAINER` |

**Operational policy:** retired GPG keys remain on the maintainer's
GitHub profile so older tags stay re-verifiable. Do not delete a key
from the GitHub profile after it has signed a release; rotate by adding
a new key and leaving the old one in place.

### Older releases (`v0.1.0` through `v0.4.x`)

Releases predating v0.4.4 were signed by a dedicated project GPG key
(long key ID `0x1FC793D7F2E3FDDD`, fingerprint
`53BC D4E3 C0CC 9ACF 40F4  6669 1FC7 93D7 F2E3 FDDD`). They shipped a
**KAR** built from the then-current OSGi/Karaf plugin; their release pages
still carry the `KEYS` file, the `.asc` signatures, and the `.sha512`
checksums. The verification flow for those releases is the GPG-based one
documented in the historical RELEASING.md at the
[`v0.4.3` tag](https://github.com/opennms-forge/prometheus-remote-writer/blob/v0.4.3/RELEASING.md#verifying-a-release)
(import `KEYS`, cross-check the canonical fingerprint above, then
`gpg --verify <file>.asc`) and remains valid indefinitely.

The project key has been retired for *new* signing operations. It is
not destroyed; it sits in the maintainer's keyring as a verifier of
record for pre-v0.4.4 releases.

> **Note on `releases/latest/download/KEYS`:** consumers who scripted
> against `https://github.com/opennms-forge/prometheus-remote-writer/releases/latest/download/KEYS`
> see a 404 from v0.4.4 onward — `latest` resolves to the newest release,
> which no longer ships `KEYS`. To pin to the last release that does, use
> the explicit tag URL: `…/releases/download/v0.4.3/KEYS`.

> **Note on the KAR:** the gateway pivot (v0.5.0) replaced the Karaf plugin
> with a standalone container image. Releases from v0.5.0 onward publish a
> container image, not a `.kar` — see "What gets published" below.

## Pre-flight checklist

Before tagging, confirm:

- [ ] `main` is on the commit you want to ship.
- [ ] `make verify` is green locally (fmt check + clippy + unit tests).
- [ ] `make integration` is green (Testcontainers Kafka integration tests —
      needs Docker), if the change touched the ingest/sender path.
- [ ] The gateway image builds and runs: `make image`, then a `docker run`
      against a Kafka + Remote Write target (or `make smoke`).
- [ ] CI is green on the commit being tagged.
- [ ] `CHANGELOG.md` has a `## [X.Y.Z]` section for the target version with
      the changes since the previous release. Move `[Unreleased]` content
      into the new version section if needed.
- [ ] The workspace `version` in `Cargo.toml` matches the tag (without the `v`
      prefix). For `v0.5.0` the version is `0.5.0`.
- [ ] `README.md` Quick-start references match the target version.
- [ ] **The git tag will be GPG-signed** — use `git tag -s vX.Y.Z -m "vX.Y.Z"`
      (NOT `git tag` plain or `git tag -a`). The `release.yml` workflow
      verifies the tag signature and refuses to publish if it's missing.
- [ ] **The tag signing key is on your GitHub profile** — the workflow
      fetches public keys from `https://github.com/${RELEASE_MAINTAINER}.gpg`
      and accepts a tag signature by any of them. Add the key under
      Settings → SSH and GPG keys before tagging.

## Release steps

### 1. Update version and CHANGELOG

The version is declared once in the workspace and inherited by both crates
(`version.workspace = true`). If `Cargo.toml` is on the next-dev version and
you're cutting `v0.5.0`, set it to the release version:

```bash
# Edit [workspace.package] version = "0.5.0" in the root Cargo.toml, then
# refresh the lockfile so the recorded package versions match.
cargo build --workspace
git status   # sanity-check: Cargo.toml + Cargo.lock both changed
```

Edit `CHANGELOG.md`:

- Move content under `## [Unreleased]` into a new `## [0.5.0] — YYYY-MM-DD`
  section.
- Add a fresh empty `## [Unreleased]` at the top.
- Update the comparison links at the bottom of the file.

Commit all changes together:

```bash
git add Cargo.toml Cargo.lock CHANGELOG.md
git commit -m "release: v0.5.0"
```

### 2. Tag the release

```bash
# Sign the tag with a GPG key registered on your GitHub profile.
git tag -s v0.5.0 -m "v0.5.0"

# Verify the signature locally before pushing.
git tag -v v0.5.0
# expected: "Good signature from <your name> <your email> ..."

git push origin main
git push origin v0.5.0
```

Pushing the tag triggers `.github/workflows/release.yml`:

- Resolves the tag and version.
- Fetches the maintainer's public keys from `github.com/<RELEASE_MAINTAINER>.gpg`.
- Verifies the pushed tag's GPG signature; **fails the workflow if the tag is unsigned or signed by a key not on the maintainer's profile**.
- Builds a multi-arch (`linux/amd64,linux/arm64`) image via Buildx + QEMU and pushes it to `ghcr.io/<owner>/<repo>`, tagged with the version and `latest`.
- Generates a CycloneDX SBOM (syft) over the pushed image by digest.
- Produces SLSA Build Provenance attestations — one for the image (by digest, pushed to the registry) and one for the SBOM — via Sigstore.
- Extracts the `## [0.5.0]` section from `CHANGELOG.md` and appends the image reference pinned by `@sha256:<digest>` plus a verification command as the release body.
- Creates a GitHub Release named `v0.5.0` with the SBOM attached as an asset.

Watch the run:

```bash
gh run watch --repo opennms-forge/prometheus-remote-writer
```

### 3. Post-release

Bump `main` to the next development version:

```bash
# Edit [workspace.package] version = "0.5.1" (or the next planned version)
# in Cargo.toml, then refresh the lockfile.
cargo build --workspace
git add Cargo.toml Cargo.lock
git commit -m "chore: bump to 0.5.1"
git push origin main
```

Announce the release (release-notes URL from the GitHub Releases page).

## Re-running a release

If the workflow fails after tag push (bad CHANGELOG, transient CI issue), you
can re-run it manually via the `workflow_dispatch` trigger:

- GitHub UI → Actions → **Release** → **Run workflow** → enter the tag name.

Or via CLI:

```bash
gh workflow run release.yml -f tag=v0.5.0
```

`gh release create` fails if the release already exists, so delete the previous
release first if you're recovering from a partial run.

> **Heads up on partial-failure re-runs:** the workflow publishes
> attestations *before* it creates the GitHub Release. If a re-run
> reaches the attest steps again (i.e., you deleted the release but
> not the attestations), each `actions/attest-build-provenance`
> invocation produces a fresh attestation for the same subject.
> Verification still succeeds against any valid attestation, but the
> repo's attestations endpoint accumulates duplicates and the
> Sigstore transparency log shows extra entries. To avoid this,
> delete only the GitHub Release (`gh release delete <tag>`) before
> re-running; the attestation from the partial run remains usable.
> (Re-pushing the same image digest is idempotent in the registry.)
>
> **Heads up on running the workflow against a pre-v0.4.4 tag**
> (e.g., `gh workflow run release.yml -f tag=v0.4.3`): the tag-verify
> step imports keys from `github.com/<RELEASE_MAINTAINER>.gpg`, but
> pre-v0.4.4 tags were signed by the retired project key, which is
> **not** on the maintainer's GitHub profile. Verification will fail.
> Pre-v0.5.0 tags also predate the container-image pipeline. The
> supported way to re-issue a historical release is to check out
> RELEASING.md and release.yml at that tag and run the flow of record.

## Hotfix releases

For a patch release (e.g. `v0.5.1`) on top of `v0.5.0`:

1. Branch off the previous tag: `git checkout -b hotfix/0.5.1 v0.5.0`.
2. Apply the fix, commit, update CHANGELOG and the workspace version.
3. Merge back to `main` (or cherry-pick).
4. **GPG-sign** tag `v0.5.1` on the hotfix branch with a key registered
   on your GitHub profile: `git tag -s v0.5.1 -m "v0.5.1"` and verify
   with `git tag -v v0.5.1`.
5. Push the tag.

## What gets published

| Artifact | Where | How consumed |
|---|---|---|
| `ghcr.io/opennms-forge/prometheus-remote-writer:<version>` (and `:latest`) | GitHub Container Registry | Multi-arch (`amd64`, `arm64`) image. Pull and run; pin by the immutable `@sha256:<digest>` printed in the release notes. Verify with `gh attestation verify oci://…` — see "Verifying a release". |
| `prometheus-remote-writer-<version>.cdx.json` | GitHub Release asset | CycloneDX SBOM (syft over the shipped image — OS packages + Rust dependency tree); fed to Trivy / Grype / Dependency-Track / FOSSA-style consumers. Verify with `gh attestation verify`. |
| Build provenance attestations | <https://github.com/opennms-forge/prometheus-remote-writer/attestations> | One per subject (image + SBOM). SLSA Build Provenance signed by Sigstore via GitHub Actions OIDC. The image attestation is also pushed to the registry alongside the image; the SBOM attestation is fetched server-side by `gh attestation verify`. |

## Verifying a release

Each release subject carries a SLSA Build Provenance attestation that
encodes (a) the subject's digest, (b) the workflow file path that
produced it, (c) the source commit, and (d) the GitHub Actions run that
generated the attestation — all signed by Sigstore via the workflow's
OIDC token. Verification resolves through Sigstore root CAs and
GitHub's identity binding, not through a maintainer-published key
fingerprint.

**Prerequisite:** `gh` CLI **2.49.0 or newer** (released 2024-04-30).
Check with `gh --version`.

```bash
TAG=v0.5.0
IMAGE=ghcr.io/opennms-forge/prometheus-remote-writer
BASE=https://github.com/opennms-forge/prometheus-remote-writer/releases/download/${TAG}

# 1. Resolve the image digest (the release notes also print it). Then verify
#    the image. `gh attestation verify` checks the Sigstore signature,
#    confirms the certificate identity (the workflow path), and compares the
#    image digest against the attestation subject.
DIGEST=$(docker buildx imagetools inspect "${IMAGE}:${TAG#v}" --format '{{.Manifest.Digest}}')
gh attestation verify "oci://${IMAGE}@${DIGEST}" \
  --repo opennms-forge/prometheus-remote-writer

# 2. Verify the SBOM the same way (downloadable release asset).
curl -O ${BASE}/prometheus-remote-writer-${TAG#v}.cdx.json
gh attestation verify \
  prometheus-remote-writer-${TAG#v}.cdx.json \
  --repo opennms-forge/prometheus-remote-writer
```

A successful verification prints the signer identity (the workflow
path: `https://github.com/opennms-forge/prometheus-remote-writer/.github/workflows/release.yml@refs/tags/v0.5.0`),
the predicate type (`https://slsa.dev/provenance/v1`), and the commit
SHA the subject was built from. For an extra defense-in-depth
assertion, pin against the workflow ref via `--signer-workflow`:

```bash
gh attestation verify "oci://${IMAGE}@${DIGEST}" \
  --repo opennms-forge/prometheus-remote-writer \
  --signer-workflow opennms-forge/prometheus-remote-writer/.github/workflows/release.yml
```

The `--signer-workflow` flag takes an `[host/]<owner>/<repo>/<path>/<to>/<workflow>`
form; a bare workflow path will not match.

### Air-gap verification

`gh attestation verify` contacts GitHub by default to fetch the
attestation bundle. For air-gapped environments, download the bundle
for a release asset on a connected host once and transport it into the
air-gap:

```bash
# On a connected host:
gh attestation download \
  prometheus-remote-writer-${TAG#v}.cdx.json \
  --repo opennms-forge/prometheus-remote-writer
# Produces: prometheus-remote-writer-<version>.cdx.json.jsonl

# Inside the air-gap, with the .cdx.json and the .jsonl both present:
gh attestation verify \
  prometheus-remote-writer-${TAG#v}.cdx.json \
  --bundle prometheus-remote-writer-${TAG#v}.cdx.json.jsonl \
  --repo opennms-forge/prometheus-remote-writer
```

The `--bundle` flag short-circuits the network fetch; Sigstore's
trusted root keys ship inside `gh` so the cryptographic verification
itself is fully offline. For the **image**, the provenance attestation
is pushed to the registry alongside it (`push-to-registry`), so a
registry mirror that copies referrers (e.g. `skopeo copy
--all` / `oras cp`) carries the attestation into the air-gap, where
`gh attestation verify oci://… --bundle <downloaded.jsonl>` verifies it
offline.

### Honest trust note

Verification of a release resolves to two trust roots that sit upstream
of this project:

1. **Sigstore's trusted root CAs** (Fulcio, Rekor public keys). `gh`
   ships them; the Sigstore project rotates them publicly. Compromise
   of these is a global ecosystem event, not a project-specific risk.
2. **GitHub's OIDC identity binding for this repository.** The
   attestation says "the workflow at `.github/workflows/release.yml`
   in this repo, running on a tag, produced this image." Compromise
   of the workflow file or the repository's branch protection on `main`
   would let an attacker forge a valid attestation.

That's the trust model, end to end. It is not "the maintainer's key
signs the artifact directly" — Sigstore replaces that with "the
maintainer's workflow, vouched for by GitHub, signed the artifact via
a short-lived certificate." Different shape; comparable or stronger
properties in 2026 because (a) there is no long-lived key to leak and
(b) every signing event is publicly logged in the Sigstore
transparency log.

For the **tag** (not the artifact): trust resolves through
`github.com/<RELEASE_MAINTAINER>.gpg`. A consumer who trusts GitHub to
correctly serve that file gets a valid signature check. Note that
the `<RELEASE_MAINTAINER>` value itself lives in the workflow file in
this repository — so a repo-write compromise can both rewrite the
maintainer pointer *and* forge a new attestation. The tag-verify
path is not a fully-independent second factor on the CI side; it is
a defense against a maintainer who forgot `git tag -s` or used the
wrong key. Out-of-band fingerprint cross-checks (e.g. via a
maintainer's personal site or a public keyserver) are an additional
defense layer if the GitHub trust path is insufficient for the
threat model — but the workflow itself reduces to GitHub-trust
either way.

## Documentation

There is no separate published docs site (the GitHub Pages / AsciiDoc
pipeline was retired with the gateway pivot). Operator documentation lives
in the repository: [`README.md`](README.md) for the overview, configuration,
and quickstart, and [`e2e/README.md`](e2e/README.md) for the end-to-end
sandbox.

## Deferred to a later release

- **Cosign image signature** (in addition to the SLSA provenance
  attestation). The provenance attestation already binds the image
  digest to the workflow identity; a separate `cosign sign` keyless
  signature becomes worthwhile if a downstream policy engine
  (Kyverno, Sigstore policy-controller) requires a cosign signature
  predicate specifically.
- **Dedicated SBOM attestation** (`actions/attest-sbom`). The current
  pipeline emits a build-provenance attestation for the SBOM file,
  which encodes its hash and binds it to the workflow run. A
  dedicated SBOM attestation predicate becomes worthwhile if a
  downstream consumer (Dependency-Track, Grype) asks for it
  specifically.
- **Multi-maintainer signing roster.** The workflow's
  `RELEASE_MAINTAINER` env value is currently a single GitHub
  username. Promoting it to a list (or a repo variable) so any
  named maintainer can release is a small future change when the
  project gains co-maintainers.
