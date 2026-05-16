# Releasing

This document describes the release procedure for `prometheus-remote-writer`.
It assumes the `main` branch is in a releasable state (CI green, `make verify`
passing locally, KAR install verified against a Horizon 35 target).

## Summary

Releases are driven by **pushing a `v*.*.*` git tag**. The
[`release.yml`](.github/workflows/release.yml) workflow picks up the tag push,
verifies the tag's signature against the maintainer's GitHub-registered
GPG keys, builds the KAR, extracts the matching section from `CHANGELOG.md`,
attests the artifacts via Sigstore, and creates a GitHub Release with the
KAR and SBOM attached.

Tags follow [SemVer](https://semver.org): `vMAJOR.MINOR.PATCH` (`v0.1.0`,
`v0.2.0`, `v1.0.0-rc1`).

## Maintainer signing identity

Starting with **v0.5.0**, the release pipeline does **not** use a dedicated
project GPG key. Two distinct trust paths cover the release:

1. **The git tag** is signed locally on the maintainer's workstation with
   a personal GPG key that is **registered on their GitHub profile**.
   The canonical trust path for verifiers is `https://github.com/<maintainer>.gpg`,
   which returns every key the maintainer has ever registered on the
   profile (active + historical).
2. **The release artifacts (KAR, SBOM)** are signed in CI via Sigstore
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

Releases predating v0.5.0 were signed by a dedicated project GPG key
(long key ID `0x1FC793D7F2E3FDDD`, fingerprint
`53BC D4E3 C0CC 9ACF 40F4  6669 1FC7 93D7 F2E3 FDDD`). Their release
pages still carry the `KEYS` file, the `.asc` signatures, and the
`.sha512` checksums. The verification flow for those releases is the
GPG-based one documented in the historical RELEASING.md at the
[`v0.4.3` tag](https://github.com/opennms-forge/prometheus-remote-writer/blob/v0.4.3/RELEASING.md#verifying-a-release)
(import `KEYS`, cross-check the canonical fingerprint above, then
`gpg --verify <file>.asc`) and remains valid indefinitely.

The project key has been retired for *new* signing operations. It is
not destroyed; it sits in the maintainer's keyring as a verifier of
record for pre-v0.5.0 releases.

> **Note on `releases/latest/download/KEYS`:** consumers who scripted
> against `https://github.com/opennms-forge/prometheus-remote-writer/releases/latest/download/KEYS`
> will see a 404 once v0.5.0 is published — `latest` resolves to the
> newest release, which no longer ships `KEYS`. To pin to the last
> release that does, use the explicit tag URL:
> `…/releases/download/v0.4.3/KEYS`.

## Pre-flight checklist

Before tagging, confirm:

- [ ] `main` is on the commit you want to ship.
- [ ] `make verify` is green locally (unit tests + Testcontainers integration
      test against a real Prometheus container).
- [ ] CI is green on the commit being tagged.
- [ ] `CHANGELOG.md` has a `## [X.Y.Z]` section for the target version with
      the changes since the previous release. Move `[Unreleased]` content
      into the new version section if needed.
- [ ] The `<version>` in `pom.xml` matches the tag (without the `v` prefix).
      For `v0.5.0` the version is `0.5.0`; once shipped, bump the pom to
      the next `-SNAPSHOT` on `main`.
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

If the pom is still on `0.5.0-SNAPSHOT` and you're cutting `v0.5.0`, strip the
`-SNAPSHOT`:

```bash
./mvnw versions:set -DnewVersion=0.5.0 -DgenerateBackupPoms=false
git status   # sanity-check — versions:set updates ALL 5 poms
            # (root + 4 child modules), not just the root
```

> **⚠️ Footgun, learned the hard way during v0.3.2.** `versions:set`
> modifies the root `pom.xml` AND each child module's `<parent><version>`
> reference (`plugin/pom.xml`, `karaf-features/pom.xml`,
> `assembly/kar/pom.xml`, `docs/pom.xml`). All 5 must be staged together.
> Staging only the root pom (`git add pom.xml`) leaves the children
> stuck at the previous SNAPSHOT version, and the release CI fails with
> `Non-resolvable parent POM`. Local `make build` may still succeed
> because of a cached parent install — only CI catches this.

Edit `CHANGELOG.md`:

- Move content under `## [Unreleased]` into a new `## [0.5.0] — YYYY-MM-DD`
  section.
- Add a fresh empty `## [Unreleased]` at the top.
- Update the comparison links at the bottom of the file.

Commit all changes together (note: `*/pom.xml` covers the 4 child
modules; `git add -u` would also work since `versions:set` only touches
already-tracked files):

```bash
git add pom.xml */pom.xml */**/pom.xml CHANGELOG.md
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
- Builds the KAR via `make kar`; generates the SBOM via `make sbom`.
- Extracts the `## [0.5.0]` section from `CHANGELOG.md` as the release body.
- Produces SLSA Build Provenance attestations (one for the KAR, one for the SBOM) via Sigstore.
- Creates a GitHub Release named `v0.5.0` with the KAR and SBOM attached as assets.

Watch the run:

```bash
gh run watch --repo opennms-forge/prometheus-remote-writer
```

### 3. Post-release

Bump `main` to the next development version:

```bash
./mvnw versions:set -DnewVersion=0.6.0-SNAPSHOT -DgenerateBackupPoms=false
git add pom.xml
git commit -m "chore: bump to 0.6.0-SNAPSHOT"
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

This is idempotent — `gh release create` will fail if the release already
exists, so delete the previous release and its asset first if you're
recovering from a partial run.

> **Heads up on partial-failure re-runs:** the workflow publishes
> attestations *before* it creates the GitHub Release. If a re-run
> reaches the attest steps again (i.e., you deleted the release but
> not the attestations), each `actions/attest-build-provenance`
> invocation produces a fresh attestation for the same artifact.
> Verification still succeeds against any valid attestation, but the
> repo's attestations endpoint accumulates duplicates and the
> Sigstore transparency log shows extra entries. To avoid this,
> delete only the GitHub Release (`gh release delete <tag>`) before
> re-running; the attestation from the partial run remains usable.
>
> **Heads up on running the v0.5.0+ workflow against a pre-v0.5.0
> tag** (e.g., `gh workflow run release.yml -f tag=v0.4.3`): the
> tag-verify step imports keys from `github.com/<RELEASE_MAINTAINER>.gpg`,
> but pre-v0.5.0 tags were signed by the retired project key, which
> is **not** on the maintainer's GitHub profile. Verification will
> fail. The supported way to re-issue a pre-v0.5.0 release is to
> check out RELEASING.md and release.yml at the v0.4.3 tag and run
> the legacy GPG-based flow against that historical workflow shape.

## Hotfix releases

For a patch release (e.g. `v0.5.1`) on top of `v0.5.0`:

1. Branch off the previous tag: `git checkout -b hotfix/0.5.1 v0.5.0`.
2. Apply the fix, commit, update CHANGELOG and pom version.
3. Merge back to `main` (or cherry-pick).
4. **GPG-sign** tag `v0.5.1` on the hotfix branch with a key registered
   on your GitHub profile: `git tag -s v0.5.1 -m "v0.5.1"` and verify
   with `git tag -v v0.5.1`.
5. Push the tag.

## What gets published

| Artifact | Where | How consumed |
|---|---|---|
| `prometheus-remote-writer-kar-<version>.kar` | GitHub Release asset | Download and install via Karaf `kar:install <path>`. Verify with `gh attestation verify` — see "Verifying a release" below. |
| `prometheus-remote-writer-<version>.cdx.json` | GitHub Release asset | CycloneDX 1.6 SBOM (aggregate across the full Maven reactor); fed to Trivy / Grype / Dependency-Track / FOSSA-style consumers. Generate locally with `make sbom`. Verify with `gh attestation verify`. |
| Build provenance attestations | <https://github.com/opennms-forge/prometheus-remote-writer/attestations> | One per artifact (KAR + SBOM). SLSA Build Provenance signed by Sigstore via GitHub Actions OIDC. Fetched server-side by `gh attestation verify`; not a downloadable release asset. |
| `prometheus-remote-writer-<version>.jar` (bundle) | Not auto-published | Planned. The repo's migration to `opennms-forge` is done; remaining decision is the Maven-repo target (Central via Sonatype, GitHub Packages, or a private Nexus). When Maven Central publication lands, the maintainer's personal GPG key serves as the Central PGP identity (a common pattern for single-maintainer projects). |
| `prometheus-remote-writer-features-<version>-features.xml` | Not auto-published | Same as above — consumed via `feature:repo-add mvn:…/xml/features` when a Maven repo is available. |

## Verifying a release

Each release artifact carries a SLSA Build Provenance attestation that
encodes (a) the artifact's SHA-256, (b) the workflow file path that
produced it, (c) the source commit, and (d) the GitHub Actions run that
generated the attestation — all signed by Sigstore via the workflow's
OIDC token. Verification resolves through Sigstore root CAs and
GitHub's identity binding, not through a maintainer-published key
fingerprint.

**Prerequisite:** `gh` CLI **2.49.0 or newer** (released 2024-04-30).
Check with `gh --version`.

```bash
TAG=v0.5.0
BASE=https://github.com/opennms-forge/prometheus-remote-writer/releases/download/${TAG}

# 1. Download the artifact(s) you want to verify.
curl -O ${BASE}/prometheus-remote-writer-kar-${TAG#v}.kar
curl -O ${BASE}/prometheus-remote-writer-${TAG#v}.cdx.json

# 2. Verify the KAR. `gh attestation verify` fetches the attestation
#    bundle from the repository's /attestations endpoint, checks the
#    Sigstore signature, confirms the certificate identity (the
#    workflow path), and compares the artifact's SHA-256 against the
#    attestation subject digest.
gh attestation verify \
  prometheus-remote-writer-kar-${TAG#v}.kar \
  --repo opennms-forge/prometheus-remote-writer

# 3. Verify the SBOM the same way.
gh attestation verify \
  prometheus-remote-writer-${TAG#v}.cdx.json \
  --repo opennms-forge/prometheus-remote-writer
```

A successful verification prints the signer identity (the workflow
path: `https://github.com/opennms-forge/prometheus-remote-writer/.github/workflows/release.yml@refs/tags/v0.5.0`),
the predicate type (`https://slsa.dev/provenance/v1`), and the commit
SHA the artifact was built from. For an extra defense-in-depth
assertion, pin against the workflow ref via `--signer-workflow`:

```bash
gh attestation verify \
  prometheus-remote-writer-kar-${TAG#v}.kar \
  --repo opennms-forge/prometheus-remote-writer \
  --signer-workflow opennms-forge/prometheus-remote-writer/.github/workflows/release.yml
```

The `--signer-workflow` flag takes an `[host/]<owner>/<repo>/<path>/<to>/<workflow>`
form; a bare workflow path will not match.

### Air-gap verification

`gh attestation verify` contacts GitHub by default to fetch the
attestation bundle. For air-gapped environments, download the bundle
on a connected host once and transport it into the air-gap.

```bash
# On a connected host:
gh attestation download \
  prometheus-remote-writer-kar-${TAG#v}.kar \
  --repo opennms-forge/prometheus-remote-writer
# Produces: prometheus-remote-writer-kar-<version>.kar.jsonl

# Inside the air-gap, with the .kar and the .jsonl both present:
gh attestation verify \
  prometheus-remote-writer-kar-${TAG#v}.kar \
  --bundle prometheus-remote-writer-kar-${TAG#v}.kar.jsonl \
  --repo opennms-forge/prometheus-remote-writer
```

The `--bundle` flag short-circuits the network fetch; Sigstore's
trusted root keys ship inside `gh` so the cryptographic verification
itself is fully offline.

### Honest trust note

Verification of a v0.5.0+ release resolves to two trust roots that
sit upstream of this project:

1. **Sigstore's trusted root CAs** (Fulcio, Rekor public keys). `gh`
   ships them; the Sigstore project rotates them publicly. Compromise
   of these is a global ecosystem event, not a project-specific risk.
2. **GitHub's OIDC identity binding for this repository.** The
   attestation says "the workflow at `.github/workflows/release.yml`
   in this repo, running on a tag, produced this artifact." Compromise
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

## Docs site (GitHub Pages)

A separate workflow,
[`.github/workflows/publish-docs.yml`](.github/workflows/publish-docs.yml),
publishes the rendered single-page HTML documentation to
<https://opennms-forge.github.io/prometheus-remote-writer/> whenever a GitHub
Release is published. The site always reflects the most recent release
tag — older versions are not retained as separate URLs in this release line.

### One-time repo setup

Before the first publish workflow run, enable Pages in the repository:

- **Settings → Pages → Build and deployment → Source: GitHub Actions.**

The workflow needs `pages: write` and `id-token: write`, which are already
declared at workflow scope. No `gh-pages` branch is used.

### Manual republish

If a release ships with a docs typo, fix it on `main` and re-run the
workflow against the same tag — the published site updates without
cutting a new release:

```bash
gh workflow run publish-docs.yml -f tag=v0.5.0
```

The `release: published` trigger fires once per release; `workflow_dispatch`
is for these out-of-band republishes.

## Deferred to a later release

- **Maven artifact publication** — required for the Karaf
  `feature:repo-add mvn:…` install flow shown in the README. The repo
  now lives under the `opennms-forge` namespace; remaining decision is
  the Maven-repo target (Central via Sonatype, GitHub Packages, or a
  private Nexus). The maintainer's personal GPG key (the same one
  that signs release tags) serves as the Central PGP identity when
  that work lands — no additional key infrastructure needed, no
  conflict with the Sigstore artifact-signing path.
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
