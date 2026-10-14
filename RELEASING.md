# Releasing

A release is a `vMAJOR.MINOR.PATCH` tag on `main`. Pushing it runs
[.github/workflows/release.yml](.github/workflows/release.yml), which publishes nothing
until every check before it passes, and runs one release at a time:

1. **preflight**: the version from the tag, checked to be on `main`; a refusal if the
   tag's GitHub release exists already; the image tags it will move (below); its section
   of [CHANGELOG.md](CHANGELOG.md) as the release notes, with relative links pointed at
   the tag's files; and the deploy samples' image tags checked against it.
2. **CI and chaos**: every check of [ci.yml](.github/workflows/ci.yml) and
   [chaos.yml](.github/workflows/chaos.yml), called as the gate.
3. **binaries**: static binaries (`CGO_ENABLED=0`, `-trimpath`,
   `-X main.version=<tag>`) for linux/amd64, linux/arm64, windows/amd64, darwin/amd64 and
   darwin/arm64, built with go.mod's `toolchain` (checked), each archived with LICENSE,
   THIRD_PARTY_NOTICES ([tools/notices.sh](tools/notices.sh): Go's licence and every
   dependency's, from go-licenses), README.md and docs/operations.md;
   `searchlight-deploy-<version>.tar.gz` with the Compose samples, `.env.example`, nginx's
   config and the Kubernetes manifests; and `SHA256SUMS` over them all.
4. **smoke**: each archive checked against `SHA256SUMS`, unpacked and run on its own
   platform (darwin/amd64 under Rosetta 2) on SQLite with `insecure_no_auth`: `/readyz`,
   then an index created, a document indexed and found
   ([.github/scripts/smoke.sh](.github/scripts/smoke.sh)).
5. **image**: the linux/amd64 and linux/arm64 image built once from deploy/Dockerfile,
   with a provenance attestation and an SBOM, and pushed by digest to the staging tag
   `ghcr.io/imposter/searchlight:sha-<commit>`.
6. **image smoke**: that digest pulled on a native runner per platform (arm64 on
   `ubuntu-24.04-arm`), checked to hold the same Go, LICENSE and THIRD_PARTY_NOTICES as
   the archive of its platform, then run and probed like the binaries.
7. **image tags**: the tested digest tagged `<version>`, plus `<major>.<minor>`,
   `<major>` and `latest` for each line this release is the newest stable release of. A
   patch to an older line (v1.0.1 after v1.1.0) gets `1.0.1` and `1.0` only. Each tag is
   checked to resolve to the tested digest.
8. **release**: the GitHub release for the tag, with the archives, the deploy bundle and
   `SHA256SUMS`, and the CHANGELOG section as its notes.

## Prepare it

On a branch:

- **CHANGELOG.md**: add `## [X.Y.Z] - YYYY-MM-DD`, with its link at the bottom. The
  workflow refuses a tag without that section.
- **The samples' image**: set the new version as `SEARCHLIGHT_VERSION` in
  deploy/.env.example, as its default in deploy/compose.postgres.yml and
  deploy/compose.sqlite.yml, and as the image in deploy/k8s/statefulset.yaml. The
  workflow refuses a release whose samples run another version.
- **The examples**: the download URLs and tags in README.md's Install section and in
  docs/operations.md (`git grep -n 1.0.0 -- README.md docs/operations.md`).
- **The toolchain**: go.mod's `toolchain` line is the Go the binaries are built with, and
  must be the Go of deploy/Dockerfile's `golang` image (CI checks it); bump both together
  for a Go security release.

Then run the whole workflow as a dry run, which builds and tests everything and writes
nothing to the registry or the releases. On a branch it takes the version from
CHANGELOG.md's newest section:

```sh
gh workflow run release.yml --ref <branch> -f dry_run=true
gh run watch
```

GitHub runs `workflow_dispatch` only for a workflow that is on the default branch, so
the first release's dry run waits until release.yml is merged. The run's summary shows
the release notes, the image tags it would move, the toolchain and the checksums; its
`dist` artifact holds the archives. Merge the branch once it passes.

## Tag it

```sh
git switch main && git pull --ff-only
git tag -a v1.0.0 -m "Searchlight 1.0.0"
git push origin v1.0.0
```

Watch it with `gh run watch`.

- **A job fails before the image tags are set** (the gate, the binaries, a smoke test,
  the image build): nothing a user pulls has changed, only perhaps the staging tag. If
  the cause was transient, open the run and use **Re-run failed jobs**. If it needs a fix,
  fix it on `main`, delete the tag (`git push --delete origin v1.0.0` and
  `git tag -d v1.0.0`) and tag again.
- **The image tags or the release job fail**: use **Re-run failed jobs** on the same run
  (`gh run rerun <run id> --failed`). It reuses that run's tested digest and artifacts,
  so it builds nothing again.
- **Never start a new run for a published tag.** Preflight refuses a tag whose GitHub
  release exists, so a second run cannot rebuild and re-push a published version.

A tag with a pre-release suffix (`v1.1.0-rc.1`) publishes a GitHub pre-release and only
the image tag `1.1.0-rc.1`: `<major>.<minor>`, `<major>` and `latest` stay where they
are, and so may the samples.

## The first release: make the package public

GHCR creates a new package as private, and the release's README, Compose samples and
Kubernetes manifests all pull `ghcr.io/imposter/searchlight`. So for the first release,
as soon as the **image** job has pushed (watch the run), and before you announce the
release:

1. Open github.com/users/Imposter/packages/container/searchlight/settings.
2. Check that it is connected to this repository (the image's
   `org.opencontainers.image.source` annotation does that).
3. Under "Danger Zone", change its visibility to public.

Then `docker pull ghcr.io/imposter/searchlight:1.0.0` works without a login. Later
releases push to the same package and stay public.
