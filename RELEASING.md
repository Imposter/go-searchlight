# Releasing

A release is a `vMAJOR.MINOR.PATCH` tag on `main`. Pushing it runs
[.github/workflows/release.yml](.github/workflows/release.yml), which publishes nothing
until every check before it passes:

1. **version**: the version from the tag, checked to be on `main`; its section of
   [CHANGELOG.md](CHANGELOG.md) as the release notes; the deploy samples' image tags
   checked against it.
2. **CI**: every check of [ci.yml](.github/workflows/ci.yml), called as the gate.
3. **binaries**: static binaries (`CGO_ENABLED=0`, `-trimpath`,
   `-X main.version=<tag>`) for linux/amd64, linux/arm64, windows/amd64, darwin/amd64 and
   darwin/arm64, each archived with README.md and docs/operations.md;
   `searchlight-deploy-<version>.tar.gz` with the Compose samples, `.env.example`, nginx's
   config and the Kubernetes manifests; and `SHA256SUMS` over them all.
4. **smoke**: each archive checked against `SHA256SUMS`, unpacked and run on its own
   platform (darwin/amd64 under Rosetta 2) on SQLite with `insecure_no_auth`: `/readyz`,
   then an index created, a document indexed and found
   ([.github/scripts/smoke.sh](.github/scripts/smoke.sh)).
5. **image**: the linux/amd64 and linux/arm64 images built from deploy/Dockerfile, each
   run on SQLite and probed the same way (arm64 under QEMU), then pushed as one
   multi-arch image to `ghcr.io/imposter/searchlight` with the tags `<version>`,
   `<major>.<minor>`, `<major>` and `latest`, a provenance attestation and an SBOM.
6. **release**: the GitHub release for the tag, with the archives, the deploy bundle and
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

Then run the whole workflow as a dry run, which builds and tests everything and
publishes nothing. On a branch it takes the version from CHANGELOG.md's newest section:

```sh
gh workflow run release.yml --ref <branch> -f dry_run=true
gh run watch
```

GitHub runs `workflow_dispatch` only for a workflow that is on the default branch, so
the first release's dry run waits until release.yml is merged. The run's summary shows
the release notes, the checksums and the image tags; its `dist` artifact holds the
archives. Merge the branch once it passes.

## Tag it

```sh
git switch main && git pull --ff-only
git tag -a v1.0.0 -m "Searchlight 1.0.0"
git push origin v1.0.0
```

Watch it with `gh run watch`. If a job fails before the image is pushed, nothing is
published: fix the cause on `main`, delete the tag (`git push --delete origin v1.0.0` and
`git tag -d v1.0.0`) and tag again. If the image is pushed and the release job fails,
re-run the failed jobs (`gh run rerun <run id> --failed`); pushing the same tags again is
harmless. To run the publishing workflow again for an existing tag:
`gh workflow run release.yml --ref v1.0.0 -f dry_run=false`.

A tag with a pre-release suffix (`v1.1.0-rc.1`) publishes a GitHub pre-release and only
the image tag `1.1.0-rc.1`: `<major>.<minor>`, `<major>` and `latest` stay on the last
release, and so may the samples.

## After the first release

GHCR makes a new package private. Once `ghcr.io/imposter/searchlight` exists, open its
settings (github.com/users/Imposter/packages/container/searchlight/settings), check it
is connected to this repository (the image's `org.opencontainers.image.source` label
does that) and set its visibility to public, so `docker pull` needs no login.
