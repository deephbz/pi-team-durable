# Releasing

A release is a version tag on `main`. Pushing the tag runs
[`publish.yml`](../../.github/workflows/publish.yml): it requires the CI gate
for the tagged tree (instant when `main` already passed it), verifies the
package, publishes it to npm with provenance, and creates the GitHub Release.
GitHub Releases are the release history; don't add a root changelog.

1. Merge the change to `main` through a normal PR with green CI. In the same
   PR, bump `version` in `package.json` and `package-lock.json`, and add
   `docs/release/v<version>-release-notes.md`. The workflow uses that file as
   the GitHub Release body.
2. Run the privacy gate on the release commit. The pre-push hook runs the same
   scan:
   `git-privacy-scan --ref HEAD --base "$(git config privacy.baseline)" range`.
   Any finding blocks the release. Never print a detected value.
3. Tag and push: `git tag -a v<version> -m v<version>` then
   `git push origin v<version>`.

A version with a prerelease suffix goes to npm `next`; any other version goes
to `latest`. If the workflow fails before `npm publish`, fix the problem,
delete the tag, and tag the fixed commit. If it fails after `npm publish`, the
version is spent: release the next version.

Publication adds no commit. Don't add checklist or receipt files, and don't
write the published version into the README.

If a published version is bad, deprecate it and publish a corrected higher
version. Never retarget a pushed tag of a published version or replace package
bytes.

## The first version

npm trusted publishing needs an existing package, so the first version,
`0.1.0-rc.1`, was published by hand from a clean checkout of its tagged commit.
It is the one exception to CI-only releases and has no npm provenance. Its tag
push runs `publish.yml`, whose publish step fails because the version exists.
Its GitHub Release was created by hand. After the first publish, trusted
publishing was configured for `publish.yml` and the `npm` environment. Every
later version publishes from CI.

## Verification before a release

`npm run build`, `npm run typecheck`, `npm test` and `npm run verify:package`
must pass. Run the oracle with at least 500 seeds by hand:
`npm run test:oracle -- --seeds 500 --shards 4`.
