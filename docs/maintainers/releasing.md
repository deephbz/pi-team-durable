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

## The first versions

npm trusted publishing needs an existing package, and `npm trust` needs the
owner's two-factor authentication. So `0.1.0-rc.1` and `0.1.0-rc.2` were
published by hand, each from a clean checkout of its tagged commit, to npm
`next`. They are the exceptions to CI-only releases and have no npm provenance.
Their tag pushes ran `publish.yml`: the CI gate passed, then the publish step
failed because the version already existed. Their GitHub Releases were created
by hand with `gh release create --prerelease`.

Until trusted publishing exists, publish each release the same way and create
the GitHub Release by hand. The owner configures trust once, with two-factor
authentication, using the owner's own npm login:

```sh
npm trust github @hypercarrier/pi-team-durable --file publish.yml \
  --repo deephbz/pi-team-durable --env npm --allow-publish
```

After that, pushing a tag publishes from CI.

## The latest dist-tag

A first publish always sets `latest`, and npm does not allow removing it. A
later prerelease published with `--tag next` leaves `latest` behind. Until
`0.1.0` ships, move it by hand:
`npm dist-tag add @hypercarrier/pi-team-durable@<version> latest`.

## Verification before a release

`npm run build`, `npm run typecheck`, `npm test` and `npm run verify:package`
must pass. Run the oracle with at least 500 seeds by hand:
`npm run test:oracle -- --seeds 500 --shards 4`.
