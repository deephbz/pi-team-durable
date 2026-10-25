# Pi Team Durable agent guide

Pi Team Durable runs Pi Team Bright Team semantics on pi-durable. It is a
prerelease in hardening. A host supplies the Team ports; this repository has no
runtime of its own.

## Read order

1. `README.md`: purpose, status, known issues, API.
2. `src/authority/index.ts`: the public export. Do not export more without a
   decision.
3. `docs/maintainers/releasing.md`: before any release.

## Rules

- Keep the persisted value `sender: 'durable-team'` unchanged.
- Keep the peer dependencies on `@earendil-works/*` pinned exactly.
- Run tests proportional to the change. Run `npm run build`, `npm run typecheck`,
  `npm test` and `npm run verify:package` once on the final tree.

## Commit identity and privacy

- Author and committer are the GitHub identity `deephbz` with its
  `@users.noreply.github.com` email. Never use a local account, host or personal
  email, and never infer one from the environment.
- Commit messages, trailers, tracked files, pull request text and tags carry no
  agent session or thread reference.
- Tracked files carry no local path, host name, personal name, credential or
  private project association. Use synthetic examples.
- Run `git-privacy-scan --ref HEAD history` before publication. Never print a
  detected value.

## Releases

A release is a version tag pushed to `main`; CI publishes it. See
[releasing](docs/maintainers/releasing.md). Never publish from a local checkout,
except the documented first version.
