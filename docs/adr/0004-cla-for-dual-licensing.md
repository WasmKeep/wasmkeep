# 0004. A CLA, so the project can also be licensed commercially

- Status: accepted
- Date: 2026-11-02

## Context

The project had to choose between a CLA and a DCO. A DCO is friendlier: a
contributor certifies they have the right to submit their work, under the
project's license. A CLA also grants the maintainer direct, broader rights
from each contributor, including licensing the whole codebase under other
terms. I decided the project should keep the option to offer commercial
licenses, so it asks for a CLA.

## Decision

- Contributors sign an individual CLA ([CLA.md](../../CLA.md)) granting
  copyright and patent licenses that allow distribution under any terms,
  including commercial ones. Contributors keep their copyright.
- The core stays Apache-2.0; the CLA promises that accepted contributions stay
  available under Apache-2.0 (no relicensing *away* from open source).
- Enforcement by a CLA check on pull requests (the CLA Assistant Lite GitHub
  Action): a contributor signs once by posting a comment, and the signature
  is recorded in the repository. A corporate CLA is added when the first
  company contributes.

## Consequences

- Some contributors avoid CLAs; the friction is one comment on the first pull
  request, and the reason is stated plainly in CONTRIBUTING.
- The name is protected separately ([TRADEMARK.md](../../TRADEMARK.md)).
