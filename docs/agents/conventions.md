# Conventions

## Reuse and design

- Search for and reuse existing helpers before adding code. Refactor an existing implementation if that avoids duplication.
- Use descriptive names; avoid unexplained abbreviations and single-letter names.
- Keep `ResourceManager` focused on run/job persistence and lifecycle coordination.
- Keep execution-engine behavior behind compiler or execution abstractions. Shared code must remain engine-neutral; do not downcast to `*util.Workflow` in shared layers.
- Put reusable interfaces in neutral packages and use natural domain types. Preserve documented ownership and field-wise override behavior.

## Tests

- Add a unit test for non-trivial functions, methods, and exported APIs. Add coverage when changed behavior needs it.
- Run the relevant tests before submitting. Document unrelated pre-existing failures in the PR description.

## Comments and documentation

- Comment only non-obvious constraints, invariants, workarounds, or surprising behavior.
- Keep comments short. Do not narrate implementation, current-PR history, or obvious test setup.
- Error messages must state the problem and corrective action.
- Write concise GoDoc for exported Go APIs. Python SDK public docstrings are user-facing Sphinx documentation.

## Commits

- Sign commits with `git commit -s`.
- Every commit must be authored, committed, and signed off by the **human submitter**, never by an agent/tool identity. Automated environments often default the git identity to the agent (for example `Claude <noreply@anthropic.com>`), which makes `git commit -s` sign off as the agent and violates the DCO requirement that the submitter attests to each commit. Before committing, set the repo-local identity to the submitter (`git config user.name` / `git config user.email`) and verify with `git log -1 --format='%an <%ae> / %(trailers:key=Signed-off-by)'` that both the author and the `Signed-off-by:` trailer are the submitter. When adding follow-up commits to a PR the submitter already authored, reuse that same identity (for example `git commit -C <original-commit>`); do not add a second, agent-identity sign-off.
- Do not add AI agents as commit co-authors.
- Follow [`CONTRIBUTING.md`](../../CONTRIBUTING.md) for DCO and PR conventions.

## Pull request watch policy

- After you successfully open a pull request (a `create_pull_request` call that
  succeeds), immediately call `subscribe_pr_activity` for that PR — passing its
  owner, repo, and number — before ending your turn. This hands the PR to the
  persistent watcher so review-comment feedback and CI status are tracked and
  acted on until the PR is merged or closed.
