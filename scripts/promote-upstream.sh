#!/usr/bin/env bash
set -euo pipefail

# Promote a fork branch to a pull request against the upstream repository.
#
# Opens (or reports, if one already exists) a cross-fork PR from
# ${FORK_REPO}:${BRANCH} into the upstream repo's base branch, using UPSTREAM_PAT
# (a human token with rights to open PRs on the upstream repo — the workflow's
# default GITHUB_TOKEN cannot write there).
#
# Change detection uses GitHub's server-side compare API rather than a local
# diff, so it works regardless of how shallow the checkout is and handles the
# cross-fork range directly.

UPSTREAM_REPO="${UPSTREAM_REPO:-kubeflow/pipelines}"
FORK_REPO="${FORK_REPO:?FORK_REPO is required}"
FORK_OWNER="${FORK_REPO%%/*}"
BRANCH="${BRANCH:?BRANCH is required}"
BASE_BRANCH="${BASE_BRANCH:-}"
PR_TITLE="${PR_TITLE:-}"
PR_BODY="${PR_BODY:-}"
DRAFT="${DRAFT:-false}"
REQUIRE_REVIEW="${REQUIRE_REVIEW:-true}"

if [[ -z "${UPSTREAM_PAT:-}" ]]; then
  echo "UPSTREAM_PAT is not set. Add a PAT with rights to open PRs on ${UPSTREAM_REPO} as a secret." >&2
  exit 1
fi
# gh authenticates from GH_TOKEN; use the upstream PAT so we can write to upstream.
export GH_TOKEN="${UPSTREAM_PAT}"

# Resolve the upstream default branch when a base is not given explicitly.
if [[ -z "${BASE_BRANCH}" ]]; then
  BASE_BRANCH="$(gh repo view "${UPSTREAM_REPO}" --json defaultBranchRef --jq '.defaultBranchRef.name')"
fi
echo "Promoting ${FORK_REPO}:${BRANCH} -> ${UPSTREAM_REPO}:${BASE_BRANCH}"

# Confirm the branch actually exists on the fork.
if ! git ls-remote --exit-code --heads "https://github.com/${FORK_REPO}.git" "${BRANCH}" >/dev/null 2>&1; then
  echo "Branch '${BRANCH}' does not exist on ${FORK_REPO}." >&2
  exit 1
fi

# Server-side compare of the cross-fork range. status is one of
# identical/ahead/behind/diverged; files lists the changed paths.
COMPARE_JSON="$(gh api "repos/${UPSTREAM_REPO}/compare/${BASE_BRANCH}...${FORK_OWNER}:${BRANCH}" 2>/dev/null || true)"
if [[ -z "${COMPARE_JSON}" ]]; then
  echo "Failed to compare ${UPSTREAM_REPO}:${BASE_BRANCH}...${FORK_OWNER}:${BRANCH}." >&2
  echo "Check that UPSTREAM_PAT can read both repositories and the branch is pushed." >&2
  exit 1
fi

COMPARE_STATUS="$(jq -r '.status' <<<"${COMPARE_JSON}")"
CHANGED_FILES="$(jq -r '.files[]?.filename' <<<"${COMPARE_JSON}")"

# Guard: refuse an empty promotion.
if [[ "${COMPARE_STATUS}" == "identical" || "${COMPARE_STATUS}" == "behind" || -z "${CHANGED_FILES}" ]]; then
  echo "Refusing to promote: '${BRANCH}' has no changes to contribute over ${UPSTREAM_REPO}:${BASE_BRANCH} (status: ${COMPARE_STATUS})." >&2
  exit 1
fi

# Guard: never leak fork-only automation into an upstream PR. These files exist
# only on the fork and would pollute the upstream diff.
FORK_ONLY_RE='(^|/)(pr-review\.yml|promote-upstream\.yml|review-pr\.sh|promote-upstream\.sh|AGENTS\.md)$'
LEAKED="$(grep -Ei "${FORK_ONLY_RE}" <<<"${CHANGED_FILES}" || true)"
if [[ -n "${LEAKED}" ]]; then
  echo "Refusing to promote: branch modifies fork-only automation files:" >&2
  echo "${LEAKED}" >&2
  exit 1
fi

# Optional gate: only promote a branch whose fork PR passed the review check.
# The PR Review workflow fails its check on a non-APPROVE verdict, so an
# all-green 'review' check means the automated reviewer signed off.
if [[ "${REQUIRE_REVIEW}" == "true" ]]; then
  PR_NUMBER="$(gh pr list --repo "${FORK_REPO}" --head "${BRANCH}" --state open \
    --json number --jq '.[0].number // empty')"
  if [[ -z "${PR_NUMBER}" ]]; then
    echo "require_review_pass is true but no open PR on ${FORK_REPO} targets '${BRANCH}'." >&2
    echo "Open a fork PR for this branch (so the review gate runs) or re-run with require_review_pass=false." >&2
    exit 1
  fi
  # `gh pr checks` exits non-zero when checks are failing/pending, so guard the
  # substitution with `|| true` and evaluate the states ourselves.
  REVIEW_STATES="$(gh pr checks "${PR_NUMBER}" --repo "${FORK_REPO}" --json name,state \
    --jq '.[] | select(.name | test("review"; "i")) | .state' 2>/dev/null || true)"
  if [[ -z "${REVIEW_STATES}" ]]; then
    echo "No review check found on fork PR #${PR_NUMBER}; cannot confirm the gate." >&2
    echo "Wait for 'PR Review' to run, or re-run with require_review_pass=false." >&2
    exit 1
  fi
  while IFS= read -r state; do
    [[ -z "${state}" ]] && continue
    if [[ "${state^^}" != "SUCCESS" ]]; then
      echo "Review gate is not green for fork PR #${PR_NUMBER} (found state: ${state})." >&2
      echo "Let the reviewer APPROVE, apply the override label and re-run it, or set require_review_pass=false." >&2
      exit 1
    fi
  done <<<"${REVIEW_STATES}"
  echo "Review gate green for fork PR #${PR_NUMBER}."
fi

# Idempotency: reuse an existing open upstream PR for this head branch.
EXISTING="$(gh pr list --repo "${UPSTREAM_REPO}" --head "${FORK_OWNER}:${BRANCH}" --state open \
  --json url --jq '.[0].url // empty')"
if [[ -n "${EXISTING}" ]]; then
  echo "Upstream PR already open for ${FORK_OWNER}:${BRANCH}: ${EXISTING}"
  exit 0
fi

# Default the title/body from the branch's tip commit when not provided.
TIP_MESSAGE="$(gh api "repos/${FORK_REPO}/commits/${BRANCH}" --jq '.commit.message' 2>/dev/null || true)"
if [[ -z "${PR_TITLE}" ]]; then
  PR_TITLE="$(head -n1 <<<"${TIP_MESSAGE}")"
fi
if [[ -z "${PR_TITLE}" ]]; then
  echo "Could not determine a PR title; pass one via the 'title' input." >&2
  exit 1
fi
if [[ -z "${PR_BODY}" ]]; then
  TIP_BODY="$(tail -n +2 <<<"${TIP_MESSAGE}")"
  PR_BODY="$(printf 'Promoted from %s:%s.\n\n%s' "${FORK_REPO}" "${BRANCH}" "${TIP_BODY}")"
fi

CREATE_ARGS=(
  --repo "${UPSTREAM_REPO}"
  --base "${BASE_BRANCH}"
  --head "${FORK_OWNER}:${BRANCH}"
  --title "${PR_TITLE}"
  --body "${PR_BODY}"
)
if [[ "${DRAFT}" == "true" ]]; then
  CREATE_ARGS+=(--draft)
fi

echo "Opening upstream PR..."
gh pr create "${CREATE_ARGS[@]}"
