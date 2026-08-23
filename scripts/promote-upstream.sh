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
# When set (e.g. 'master'), promote a *derived* branch = upstream tip + only the
# commits BRANCH adds over FORK_BASE, instead of BRANCH itself. The fork's
# default branch carries fork-only automation (pr-review.yml, this script,
# AGENTS.md, ...) that a fix branch cut from it would otherwise drag into the
# upstream diff; deriving keeps just the fix commits on a clean upstream base.
FORK_BASE="${FORK_BASE:-}"
# Name prefix for the derived branch pushed back to the fork.
DERIVED_PREFIX="${DERIVED_PREFIX:-promote}"

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

# The review gate is keyed off the fork PR, which lives on the branch as passed
# in — keep that name even after BRANCH is redirected to a derived branch below.
REVIEW_BRANCH="${BRANCH}"

# Optionally derive a clean upstream-based branch before promoting. This is the
# rebase/cherry-pick step promotion needs and that the compare API alone cannot
# do: replay only the commits BRANCH adds over FORK_BASE onto the current
# upstream tip, dropping the fork-only automation that FORK_BASE carries.
if [[ -n "${FORK_BASE}" ]]; then
  FORK_GIT="https://x-access-token:${UPSTREAM_PAT}@github.com/${FORK_REPO}.git"
  git config user.name "${GIT_AUTHOR_NAME:-Herik Webb}"
  git config user.email "${GIT_AUTHOR_EMAIL:-herikwebb@users.noreply.github.com}"

  git fetch --no-tags "https://github.com/${UPSTREAM_REPO}.git" "${BASE_BRANCH}"
  UPSTREAM_TIP="$(git rev-parse FETCH_HEAD)"
  git fetch --no-tags "${FORK_GIT}" "${FORK_BASE}"
  FORK_BASE_SHA="$(git rev-parse FETCH_HEAD)"
  git fetch --no-tags "${FORK_GIT}" "${BRANCH}"
  BRANCH_SHA="$(git rev-parse FETCH_HEAD)"

  # The fix commits are exactly what BRANCH adds over FORK_BASE, oldest first.
  mapfile -t FIX_COMMITS < <(git rev-list --reverse "${FORK_BASE_SHA}..${BRANCH_SHA}")
  if [[ "${#FIX_COMMITS[@]}" -eq 0 ]]; then
    echo "Refusing to promote: '${BRANCH}' has no commits beyond '${FORK_BASE}' to derive." >&2
    exit 1
  fi

  # Default the PR title/body from the PRIMARY (first, oldest) fix commit rather
  # than the branch tip. With more than one commit GitHub falls back to the
  # branch name for the title and an empty description; deriving from the main
  # fix commit keeps the upstream PR meaningful and encapsulates every commit.
  PRIMARY_SHA="${FIX_COMMITS[0]}"
  if [[ -z "${PR_TITLE}" ]]; then
    PR_TITLE="$(git log -1 --format='%s' "${PRIMARY_SHA}")"
  fi
  if [[ -z "${PR_BODY}" ]]; then
    PR_BODY="$(
      printf 'Description of your changes:\n\n'
      git log -1 --format='%b' "${PRIMARY_SHA}"
      if [[ "${#FIX_COMMITS[@]}" -gt 1 ]]; then
        printf '\nCommits in this PR:\n'
        for commit in "${FIX_COMMITS[@]}"; do
          printf -- '- %s\n' "$(git log -1 --format='%s' "${commit}")"
        done
      fi
      printf '\nChecklist:\n'
      printf -- '- [x] You have signed off your commits\n'
      printf -- '- [x] The PR title follows the repository title convention\n'
    )"
  fi

  DERIVED_BRANCH="${DERIVED_PREFIX}/${BRANCH}"
  echo "Deriving ${DERIVED_BRANCH} = ${UPSTREAM_REPO}:${BASE_BRANCH} + ${#FIX_COMMITS[@]} fix commit(s) from ${BRANCH}."
  git checkout -B "${DERIVED_BRANCH}" "${UPSTREAM_TIP}"
  # -s adds a Signed-off-by trailer for the configured (submitter) identity so
  # the promoted commits satisfy upstream's DCO check. Skip commits that already
  # carry a matching trailer so re-runs don't duplicate it.
  if ! git cherry-pick --signoff "${FIX_COMMITS[@]}"; then
    git cherry-pick --abort || true
    echo "Fix commits do not apply cleanly onto ${UPSTREAM_REPO}:${BASE_BRANCH}." >&2
    echo "Rebase the fix onto the current upstream tip and retry." >&2
    exit 1
  fi
  # The derived branch is fully script-managed and regenerated on every run, so
  # a plain force is safe and avoids force-with-lease failing on a fresh ref.
  git push --force "${FORK_GIT}" "HEAD:${DERIVED_BRANCH}"
  echo "Pushed derived branch ${FORK_REPO}:${DERIVED_BRANCH}."
  # Promote the derived branch from here on.
  BRANCH="${DERIVED_BRANCH}"
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
FORK_ONLY_RE='(^|/)(pr-review\.yml|promote-upstream\.yml|review-pr\.sh|review-pr-claude\.sh|promote-upstream\.sh|AGENTS\.md)$'
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
  PR_NUMBER="$(gh pr list --repo "${FORK_REPO}" --head "${REVIEW_BRANCH}" --state open \
    --json number --jq '.[0].number // empty')"
  if [[ -z "${PR_NUMBER}" ]]; then
    echo "require_review_pass is true but no open PR on ${FORK_REPO} targets '${REVIEW_BRANCH}'." >&2
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
