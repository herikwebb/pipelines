#!/usr/bin/env bash
set -euo pipefail

echo "Reviewing PR #${PR_NUMBER} in ${REPO}"
echo "Base: ${BASE_SHA}"
echo "Head: ${HEAD_SHA}"

if [[ -z "${OPENAI_API_KEY:-}" ]]; then
  echo "OPENAI_API_KEY is not set. Add it as a GitHub Actions secret." >&2
  exit 1
fi

OPENAI_MODEL="${OPENAI_MODEL:-gpt-5.5}"
OPENAI_MAX_OUTPUT_TOKENS="${OPENAI_MAX_OUTPUT_TOKENS:-3000}"
DIFF_LIMIT_BYTES="${DIFF_LIMIT_BYTES:-120000}"

# Escape hatch: a maintainer-applied label lets a PR merge despite a
# CHANGES_REQUESTED verdict. The reviewer is an LLM and will occasionally flag
# correct or intentional code; without an override a fail-closed gate would
# block those PRs (and any automated promotion keyed on this check) forever.
# The review still runs and its comment is still posted for the record; only the
# exit status is relaxed.
OVERRIDE_LABEL="${REVIEW_GATE_OVERRIDE_LABEL:-override-review-gate}"
OVERRIDE="false"
if gh pr view "${PR_NUMBER}" --repo "${REPO}" --json labels \
    --jq '.labels[].name' 2>/dev/null | grep -qx "${OVERRIDE_LABEL}"; then
  OVERRIDE="true"
fi

# Prefer a three-dot (merge-base) range so the reviewer sees exactly what the PR
# would introduce on merge, not unrelated commits the base has moved ahead by. A
# two-dot range misreports files when the head branch is based on an older commit
# than the PR base (e.g. a fix branched off the upstream tip while the fork's main
# carries extra commits). Three-dot needs the merge base present locally; the
# workflow checks out with fetch-depth: 0 so it normally is, but fall back to a
# two-dot range on a shallow clone rather than erroring out.
if git merge-base "${BASE_SHA}" "${HEAD_SHA}" >/dev/null 2>&1; then
  DIFF_RANGE=("${BASE_SHA}...${HEAD_SHA}")
else
  echo "No merge base for ${BASE_SHA}...${HEAD_SHA}; falling back to two-dot range." >&2
  DIFF_RANGE=("${BASE_SHA}" "${HEAD_SHA}")
fi
CHANGED_FILES="$(git diff --name-only "${DIFF_RANGE[@]}")"
DIFF_STAT="$(git diff --stat "${DIFF_RANGE[@]}")"
DIFF_FILE="$(mktemp)"
PROMPT_FILE="$(mktemp)"
REVIEW_FILE="$(mktemp)"
VERDICT_FILE="$(mktemp)"
trap 'rm -f "${DIFF_FILE}" "${PROMPT_FILE}" "${REVIEW_FILE}" "${VERDICT_FILE}"' EXIT

git diff --find-renames --unified=80 "${DIFF_RANGE[@]}" > "${DIFF_FILE}"

DIFF_BYTES="$(wc -c < "${DIFF_FILE}" | tr -d ' ')"
if (( DIFF_BYTES > DIFF_LIMIT_BYTES )); then
  head -c "${DIFF_LIMIT_BYTES}" "${DIFF_FILE}" > "${DIFF_FILE}.truncated"
  mv "${DIFF_FILE}.truncated" "${DIFF_FILE}"
  {
    echo
    echo "[Diff truncated to ${DIFF_LIMIT_BYTES} bytes from ${DIFF_BYTES} bytes.]"
  } >> "${DIFF_FILE}"
fi

cat > "${PROMPT_FILE}" <<EOF
Review this GitHub pull request.

Repository: ${REPO}
Pull request: #${PR_NUMBER}
Base SHA: ${BASE_SHA}
Head SHA: ${HEAD_SHA}

Changed files:

\`\`\`
${CHANGED_FILES}
\`\`\`

Diff stat:

\`\`\`
${DIFF_STAT}
\`\`\`

Unified diff:

\`\`\`diff
$(cat "${DIFF_FILE}")
\`\`\`
EOF

python3 - "${PROMPT_FILE}" "${VERDICT_FILE}" > "${REVIEW_FILE}" <<'PY'
import json
import os
import sys
import urllib.error
import urllib.request

prompt_path = sys.argv[1]
verdict_path = sys.argv[2]

with open(prompt_path, "r", encoding="utf-8", errors="replace") as prompt_file:
    prompt = prompt_file.read()

model = os.environ["OPENAI_MODEL"]
max_output_tokens = int(os.environ.get("OPENAI_MAX_OUTPUT_TOKENS", "3000"))

payload = {
    "model": model,
    "instructions": (
        "You are an expert automated pull request reviewer. "
        "Find concrete correctness, security, reliability, performance, and test-coverage issues. "
        "Prioritize actionable findings tied to changed code. "
        "If you find issues, return a concise Markdown review with severity, file path, and rationale. "
        "If you do not find any issues, say that no automated findings were found and mention residual risks briefly. "
        "Do not invent line numbers when the diff does not provide enough context. "
        "End your reply with a final line containing exactly 'VERDICT: APPROVE' when there are no "
        "actionable findings that should block merge, or 'VERDICT: CHANGES_REQUESTED' when there are. "
        "Emit that verdict line last, on its own line, with nothing after it."
    ),
    "input": prompt,
    "max_output_tokens": max_output_tokens,
}

request = urllib.request.Request(
    "https://api.openai.com/v1/responses",
    data=json.dumps(payload).encode("utf-8"),
    headers={
        "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
        "Content-Type": "application/json",
    },
    method="POST",
)

try:
    with urllib.request.urlopen(request, timeout=120) as response:
        data = json.loads(response.read().decode("utf-8"))
except urllib.error.HTTPError as error:
    detail = error.read().decode("utf-8", errors="replace")
    print(f"OpenAI API request failed with HTTP {error.code}: {detail}", file=sys.stderr)
    raise SystemExit(1)
except urllib.error.URLError as error:
    print(f"OpenAI API request failed: {error}", file=sys.stderr)
    raise SystemExit(1)

review = data.get("output_text")
if not review:
    parts = []
    for item in data.get("output", []):
        for content in item.get("content", []):
            text = content.get("text")
            if text:
                parts.append(text)
    review = "\n".join(parts)

review = (review or "").strip()
if not review:
    print("OpenAI API response did not include review text.", file=sys.stderr)
    raise SystemExit(1)

# Split the machine-readable verdict off the human-facing review. Fail closed:
# a missing or unrecognized verdict is treated as CHANGES_REQUESTED so an
# ambiguous review never auto-approves a promotion.
verdict = "CHANGES_REQUESTED"
body_lines = []
for line in review.splitlines():
    stripped = line.strip()
    if stripped.upper().startswith("VERDICT:"):
        value = stripped.split(":", 1)[1].strip().upper()
        if value in ("APPROVE", "CHANGES_REQUESTED"):
            verdict = value
        continue
    body_lines.append(line)

with open(verdict_path, "w", encoding="utf-8") as verdict_file:
    verdict_file.write(verdict)

print("\n".join(body_lines).strip())
PY

REVIEW="$(cat "${REVIEW_FILE}")"
VERDICT="$(cat "${VERDICT_FILE}" 2>/dev/null || echo CHANGES_REQUESTED)"
[[ -n "${VERDICT}" ]] || VERDICT="CHANGES_REQUESTED"

OVERRIDE_NOTE=""
if [[ "${OVERRIDE}" == "true" ]]; then
  OVERRIDE_NOTE="
Gate override: \`${OVERRIDE_LABEL}\` label present — merge not blocked by this verdict."
fi

BODY=$(cat <<EOF
## Automated PR Review

Model: \`${OPENAI_MODEL}\`
Verdict: \`${VERDICT}\`${OVERRIDE_NOTE}

Changed files:

\`\`\`
${CHANGED_FILES}
\`\`\`

Review result:

${REVIEW}
EOF
)

gh pr comment "${PR_NUMBER}" \
  --repo "${REPO}" \
  --body "${BODY}"

# Gate: a non-APPROVE verdict fails the check so "reviewer signed off" == the
# review check is green. This is what lets an automated promotion flow key off
# CI status rather than string-matching the comment body.
echo "Automated review verdict: ${VERDICT}"
if [[ "${VERDICT}" != "APPROVE" ]]; then
  if [[ "${OVERRIDE}" == "true" ]]; then
    echo "Verdict is ${VERDICT}, but the '${OVERRIDE_LABEL}' label is present; overriding the gate."
    exit 0
  fi
  echo "::error::Automated reviewer requested changes; see the PR review comment." >&2
  echo "::error::To merge anyway, apply the '${OVERRIDE_LABEL}' label and re-run this check." >&2
  exit 1
fi
