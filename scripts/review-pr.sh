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

CHANGED_FILES="$(git diff --name-only "${BASE_SHA}" "${HEAD_SHA}")"
DIFF_STAT="$(git diff --stat "${BASE_SHA}" "${HEAD_SHA}")"
DIFF_FILE="$(mktemp)"
PROMPT_FILE="$(mktemp)"
REVIEW_FILE="$(mktemp)"
trap 'rm -f "${DIFF_FILE}" "${PROMPT_FILE}" "${REVIEW_FILE}"' EXIT

git diff --find-renames --unified=80 "${BASE_SHA}" "${HEAD_SHA}" > "${DIFF_FILE}"

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

python3 - "${PROMPT_FILE}" > "${REVIEW_FILE}" <<'PY'
import json
import os
import sys
import urllib.error
import urllib.request

prompt_path = sys.argv[1]

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
        "Do not invent line numbers when the diff does not provide enough context."
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

print(review)
PY

REVIEW="$(cat "${REVIEW_FILE}")"

BODY=$(cat <<EOF
## Automated PR Review

Model: \`${OPENAI_MODEL}\`

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
