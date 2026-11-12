#!/usr/bin/env bash
set -euo pipefail

API="http://127.0.0.1:8060/v1/chat/completions"
MODEL="commit"

while IFS= read -r -d '' file; do
    echo
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Archivo: $file"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    diff=$(git diff HEAD -- "$file")

    [[ -z "$diff" ]] && continue

    response=$(jq -n \
        --arg model "$MODEL" \
        --arg diff "$diff" \
        '{
          model: $model,
          messages: [
            {
              role: "system",
              content: "Generate a concise Conventional Commit message for the provided Git diff. Return only the commit message."
            },
            {
              role: "user",
              content: $diff
            }
          ],
          temperature: 0.2,
          max_tokens: 128,
          stream: false
        }' | curl -fsS "$API" \
            -H "Content-Type: application/json" \
            -d @-)

    echo "$response" | jq -r '.choices[0].message.content'

done < <(git diff HEAD --name-only -z)
