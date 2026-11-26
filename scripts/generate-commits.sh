#!/usr/bin/env bash
set -euo pipefail

API="http://127.0.0.1:8090/v1/chat/completions"
MODEL="qwen2-coder-14B"

EXCLUDE_EXT=()
EXCLUDE_DIR=()

usage() {
    cat <<EOF
Uso: $(basename "$0") [opciones]

Opciones:
  --exclude-ext EXT[,EXT...]   Exclude extension
  --exclude-dir DIR[,DIR...]   Exclude folders
  -h, --help                   Show help

Example:
  $(basename "$0") --exclude-ext log,md,json \
    --exclude-dir node_modules,dist,build
EOF
}

add_exclusions() {
    local -n target="$1"
    local value="$2"
    local item
    local -a items

    IFS=',' read -r -a items <<< "$value"

    for item in "${items[@]}"; do
        [[ -z "$item" ]] && continue
        target+=("$item")
    done
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --exclude-ext|--exclude-dir)
            if [[ $# -lt 2 || -z "$2" ]]; then
                echo "Error: $1 required value" >&2
                exit 1
            fi

            case "$1" in
                --exclude-ext)
                    add_exclusions EXCLUDE_EXT "$2"
                    ;;
                --exclude-dir)
                    add_exclusions EXCLUDE_DIR "$2"
                    ;;
            esac
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

should_exclude() {
    local file="$1"
    local ext dir
    local normalized

    for ext in "${EXCLUDE_EXT[@]}"; do
        ext="${ext#.}"

        if [[ "$file" == *."$ext" ]]; then
            return 0
        fi
    done

    for dir in "${EXCLUDE_DIR[@]}"; do
        normalized="${dir#./}"
        normalized="${normalized%/}"

        [[ -z "$normalized" ]] && continue

        # Exclude files
        if [[ "$file" == "$normalized/"* ]]; then
            return 0
        fi

        # Exclude folders
        if [[ "$file" == */"$normalized/"* ||
              "$file" == "$normalized/"* ]]; then
            return 0
        fi
    done

    return 1
}

while IFS= read -r -d '' file; do

    if should_exclude "$file"; then
        echo "⏭ Excluded: $file" >&2
        continue
    fi

    diff=$(git diff HEAD -- "$file")

    [[ -z "$diff" ]] && continue

    response=$(
        jq -n \
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
            }' |
        curl -fsS "$API" \
            -H "Content-Type: application/json" \
            -d @-
    )

    echo
    echo "$file"
    echo "$response" | jq -r '.choices[0].message.content'

done < <(git diff HEAD --name-only -z)
