#!/usr/bin/env bash

# Error-level Python checks.  Run with no arguments to check every tracked
# .py file, or pass specific paths (a pre-commit hook passes staged files).

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

if [[ $# -gt 0 ]]; then
    files=("$@")
else
    mapfile -t files < <(git ls-files '*.py')
fi

if [[ ${#files[@]} -eq 0 ]]; then
    echo "lint: no Python files to check"
    exit 0
fi

if ! command -v flake8 >/dev/null 2>&1; then
    echo "lint: flake8 not found, install with 'pip3 install flake8'" >&2
    exit 1
fi

status=0

# E9 covers SyntaxError, IndentationError and TabError; F63/F7/F82 cover
# undefined names and comparisons that always take one branch.  W191/E101
# catch a tab in the indentation of a space-indented file, which Python
# rejects with TabError once someone edits that block.
echo "== flake8 errors =="
flake8 --select=E9,F63,F7,F82,W191,E101 --show-source "${files[@]}" || status=1

# Advisory: pylint still reports about thirty defects that predate this
# script, so its exit status is deliberately dropped.
if command -v pylint >/dev/null 2>&1; then
    echo "== pylint errors (advisory) =="
    pylint --errors-only --score=n \
           --disable=import-error,no-name-in-module "${files[@]}" || true
else
    echo "== pylint not installed, skipped =="
fi

if [[ ${status} -eq 0 ]]; then
    echo "lint: clean (${#files[@]} files)"
fi

exit "${status}"
