#!/usr/bin/env bash

# Run the durable-execution suite against a built wheel, the way a consumer
# installs it.
#
# The editable development environment cannot host this suite. Temporaless
# depends on protovalidate, which needs a newer `buf.validate` than the one
# this SDK vendors from its attested contract bundle, and both packages own
# that top-level module. Installing medallion first and Temporaless second
# resolves it to the superset both packages work against; an editable checkout
# instead shadows it with the vendored projection.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
commit="${TEMPORALESS_COMMIT:?TEMPORALESS_COMMIT is required}"
requirement="temporaless @ git+https://github.com/jim-technologies/temporaless.git@${commit}#subdirectory=core/py"

command -v uv >/dev/null || {
  echo "uv is not on PATH; run inside 'flox activate'" >&2
  exit 1
}

venv="$(mktemp -d)"
cleanup() {
  rm -rf "$venv"
}
trap cleanup EXIT

cd "$root/python"
uv venv --quiet "$venv/env"
export VIRTUAL_ENV="$venv/env"

# Order is load-bearing: Temporaless must land after medallion so its complete
# buf.validate is the one both packages import.
uv pip install --quiet .
uv pip install --quiet "$requirement"

"$venv/env/bin/python" - <<'PY'
import buf.validate.validate_pb2 as validate

# Guard the ordering this script depends on. Without it the suite would fail
# far away, inside protovalidate, with an unrelated AttributeError.
if not hasattr(validate, "FieldPath"):
    raise SystemExit(
        "buf.validate resolved to the vendored projection, not Temporaless's "
        "complete copy; the ordered install in scripts/run_workflows_tests.sh "
        "did not take effect"
    )
PY

"$venv/env/bin/python" -m unittest discover tests_workflows
