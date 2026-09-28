#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
commit="${TEMPORALESS_COMMIT:?TEMPORALESS_COMMIT is required}"
requirement="temporaless @ git+https://github.com/jim-technologies/temporaless.git@${commit}#subdirectory=core/py"
version="$(tr -d '\n' <"$root/VERSION")"
wheel="$root/python/dist/medallion-${version}-py3-none-any.whl"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

uv export --project "$root/python" --locked --no-dev --no-emit-project --format requirements-txt >"$tmp/base.txt"
uv export --project "$root/python" --locked --no-dev --extra workflows --no-emit-project --format requirements-txt >"$tmp/workflows.txt"
uv venv --quiet "$tmp/base"
uv pip sync --python "$tmp/base/bin/python" "$tmp/base.txt"
uv pip install --python "$tmp/base/bin/python" --no-deps "$wheel"
cd "$tmp"

"$tmp/base/bin/python" -I - "$root" <<'PY'
from importlib.metadata import distribution
from pathlib import Path
import sys

import buf.validate.validate_pb2 as validation
import medallion
from medallion import MedallionClient, MedallionError
from medallion.connect.v1 import connect_pb2
from medallion.ingest.v1 import ingest_pb2

assert not Path(medallion.__file__).is_relative_to(Path(sys.argv[1]))
assert not any(str(file).startswith("buf/") for file in distribution("medallion").files)
assert hasattr(validation, "FieldPath")
field = connect_pb2.PublishCdcEventsRequest.DESCRIPTOR.fields_by_name["connector_id"]
assert field.GetOptions().Extensions[validation.field].string.min_len == 1
request = ingest_pb2.GetTableRequest(name="tables/fixture")
assert ingest_pb2.GetTableRequest.FromString(request.SerializeToString()) == request
client = MedallionClient(base_url="http://127.0.0.1", api_key="fixture-key",
                         workspace_id="ws_01jz9q5g6rsf7r5ar4rah1b2c3")
try:
    client.workflows.store()
except MedallionError as error:
    assert error.code == "MEDALLION_TEMPORALESS_REQUIRED"
else:
    raise AssertionError("base installation unexpectedly includes Temporaless")
PY

uv venv --quiet "$tmp/workflows"
python="$tmp/workflows/bin/python"
uv pip sync --python "$python" "$tmp/workflows.txt"
uv pip install --python "$python" --no-deps "$wheel"

prove_workflows() {
  "$python" -I - <<'PY'
from importlib.metadata import distributions
import buf.validate.validate_pb2 as validation

owners = [dist.metadata["Name"] for dist in distributions()
          if "buf/validate/validate_pb2.py" in {str(file) for file in dist.files or ()}]
assert owners == ["bufbuild-protovalidate-protocolbuffers-python"], (
    f"unexpected buf.validate owners: {owners}; rebuild the environment or uninstall "
    "all legacy owners before installing new packages (python/README.md)"
)
assert hasattr(validation, "FieldPath")
PY
  "$python" -I -m unittest discover -s "$root/python/tests" -p test_workflows.py
}

# Temporaless is already installed from the lock; the SDK lands last.
prove_workflows
uv pip uninstall --python "$python" medallion
"$python" -I -c 'import buf.validate.validate_pb2; from temporaless.connectstore import ConnectStore'
uv pip install --python "$python" --no-deps "$wheel"
uv pip uninstall --python "$python" temporaless
"$python" -I - <<'PY'
import importlib.util
from medallion import MedallionClient
from medallion.connect.v1 import connect_pb2
import buf.validate.validate_pb2 as validation

assert importlib.util.find_spec("temporaless") is None
assert hasattr(validation, "FieldPath")
assert connect_pb2.PublishCdcEventsRequest(connector_id="fixture").SerializeToString()
PY
# Now install Temporaless last. It must not replace or remove shared files.
uv pip install --python "$python" --no-deps "$requirement"
prove_workflows
uv pip check --python "$python"

# Reproduce the old co-owned namespace using the actual prior distributions.
cd "$root"
mkdir "$tmp/legacy"
git archive ea9c31c80bf0d91842956bc91ab8439b05ed0805 python | tar -x -C "$tmp/legacy"
uv build --wheel --out-dir "$tmp/legacy-dist" "$tmp/legacy/python"
uv export --project "$root/python" --locked --no-dev --extra workflows --no-emit-project \
  --no-emit-package temporaless \
  --no-emit-package bufbuild-protovalidate-protocolbuffers-python \
  --no-emit-package bufbuild-protovalidate-protocolbuffers-pyi \
  --format requirements-txt >"$tmp/legacy.txt"
uv venv --quiet --seed "$tmp/upgrade"
python="$tmp/upgrade/bin/python"
uv pip install --python "$python" --require-hashes -r "$tmp/legacy.txt"
cd "$tmp"
"$python" -m pip install --no-deps "$tmp/legacy-dist/medallion-0.4.0-py3-none-any.whl"
"$python" -m pip install --no-deps \
  'temporaless @ git+https://github.com/jim-technologies/temporaless.git@bcb66841f67718df0c04204d3d2f6ad4101ae0ae#subdirectory=core/py'
"$python" -I - <<'PY'
from importlib.metadata import distributions
from temporaless.connectstore import ConnectStore
from medallion.connect.v1 import connect_pb2

owners = {dist.metadata["Name"] for dist in distributions()
          if "buf/validate/validate_pb2.py" in {str(file) for file in dist.files or ()}}
assert owners == {"medallion", "temporaless"}, owners
PY
# All old owners must leave before pip installs the new shared dependencies.
"$python" -m pip uninstall -y medallion temporaless
"$python" -m pip install "${wheel}[workflows]"
prove_workflows
"$python" -m pip check
echo "Clean base/workflow wheels, new install orders, ownership and legacy migration passed."
