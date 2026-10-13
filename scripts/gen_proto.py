"""Regenerate the Python protobuf module from proto/sparkplug_b.proto.

Run from the repository root:

    uv run --no-project --with grpcio-tools==1.84.0 scripts/gen_proto.py

The output is committed so that the services and tests need only the protobuf
runtime. Pass --check to exit non-zero when the committed file is stale.
"""

import argparse
import sys
import tempfile
from pathlib import Path

from grpc_tools import protoc

ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "proto"
OUT_FILE = ROOT / "src" / "sparkplug_pipeline" / "sparkplug" / "sparkplug_b_pb2.py"

HEADER = """\
# This file is generated. Do not edit it by hand.
# Source: proto/sparkplug_b.proto (written for this project from the
# Sparkplug B v3.0.0 specification).
# Command: uv run --no-project --with grpcio-tools==1.84.0 scripts/gen_proto.py
"""


def generate() -> str:
    with tempfile.TemporaryDirectory() as tmp:
        rc = protoc.main(
            [
                "protoc",
                f"-I{PROTO_DIR}",
                f"--python_out={tmp}",
                str(PROTO_DIR / "sparkplug_b.proto"),
            ]
        )
        if rc != 0:
            sys.exit(f"protoc failed with exit code {rc}")
        body = (Path(tmp) / "sparkplug_b_pb2.py").read_text(encoding="utf-8")
    return HEADER + body


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if the committed file differs")
    args = parser.parse_args()
    text = generate()
    if args.check:
        current = OUT_FILE.read_text(encoding="utf-8") if OUT_FILE.exists() else ""
        if current != text:
            sys.exit(f"{OUT_FILE.relative_to(ROOT)} is stale; rerun scripts/gen_proto.py")
        print("generated protobuf module is current")
        return
    OUT_FILE.write_text(text, encoding="utf-8", newline="\n")
    print(f"wrote {OUT_FILE.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
