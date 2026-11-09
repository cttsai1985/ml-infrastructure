#!/usr/bin/env python3
"""Execute a declared research batch against an approved Prefect endpoint."""

import argparse
import json
from pathlib import Path

from mlops_platform.batch import execute

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--config", type=Path, required=True)
parser.add_argument("--data", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
execute(json.loads(args.config.read_text()), args.data, args.output)
