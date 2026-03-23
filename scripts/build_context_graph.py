from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from context_graph.pipeline import build_context_graph_artifacts


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the context graph notebook artifacts.")
    parser.add_argument("--dataset-root", default=str(PROJECT_ROOT / "sap-o2c-data"))
    parser.add_argument("--output-root", default=str(PROJECT_ROOT / "artifacts"))
    args = parser.parse_args()

    result = build_context_graph_artifacts(
        dataset_root=Path(args.dataset_root),
        output_root=Path(args.output_root),
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
