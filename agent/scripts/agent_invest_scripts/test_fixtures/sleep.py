"""Sleep helper used to verify wrapper-enforced timeouts."""

from __future__ import annotations

import argparse
import time


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, required=True)
    args = parser.parse_args()
    time.sleep(args.seconds)


if __name__ == "__main__":
    main()
