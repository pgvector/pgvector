"""Test/benchmark launcher only: all scheduling and search execute in C."""

import argparse
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=10000)
    parser.add_argument('--dimensions', type=int, default=32)
    parser.add_argument('--requests', type=int, default=1000)
    parser.add_argument('--batch-rows', type=int, default=64)
    parser.add_argument('--batch-delay-ms', type=float, default=.1)
    parser.add_argument('--pending', type=int, default=256)
    parser.add_argument('--survivor-fraction', type=float, default=.01)
    parser.add_argument('--mode', choices=['shared', 'unique'], default='shared')
    parser.add_argument('--sequential', action='store_true')
    args = parser.parse_args()
    if not 0 <= args.survivor_fraction <= 1 or args.batch_delay_ms < 0:
        parser.error('invalid survivor fraction or delay')
    root = Path(__file__).resolve().parents[2]
    subprocess.run(['make', '-C', str(root / 'ann'), 'ann-batch-bench'],
                   check=True, stdout=subprocess.DEVNULL)
    values = [args.rows, args.dimensions, args.requests,
              int(args.rows * args.survivor_fraction), args.batch_rows,
              args.pending, int(args.batch_delay_ms * 1000),
              int(args.mode == 'unique'), int(args.sequential)]
    subprocess.run([str(root / 'ann/ann-batch-bench'), *map(str, values)], check=True)


if __name__ == '__main__':
    main()
