from __future__ import annotations

import argparse

from cash_rec.pipeline import run_cash_reconciliation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Reconcile custody daily cash balances to Tradar daily cash balances.')
    parser.add_argument('--run-date', required=True, help='End date for reconciliation window in YYYY-MM-DD format.')
    parser.add_argument('--lookback-days', type=int, default=None, help='Number of business days to reconcile up to run date.')
    parser.add_argument('--tradar', help='Optional override path to Tradar file.')
    parser.add_argument('--citi', help='Optional override path to Citi file.')
    parser.add_argument('--bnp', help='Optional override path to BNP file.')
    parser.add_argument('--bnp-nz', dest='bnp_nz', help='Optional override path to BNP NZ file.')
    parser.add_argument('--mapping', help='Optional override path to mapping file.')
    parser.add_argument('--output', help='Optional output xlsx path.')
    parser.add_argument('--config', help='Optional YAML config override path.')
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = run_cash_reconciliation(
        run_date=args.run_date,
        lookback_days=args.lookback_days,
        tradar=args.tradar,
        citi=args.citi,
        bnp=args.bnp,
        bnp_nz=args.bnp_nz,
        mapping=args.mapping,
        output=args.output,
        config_path=args.config,
    )
    for source_name, path in result['file_paths'].items():
        print(f'Using {source_name} file: {path}')
    print(f"Run date: {result['run_date'].date()} | Lookback business days: {result['lookback_days']}")
    print(f"Tradar report period: {result['report_start'].date()} to {result['report_end'].date()}")
    print(f"Output written to {result['output_path']}")
    print(f"Rec rows: {len(result['rec_detail'])}")
    print(f"Unmapped custody rows: {len(result['unmapped'])}")
    print(f"Out-of-scope custody rows: {len(result['out_of_scope'])}")


if __name__ == '__main__':
    main()
