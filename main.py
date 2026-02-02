import argparse
from pipeline_lib.logging_config import setup_logging
from pipeline_lib.rawdata_fetch import generate_rawdata_snapshot, compare_rawdata_snapshots
from pipeline_lib.transform_rawdata import transform_enqueued_items
from pipeline_lib.olap_sync import olap_sync
from pipeline_lib.powerbi import powerbi_refresh
from pipeline_lib.cqr import cqr

# --- Setup loggers
setup_logging()

# --- Main function
def main():
    parser = argparse.ArgumentParser(description="Run steps of the QUALITY PIPELINE.")
    group = parser.add_mutually_exclusive_group(required=True)
    
    group.add_argument('--auto', action='store_true', help='Run all steps of the pipeline')
    group.add_argument('--snapshot', action='store_true', help='Only generate the raw data snapshot')
    group.add_argument('--enqueue', action='store_true', help='Only compare snapshots and enqueue items')
    group.add_argument('--transform', action='store_true', help='Only transform enqueued items')
    group.add_argument('--olap', action='store_true', help='Only sync OLAP reports')
    group.add_argument('--pbi', action='store_true', help='Only refresh Power BI dataset')
    group.add_argument('--cqr', action='store_true', help='Only run the CQR process')

    parser.add_argument(
        '--week',
        type=str,
        help='Target week for CQR (e.g. 2025-01-16)'
    )

    args = parser.parse_args()

    if args.week and not args.cqr:
        parser.error("--week can only be used together with --cqr")

    print("QUALITY PIPELINE - Iteration Started")

    if args.auto:
        generate_rawdata_snapshot()
        compare_rawdata_snapshots()
        transform_enqueued_items()
        success_count = olap_sync()
        powerbi_refresh() if success_count > 0 else print("Power BI refresh skipped due to no OLAP updates.")
        cqr() if success_count > 0 else print("CQR process skipped due to no OLAP updates.")
    elif args.snapshot:
        generate_rawdata_snapshot()
    elif args.enqueue:
        compare_rawdata_snapshots()
    elif args.transform:
        transform_enqueued_items()
    elif args.olap:
        olap_sync()
    elif args.pbi:
        powerbi_refresh()
    elif args.cqr:
        cqr(week=args.week) 

    print("QUALITY PIPELINE - Iteration Ended")

if __name__ == "__main__":
    main()