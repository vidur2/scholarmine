"""Command-line interface for ScholarMine."""

import argparse
import logging
import os
import sys

from .runner import CSVResearcherRunner

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("stem").setLevel(logging.WARNING)


def main() -> None:
    """Main entry point for the scholarmine CLI."""
    parser = argparse.ArgumentParser(
        description="ScholarMine - Google Scholar scraper using Tor for IP rotation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  scholarmine researchers.csv
  scholarmine researchers.csv --max-threads 5
  scholarmine researchers.csv --max-requests-per-ip 5 --output-dir ./output
  scholarmine researchers.csv --continue
        """,
    )
    parser.add_argument(
        "csv_file",
        help="Path to CSV file containing researcher name and Google Scholar URL",
    )
    parser.add_argument(
        "--max-threads",
        type=int,
        default=10,
        help="Maximum number of concurrent threads (default: 10)",
    )
    parser.add_argument(
        "--max-requests-per-ip",
        type=int,
        default=10,
        help="Maximum requests per IP before rotating (default: 10)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="Researcher_Profiles",
        help="Output directory for researcher profiles (default: Researcher_Profiles)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum retry attempts per researcher before giving up (default: 5)",
    )
    parser.add_argument(
        "--continue",
        action="store_true",
        dest="continue_scraping",
        help="Continue scraping from the latest log file, processing only unsuccessful or pending researchers",
    )
    parser.add_argument(
        "--log-dir",
        type=str,
        default=None,
        help=(
            "Log directory for this session (advanced). "
            "Pin logs to a specific folder instead of auto-generating one — "
            "useful when running multiple parallel jobs. "
            "With --continue, resumes from this exact directory."
        ),
    )

    args = parser.parse_args()

    if not args.csv_file.lower().endswith(".csv"):
        print("Error: Only CSV files are supported!")
        sys.exit(1)

    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file not found: {args.csv_file}")
        sys.exit(1)

    runner = None
    try:
        continue_from_log = None
        log_dir = args.log_dir

        if args.continue_scraping:
            if log_dir:
                continue_from_log = log_dir
            else:
                continue_from_log = CSVResearcherRunner.find_latest_log_directory()
            if not continue_from_log:
                print("Error: No previous log directories found for --continue mode!")
                print(
                    "Run the scraper at least once without --continue to create "
                    "initial logs."
                )
                sys.exit(1)

            progress_file = os.path.join(continue_from_log, "scraping_progress.json")
            if not os.path.exists(progress_file):
                print(f"Error: Progress file not found in {continue_from_log}")
                print("The log directory may be corrupted or incomplete.")
                sys.exit(1)

            print(f"Continue mode: Using log directory: {continue_from_log}")

        runner = CSVResearcherRunner(
            csv_file=args.csv_file,
            max_threads=args.max_threads,
            max_requests_per_ip=args.max_requests_per_ip,
            output_dir=args.output_dir,
            continue_from_log=continue_from_log,
            log_dir=log_dir if not args.continue_scraping else None,
            max_retries=args.max_retries,
        )

        runner.process_researchers_from_csv()


        print("\nCSV scraping session completed!")

    except KeyboardInterrupt:
        print("\n\nCSV scraping interrupted by user!")
        if runner:
            runner.cleanup_tor()
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error during CSV scraping: {e}")
        if runner:
            runner.cleanup_tor()
        sys.exit(1)
    finally:
        if runner:
            runner.cleanup_tor()


if __name__ == "__main__":
    main()
