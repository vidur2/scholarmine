"""CSV-based batch processing runner for Scholar scraping."""

import atexit
import concurrent.futures
import csv
import json
import logging
import os
import queue
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import types
from datetime import datetime

from .ip_tracker import IPTracker
from .scraper import TorScholarSearch

logger = logging.getLogger(__name__)

TOR_SOCKS_PORT = 9150
TOR_CONTROL_PORT = 9151
TOR_STARTUP_TIMEOUT_SECONDS = 30
RETRY_WAIT_SECONDS = 1
THREAD_STAGGER_DELAY_SECONDS = 2
QUEUE_TIMEOUT_SECONDS = 5.0
MAIN_LOOP_SLEEP_SECONDS = 10
PROGRESS_UPDATE_INTERVAL_SECONDS = 30
THREAD_JOIN_TIMEOUT_SECONDS = 30
DEFAULT_MAX_RETRIES = 5
SCRAPE_ATTEMPT_TIMEOUT_SECONDS = 240
MAX_IP_RETRIES = 10
STALE_PROGRESS_TIMEOUT_SECONDS = 600
MAX_STALE_RESTARTS = 3
TOR_RESTART_DELAY_SECONDS = 5


class CSVResearcherRunner:
    """Batch processor for scraping multiple researchers from a CSV file."""

    @staticmethod
    def find_latest_log_directory() -> str | None:
        """Find the latest log directory in the logs folder.

        Returns:
            Path to the latest log directory, or None if not found.
        """
        logs_base_dir = "logs"
        if not os.path.exists(logs_base_dir):
            return None

        run_dirs = []
        for item in os.listdir(logs_base_dir):
            item_path = os.path.join(logs_base_dir, item)
            if os.path.isdir(item_path) and item.startswith("run_"):
                run_dirs.append(item_path)

        if not run_dirs:
            return None

        run_dirs.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        return run_dirs[0]

    @staticmethod
    def load_progress_from_log(log_dir: str) -> dict | None:
        """Load progress data from existing log directory.

        Args:
            log_dir: Path to the log directory.

        Returns:
            Progress data dictionary, or None if not found.
        """
        progress_file = os.path.join(log_dir, "scraping_progress.json")
        if not os.path.exists(progress_file):
            return None

        try:
            with open(progress_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load progress from {progress_file}: {e}")
            return None

    def __init__(
        self,
        csv_file: str,
        max_threads: int = 10,
        max_requests_per_ip: int = 10,
        output_dir: str | None = None,
        continue_from_log: str | None = None,
        log_dir: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        """Initialize the CSV researcher runner.

        Args:
            csv_file: Path to CSV file with researcher data.
            max_threads: Maximum concurrent threads. Defaults to 10.
            max_requests_per_ip: Max requests per IP before rotation. Defaults to 10.
            output_dir: Output directory for profiles. Defaults to "Researcher_Profiles".
            continue_from_log: Path to log directory to continue from.
            log_dir: Pin logs to this directory instead of auto-generating a timestamped one.
            max_retries: Max retry attempts per researcher before giving up. Defaults to 5.
        """
        self.csv_file = csv_file
        self.max_threads = max_threads
        self.max_requests_per_ip = max_requests_per_ip
        self.max_retries = max_retries
        self.output_dir = output_dir or "Researcher_Profiles"
        self.results_lock = threading.Lock()
        self.print_lock = threading.Lock()
        self.continue_mode = continue_from_log is not None

        if continue_from_log:
            self.logs_dir = continue_from_log
            logger.info(f"Continue mode: Using existing log directory: {self.logs_dir}")
        elif log_dir:
            self.logs_dir = log_dir
            os.makedirs(self.logs_dir, exist_ok=True)
        else:
            self.session_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.logs_dir = os.path.join("logs", f"run_{self.session_timestamp}")
            os.makedirs(self.logs_dir, exist_ok=True)

        self._setup_file_logging()

        ip_tracker_file = os.path.join(self.logs_dir, "ip_usage_tracker.json")
        self.ip_tracker = IPTracker(ip_tracker_file)

        self.tor_process = None
        self.tor_started_by_script = False
        atexit.register(self.cleanup_tor)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.progress_file = os.path.join(self.logs_dir, "scraping_progress.json")
        self.progress_lock = threading.Lock()

        if self.continue_mode:
            existing_progress = self.load_progress_from_log(self.logs_dir)
            if existing_progress:
                self.progress_data = existing_progress
                logger.info(
                    f"Continue mode: Loaded existing progress - "
                    f"{len(existing_progress.get('success', []))} successful, "
                    f"{len(existing_progress.get('pending', []))} pending"
                )
            else:
                logger.warning(
                    "Continue mode: Could not load existing progress, starting fresh"
                )
                self.progress_data = self._create_empty_progress_data()
        else:
            self.progress_data = self._create_empty_progress_data()

        self.researcher_queue: queue.Queue = queue.Queue()
        self.queue_lock = threading.Lock()
        self._active_workers = 0
        self._active_workers_lock = threading.Lock()

        if not self.start_tor_service():
            raise RuntimeError(
                "Failed to start Tor service. Please ensure Tor is installed "
                f"and not already running on port {TOR_CONTROL_PORT}."
            )

    def _setup_file_logging(self) -> None:
        """Add a timestamped file handler to the root logger for this session."""
        log_file = os.path.join(self.logs_dir, "scholarmine.log")
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logging.getLogger().addHandler(file_handler)

    def _create_empty_progress_data(self) -> dict:
        """Create an empty progress data structure."""
        return {
            "session_start": None,
            "last_updated": None,
            "total_researchers": 0,
            "pending": [],
            "success": [],
            "failed_retrying": [],
            "counts": {
                "pending": 0,
                "success": 0,
                "failed_retrying": 0,
            },
        }

    def start_tor_service(self) -> bool:
        """Start Tor service with the required configuration.

        Returns:
            True if Tor is running, False otherwise.
        """
        try:
            if self.check_tor_running():
                logger.info("Tor is already running - skipping startup")
                return True

            logger.info(
                f"Starting Tor service with control port {TOR_CONTROL_PORT} "
                "and cookie authentication disabled..."
            )

            self._tor_data_dir = tempfile.mkdtemp(prefix="scholarmine-tor-")
            empty_torrc = os.path.join(self._tor_data_dir, "empty_torrc")
            open(empty_torrc, "w").close()
            self.tor_process = subprocess.Popen(
                [
                    "tor",
                    "-f", empty_torrc,
                    "--defaults-torrc", empty_torrc,
                    "--ControlPort", str(TOR_CONTROL_PORT),
                    "--CookieAuthentication", "0",
                    "--DataDirectory", self._tor_data_dir,
                    "--SocksPort", str(TOR_SOCKS_PORT),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.tor_started_by_script = True
            logger.info(f"Tor process started with PID: {self.tor_process.pid}")

            startup_timeout = TOR_STARTUP_TIMEOUT_SECONDS
            for i in range(startup_timeout):
                if self.tor_process.poll() is not None:
                    stdout = self.tor_process.stdout.read() if self.tor_process.stdout else ""
                    stderr = self.tor_process.stderr.read() if self.tor_process.stderr else ""
                    logger.error(f"Tor process exited early with code {self.tor_process.returncode}")
                    if stdout:
                        logger.error(f"Tor stdout: {stdout.strip()}")
                    if stderr:
                        logger.error(f"Tor stderr: {stderr.strip()}")
                    return False
                if self.check_tor_running():
                    logger.info(f"Tor is ready after {i+1} seconds")
                    return True
                time.sleep(1)

            stderr = ""
            if self.tor_process.stderr:
                try:
                    stderr = self.tor_process.stderr.read()
                except Exception:
                    pass
            logger.error(f"Tor failed to start within {startup_timeout} seconds")
            if stderr:
                logger.error(f"Tor stderr: {stderr.strip()}")
            self.stop_tor_service()
            return False

        except Exception as e:
            logger.error(f"Failed to start Tor service: {e}")
            if self.tor_process:
                self.stop_tor_service()
            return False

    def stop_tor_service(self) -> None:
        """Stop the Tor service if it was started by this script."""
        if self.tor_process and self.tor_started_by_script:
            try:
                logger.info("Stopping Tor service...")
                self.tor_process.terminate()

                try:
                    self.tor_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.warning(
                        "Tor didn't stop gracefully, forcing termination..."
                    )
                    self.tor_process.kill()
                    self.tor_process.wait()

                logger.info("Tor service stopped")
                self.tor_process = None
                self.tor_started_by_script = False

            except Exception as e:
                logger.error(f"Error stopping Tor service: {e}")

    def check_tor_running(self) -> bool:
        """Check if Tor is running and accessible on the control port.

        Uses a thread with timeout to prevent hanging if Tor is unresponsive.

        Returns:
            True if Tor is accessible, False otherwise.
        """
        try:
            from stem.control import Controller

            def _check():
                with Controller.from_port(port=TOR_CONTROL_PORT) as controller:
                    controller.authenticate()

            executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            future = executor.submit(_check)
            try:
                future.result(timeout=15)
                return True
            except Exception:
                return False
            finally:
                executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            return False

    def cleanup_tor(self) -> None:
        """Cleanup method called on exit."""
        self.stop_tor_service()

    def signal_handler(self, signum: int, frame: types.FrameType | None) -> None:
        """Handle interrupt signals.

        Args:
            signum: Signal number.
            frame: Current stack frame.
        """
        logger.info(f"Received signal {signum}, cleaning up...")
        self.cleanup_tor()
        sys.exit(0)

    def extract_scholar_id_from_url(self, google_scholar_url: str) -> str | None:
        """Extract Google Scholar ID from the URL.

        Args:
            google_scholar_url: Full Google Scholar profile URL.

        Returns:
            Scholar ID or None if not found.
        """
        if not google_scholar_url or "citations?user=" not in google_scholar_url:
            return None

        try:
            match = re.search(r"user=([^&]+)", google_scholar_url)
            if match:
                return match.group(1)
        except Exception as e:
            logger.warning(
                f"Failed to extract Scholar ID from URL {google_scholar_url}: {e}"
            )

        return None

    def read_csv_file(self) -> dict[str, str]:
        """Read researchers from CSV file and extract Scholar IDs.

        Returns:
            Dictionary mapping researcher names to Scholar IDs.
        """
        try:
            researchers_data = {}
            with open(self.csv_file, "r", encoding="utf-8") as f:
                csv_reader = csv.DictReader(f)

                required_columns = {"name", "google_scholar_url"}
                missing = required_columns - set(csv_reader.fieldnames or [])
                if missing:
                    logger.error(
                        f"CSV file is missing required columns: {missing}. "
                        f"Found columns: {csv_reader.fieldnames}"
                    )
                    return {}

                for row in csv_reader:
                    name = row.get("name", "").strip()
                    google_scholar_url = row.get("google_scholar_url", "").strip()

                    if not name or not google_scholar_url:
                        continue

                    scholar_id = self.extract_scholar_id_from_url(google_scholar_url)
                    if scholar_id:
                        researchers_data[name] = scholar_id
                    else:
                        logger.warning(
                            f"Could not extract Scholar ID from URL for {name}: "
                            f"{google_scholar_url}"
                        )

            logger.info(
                f"Read {len(researchers_data)} researchers with valid Scholar IDs "
                f"from {self.csv_file}"
            )
            return researchers_data

        except FileNotFoundError:
            logger.error(f"CSV file not found: {self.csv_file}")
            return {}
        except Exception as e:
            logger.error(f"Error reading CSV file {self.csv_file}: {e}")
            return {}

    def initialize_progress_tracking(self, researchers: list[str]) -> None:
        """Initialize progress tracking with all researchers as pending.

        Args:
            researchers: List of researcher names.
        """
        with self.progress_lock:
            self.progress_data["session_start"] = datetime.now().isoformat()
            self.progress_data["last_updated"] = datetime.now().isoformat()
            self.progress_data["total_researchers"] = len(researchers)
            self.progress_data["pending"] = list(researchers)
            self.progress_data["success"] = []
            self.progress_data["failed_retrying"] = []
            self.progress_data["failed_exhausted"] = []
            self.progress_data["counts"] = {
                "pending": len(researchers),
                "success": 0,
                "failed_retrying": 0,
                "failed_exhausted": 0,
            }
            self._write_progress_file()

    def update_researcher_status(self, researcher_name: str, new_status: str) -> None:
        """Update a researcher's status and write to file immediately.

        Args:
            researcher_name: Name of the researcher.
            new_status: New status ('success', 'failed_retrying', 'failed_exhausted', 'pending').
        """
        with self.progress_lock:
            for status_list in [
                "pending",
                "success",
                "failed_retrying",
                "failed_exhausted",
            ]:
                if researcher_name in self.progress_data.get(status_list, []):
                    self.progress_data[status_list].remove(researcher_name)

            if new_status == "success":
                self.progress_data["success"].append(researcher_name)
            elif new_status == "failed_retrying":
                self.progress_data["failed_retrying"].append(researcher_name)
            elif new_status == "failed_exhausted":
                self.progress_data.setdefault("failed_exhausted", []).append(
                    researcher_name
                )
            elif new_status == "pending":
                self.progress_data["pending"].append(researcher_name)

            self.progress_data["counts"] = {
                "pending": len(self.progress_data.get("pending", [])),
                "success": len(self.progress_data.get("success", [])),
                "failed_retrying": len(self.progress_data.get("failed_retrying", [])),
                "failed_exhausted": len(self.progress_data.get("failed_exhausted", [])),
            }

            self.progress_data["last_updated"] = datetime.now().isoformat()
            self._write_progress_file()

    def _write_progress_file(self) -> None:
        """Write progress data to file (called with lock already held)."""
        try:
            with open(self.progress_file, "w", encoding="utf-8") as f:
                json.dump(self.progress_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to write progress file: {e}")

    def print_current_progress(self) -> None:
        """Log current progress status."""
        with self.progress_lock:
            counts = self.progress_data["counts"]
            total = self.progress_data["total_researchers"]
            queue_size = self.researcher_queue.qsize()

            logger.info("CURRENT PROGRESS:")
            logger.info(f"  Total researchers: {total}")
            logger.info(f"  Successfully scraped: {counts['success']}")
            logger.info(f"  In queue (pending/retrying): {queue_size}")
            logger.info(f"  Currently retrying: {counts['failed_retrying']}")
            logger.info(f"  Success rate: {(counts['success'] / total * 100):.1f}%")
            logger.info(f"  Last updated: {self.progress_data['last_updated']}")

            if queue_size == 0 and counts["failed_retrying"] == 0:
                logger.info("  All researchers completed successfully!")

    def _run_single_researcher_scrape_by_scholar_id(
        self,
        researcher_name: str,
        scholar_id: str,
        thread_id: int | None = None,
    ) -> dict:
        """Run the scraper for a single researcher using Scholar ID with IP limit checking.

        Args:
            researcher_name: Name of the researcher.
            scholar_id: Google Scholar ID.
            thread_id: Thread identifier for logging.

        Returns:
            Dictionary with scrape results.
        """
        thread_info = f"[Thread-{thread_id}]" if thread_id else ""

        ip_retry_attempt = 0
        while ip_retry_attempt <= MAX_IP_RETRIES:
            try:
                with self.print_lock:
                    if ip_retry_attempt == 0:
                        logger.info(
                            f"{thread_info} Starting Scholar ID scrape for: "
                            f"{researcher_name} (ID: {scholar_id})"
                        )
                    else:
                        logger.info(
                            f"{thread_info} IP retry #{ip_retry_attempt} for: "
                            f"{researcher_name} (forcing new IP)"
                        )

                searcher = TorScholarSearch(self.output_dir, max_retries=self.max_retries)

                with self.print_lock:
                    logger.info(
                        f"{thread_info} Requesting new Tor identity for fresh IP..."
                    )

                if thread_id:
                    stagger_delay = (thread_id - 1) * THREAD_STAGGER_DELAY_SECONDS
                    if stagger_delay > 0:
                        with self.print_lock:
                            logger.info(
                                f"{thread_info} Waiting {stagger_delay}s for "
                                "staggered identity request..."
                            )
                        time.sleep(stagger_delay)

                searcher.get_new_identity()

                current_ip = searcher.get_current_ip()

                if current_ip and current_ip != "Errored IP":
                    current_usage = self.ip_tracker.get_ip_usage_count(current_ip)

                    if current_usage >= self.max_requests_per_ip:
                        with self.print_lock:
                            logger.warning(
                                f"{thread_info} IP {current_ip} has reached/exceeded "
                                f"limit ({current_usage}/{self.max_requests_per_ip})"
                            )
                            logger.info(
                                f"{thread_info} Retrying with new IP to avoid "
                                "over-limit usage"
                            )
                        ip_retry_attempt += 1
                        backoff = min(2 ** ip_retry_attempt, 60)
                        time.sleep(backoff)
                        continue

                scrape_result = searcher.scrape_researcher_by_scholar_id(
                    scholar_id, researcher_name
                )

                if scrape_result and scrape_result.get("success"):
                    result = {
                        "success": True,
                        "stdout": (
                            f"Author: {scrape_result['author_name']}\n"
                            f"Affiliation: {scrape_result['affiliation']}\n"
                            f"Citations: {scrape_result['citations']}\n"
                            f"Papers: {scrape_result['papers_count']}\n"
                            f"Tor IP: {scrape_result['tor_ip']}\n"
                            f"Saved to: {scrape_result['folder_path']}"
                        ),
                        "stderr": "",
                        "researcher": researcher_name,
                        "thread_id": thread_id,
                        "ip_retry_attempt": ip_retry_attempt,
                        "scholar_id": scholar_id,
                    }

                    return result
                else:
                    error_msg = (
                        scrape_result.get("error", "Unknown error")
                        if scrape_result
                        else "Failed to get result"
                    )
                    return {
                        "success": False,
                        "error": error_msg,
                        "stderr": error_msg,
                        "researcher": researcher_name,
                        "thread_id": thread_id,
                        "ip_retry_attempt": ip_retry_attempt,
                        "scholar_id": scholar_id,
                    }

            except Exception as e:
                with self.print_lock:
                    logger.error(
                        f"{thread_info} Error scraping Scholar ID {scholar_id} "
                        f"for {researcher_name}: {e}"
                    )
                return {
                    "success": False,
                    "error": str(e),
                    "stderr": str(e),
                    "researcher": researcher_name,
                    "thread_id": thread_id,
                    "ip_retry_attempt": ip_retry_attempt,
                    "scholar_id": scholar_id,
                }

        with self.print_lock:
            logger.warning(
                f"{thread_info} Exhausted all {MAX_IP_RETRIES} IP retries for "
                f"{researcher_name}"
            )
        return {
            "success": False,
            "error": f"Exhausted {MAX_IP_RETRIES} IP retries",
            "stderr": f"Exhausted {MAX_IP_RETRIES} IP retries",
            "researcher": researcher_name,
            "thread_id": thread_id,
            "ip_retry_attempt": ip_retry_attempt,
            "scholar_id": scholar_id,
        }

    def _queue_worker_thread(
        self,
        thread_id: int,
        results: dict,
        successful_researchers: set,
    ) -> None:
        """Continuous worker thread that processes researchers from the queue.

        Args:
            thread_id: Thread identifier.
            results: Shared results dictionary.
            successful_researchers: Set of successfully processed researchers.
        """
        while True:
            try:
                try:
                    researcher_name, scholar_id = self.researcher_queue.get(timeout=QUEUE_TIMEOUT_SECONDS)
                except queue.Empty:
                    with self.queue_lock:
                        if self.researcher_queue.empty():
                            with self.print_lock:
                                logger.info(
                                    f"[Thread-{thread_id}] No more researchers "
                                    "in queue, thread exiting"
                                )
                            break
                        else:
                            continue

                with self._active_workers_lock:
                    self._active_workers += 1

                with self.results_lock:
                    if researcher_name not in results:
                        results[researcher_name] = []

                attempt_num = 0

                while researcher_name not in successful_researchers:
                    attempt_num += 1

                    if attempt_num > self.max_retries:
                        with self.print_lock:
                            logger.warning(
                                f"[Thread-{thread_id}] EXHAUSTED: {researcher_name} "
                                f"failed after {self.max_retries} attempts, giving up"
                            )
                        self.update_researcher_status(researcher_name, "failed_exhausted")
                        break

                    with self.print_lock:
                        logger.info(
                            f"[Thread-{thread_id}] Starting: {researcher_name} "
                            f"(Scholar ID: {scholar_id}) (Attempt #{attempt_num})"
                        )
                        if attempt_num > 1:
                            logger.info(
                                f"[Thread-{thread_id}] Retrying after failure - "
                                f"requesting fresh IP and waiting {RETRY_WAIT_SECONDS}s"
                            )

                    if attempt_num > 1:
                        if not self.check_tor_running():
                            with self.print_lock:
                                logger.error(
                                    f"[Thread-{thread_id}] Tor is no longer running, "
                                    "giving up on remaining researchers"
                                )
                            self.update_researcher_status(researcher_name, "failed_exhausted")
                            break

                        with self.print_lock:
                            logger.info(
                                f"[Thread-{thread_id}] Waiting {RETRY_WAIT_SECONDS} seconds "
                                "before retry..."
                            )
                        time.sleep(RETRY_WAIT_SECONDS)

                    start_time = time.time()
                    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                    future = executor.submit(
                        self._run_single_researcher_scrape_by_scholar_id,
                        researcher_name, scholar_id, thread_id=thread_id,
                    )
                    try:
                        result = future.result(timeout=SCRAPE_ATTEMPT_TIMEOUT_SECONDS)
                    except concurrent.futures.TimeoutError:
                        with self.print_lock:
                            logger.warning(
                                f"[Thread-{thread_id}] TIMEOUT: {researcher_name} "
                                f"exceeded {SCRAPE_ATTEMPT_TIMEOUT_SECONDS}s hard limit"
                            )
                        result = {
                            "success": False,
                            "error": f"Hard timeout after {SCRAPE_ATTEMPT_TIMEOUT_SECONDS}s",
                            "stderr": f"Hard timeout after {SCRAPE_ATTEMPT_TIMEOUT_SECONDS}s",
                            "researcher": researcher_name,
                            "thread_id": thread_id,
                            "scholar_id": scholar_id,
                        }
                    finally:
                        executor.shutdown(wait=False, cancel_futures=True)
                    end_time = time.time()

                    result["duration"] = round(end_time - start_time, 2)
                    result["attempt"] = attempt_num
                    result["timestamp"] = datetime.now().isoformat()
                    result["scholar_id"] = scholar_id

                    with self.results_lock:
                        results[researcher_name].append(result)

                    if result["success"]:
                        with self.results_lock:
                            successful_researchers.add(researcher_name)

                        self.update_researcher_status(researcher_name, "success")

                        ip_address = self.ip_tracker.extract_tor_ip_from_output(
                            result.get("stdout", "")
                        )
                        if ip_address:
                            self.ip_tracker.log_successful_scrape(
                                researcher_name, ip_address, thread_id
                            )

                        with self.print_lock:
                            logger.info(
                                f"[Thread-{thread_id}] SUCCESS: {researcher_name} "
                                f"({result['duration']}s) (Attempt #{attempt_num})"
                            )
                            if result.get("stdout"):
                                lines = result["stdout"].strip().split("\n")
                                for line in lines:
                                    if any(
                                        keyword in line
                                        for keyword in [
                                            "Author:",
                                            "Affiliation:",
                                            "Citations:",
                                            "Papers:",
                                            "Tor IP:",
                                            "Saved to:",
                                        ]
                                    ):
                                        logger.info(f"   {line}")
                        break

                    else:
                        with self.print_lock:
                            logger.warning(
                                f"[Thread-{thread_id}] FAILED: {researcher_name} "
                                f"({result['duration']}s) (Attempt #{attempt_num})"
                            )
                            error_info = result.get("error", "Unknown error")
                            if result.get("stderr"):
                                error_info = result["stderr"]
                            logger.warning(f"   Error: {error_info}")
                            logger.info(
                                f"[Thread-{thread_id}] Will retry with fresh IP "
                                f"after {RETRY_WAIT_SECONDS}s wait..."
                            )

                        self.update_researcher_status(researcher_name, "failed_retrying")

                with self._active_workers_lock:
                    self._active_workers -= 1
                self.researcher_queue.task_done()

            except Exception as e:
                with self.print_lock:
                    logger.error(f"[Thread-{thread_id}] Unexpected error: {e}")
                with self._active_workers_lock:
                    self._active_workers -= 1
                try:
                    self.researcher_queue.task_done()
                except Exception:
                    pass
                continue

    def _process_researchers_with_queue(
        self,
        researchers_data: dict[str, str],
        results: dict,
        successful_researchers: set,
    ) -> bool:
        """Process researchers using continuous queue-based approach.

        Args:
            researchers_data: Dictionary mapping names to Scholar IDs.
            results: Shared results dictionary.
            successful_researchers: Set of successfully processed researchers.

        Returns:
            True if exited due to stale progress (eligible for restart), False otherwise.
        """
        logger.info(
            f"QUEUE-BASED PROCESSING: Starting {len(researchers_data)} researchers "
            f"with Scholar IDs using {self.max_threads} continuous threads"
        )
        logger.info("Each thread will get a fresh Tor IP for every researcher scrape attempt")
        logger.info(
            f"Failed researchers retried up to {self.max_retries} times with fresh IP "
            f"and {RETRY_WAIT_SECONDS}s wait between attempts"
        )

        with self.queue_lock:
            for researcher_name, scholar_id in researchers_data.items():
                self.researcher_queue.put((researcher_name, scholar_id))

        threads = []
        for thread_id in range(1, self.max_threads + 1):
            thread = threading.Thread(
                target=self._queue_worker_thread,
                args=(thread_id, results, successful_researchers),
            )
            thread.start()
            threads.append(thread)
            logger.info(f"Started worker thread {thread_id}")

        last_progress_time = time.time()
        last_activity_time = time.time()
        last_known_successes = 0
        stale_exit = False
        while True:
            time.sleep(MAIN_LOOP_SLEEP_SECONDS)

            current_time = time.time()
            if current_time - last_progress_time >= PROGRESS_UPDATE_INTERVAL_SECONDS:
                self.print_current_progress()
                last_progress_time = current_time

            with self.results_lock:
                current_successes = len(successful_researchers)
                if current_successes == len(researchers_data):
                    logger.info(
                        f"All {len(researchers_data)} researchers have been "
                        "successfully processed!"
                    )
                    break

            if current_successes != last_known_successes:
                last_known_successes = current_successes
                last_activity_time = current_time

            with self._active_workers_lock:
                active = self._active_workers
            alive_threads = [t for t in threads if t.is_alive()]
            if not alive_threads and active == 0:
                logger.info("All worker threads have finished")
                break

            if current_time - last_activity_time >= STALE_PROGRESS_TIMEOUT_SECONDS:
                logger.warning(
                    f"No new successes in {STALE_PROGRESS_TIMEOUT_SECONDS}s, "
                    "threads appear stuck — forcing exit"
                )
                stale_exit = True
                break

        logger.info("Waiting for worker threads to finish...")
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=THREAD_JOIN_TIMEOUT_SECONDS)
                if thread.is_alive():
                    logger.warning(
                        f"Worker thread did not finish within "
                        f"{THREAD_JOIN_TIMEOUT_SECONDS}s timeout"
                    )

        try:
            while not self.researcher_queue.empty():
                self.researcher_queue.get_nowait()
                self.researcher_queue.task_done()
        except queue.Empty:
            pass

        with self.print_lock:
            logger.info("Queue processing completed!")
            self.ip_tracker.save_to_file()

        return stale_exit

    def process_researchers_from_csv(self) -> dict:
        """Process researchers from CSV file using continuous queue-based approach.

        Returns:
            Dictionary of results by researcher name.
        """
        researchers_data = self.read_csv_file()
        if not researchers_data:
            logger.error("No valid researchers with Scholar IDs found in CSV file!")
            return {}

        if self.continue_mode:
            successful_researchers_from_log = set(
                self.progress_data.get("success", [])
            )
            original_count = len(researchers_data)
            researchers_data = {
                name: scholar_id
                for name, scholar_id in researchers_data.items()
                if name not in successful_researchers_from_log
            }

            logger.info("CSV RESEARCHER SCRAPING SESSION (CONTINUE MODE)")
            logger.info(f"Continuing from: {self.logs_dir}")
            logger.info(f"Original researchers in CSV: {original_count}")
            logger.info(f"Already successful: {len(successful_researchers_from_log)}")
            logger.info(f"Remaining to process: {len(researchers_data)}")

            if not researchers_data:
                logger.info("All researchers have already been successfully processed!")
                return self.progress_data
        else:
            logger.info("CSV RESEARCHER SCRAPING SESSION (QUEUE-BASED CONTINUOUS RETRY)")

        logger.info(f"Starting at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"CSV file: {self.csv_file}")
        logger.info(f"Processing {len(researchers_data)} researchers with Scholar IDs")
        logger.info(f"Max threads: {self.max_threads}")
        logger.info(
            f"Failed researchers retried up to {self.max_retries} times with fresh IP "
            f"and {RETRY_WAIT_SECONDS}s wait between attempts"
        )
        logger.info(f"IP request limit: {self.max_requests_per_ip} per IP address")
        logger.info(f"Session logs directory: {self.logs_dir}")

        if not self.continue_mode:
            researcher_names = list(researchers_data.keys())
            self.initialize_progress_tracking(researcher_names)

        results: dict = {}
        successful_researchers: set = set()

        if self.continue_mode:
            successful_researchers.update(self.progress_data.get("success", []))

        for restart_num in range(MAX_STALE_RESTARTS + 1):
            remaining = {
                name: sid
                for name, sid in researchers_data.items()
                if name not in successful_researchers
            }
            if not remaining:
                break

            if restart_num > 0:
                logger.info(
                    f"STALE RESTART {restart_num}/{MAX_STALE_RESTARTS}: "
                    f"Restarting Tor and retrying {len(remaining)} researchers..."
                )
                self.stop_tor_service()
                time.sleep(TOR_RESTART_DELAY_SECONDS)
                if not self.start_tor_service():
                    logger.error("Failed to restart Tor after stale progress, giving up")
                    break
                with self._active_workers_lock:
                    self._active_workers = 0
                self.researcher_queue = queue.Queue()

            stale_exit = self._process_researchers_with_queue(
                remaining, results, successful_researchers
            )

            if not stale_exit:
                break

            if restart_num >= MAX_STALE_RESTARTS:
                logger.warning(
                    f"Exhausted all {MAX_STALE_RESTARTS} stale restarts, giving up"
                )

        logger.info("CSV SESSION COMPLETED - FINAL PROGRESS")
        self.print_current_progress()

        self._print_final_summary(results, successful_researchers)
        self.ip_tracker.print_usage_summary()

        self.stop_tor_service()

        return results

    def _print_final_summary(
        self,
        results: dict,
        successful_researchers: set,
    ) -> None:
        """Log final session summary.

        Args:
            results: Dictionary of results by researcher name.
            successful_researchers: Set of successfully processed researchers.
        """
        total_researchers = len(results)
        successful_count = len(successful_researchers)
        success_rate = (successful_count / total_researchers * 100) if total_researchers else 0.0
        exhausted_count = total_researchers - successful_count
        total_attempts = sum(len(attempts) for attempts in results.values())

        logger.info("FINAL SESSION SUMMARY")
        logger.info(f"Total researchers: {total_researchers}")
        logger.info(f"Successful extractions: {successful_count}")
        if exhausted_count > 0:
            logger.info(f"Failed (exhausted retries): {exhausted_count}")
        logger.info(f"Success rate: {success_rate:.1f}%")
        logger.info(f"Total attempts made: {total_attempts}")
        logger.info(f"Max retries per researcher: {self.max_retries}")

        if successful_count > 0:
            output_folder = getattr(self, "output_dir", "Researcher_Profiles")
            logger.info(f"Data saved to '{output_folder}' folder")
            logger.info("Each researcher has their own subfolder containing:")
            logger.info("  - profile.json (researcher metadata + Tor IP)")
            logger.info("  - papers.csv (top 50 paper details with descriptions)")

        logger.info("ATTEMPT STATISTICS:")
        researchers_by_attempts: dict = {}
        for name, attempts in results.items():
            attempt_count = len(attempts)
            if attempt_count not in researchers_by_attempts:
                researchers_by_attempts[attempt_count] = []
            researchers_by_attempts[attempt_count].append(name)

        for attempt_count in sorted(researchers_by_attempts.keys()):
            researchers_list = researchers_by_attempts[attempt_count]
            logger.info(f"  {attempt_count} attempt(s): {len(researchers_list)} researchers")

        retry_successes = []
        for name in successful_researchers:
            if name in results and len(results[name]) > 1:
                retry_successes.append((name, len(results[name])))

        if retry_successes:
            logger.info("RESEARCHERS THAT SUCCEEDED AFTER MULTIPLE ATTEMPTS:")
            retry_successes.sort(key=lambda x: x[1], reverse=True)
            for name, attempt_count in retry_successes:
                logger.info(f"  - {name} (succeeded on attempt #{attempt_count})")

        first_try_successes = len(researchers_by_attempts.get(1, []))
        if first_try_successes > 0:
            logger.info(f"{first_try_successes} researchers succeeded on first attempt")
            logger.info(
                f"{total_researchers - first_try_successes} researchers required retries"
            )
