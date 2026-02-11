"""
Simple API server for the Ingest Engine.
Exposes status, history, folder contents, and logs via HTTP.
This allows the web backend to run on a separate server.
"""

import os
import json
import re
import threading
import logging
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from configparser import ConfigParser
try:
    from processor import generate_pdf_report
except ImportError:
    generate_pdf_report = None

# Will be set by start_api_server()
_config = None
_base_dir = None

class IngestAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the Ingest Engine API."""

    def log_message(self, format, *args):
        """Override to use our logging instead of printing to stderr."""
        logging.debug(f"API: {args[0]}")

    def _send_json_response(self, data, status=200):
        """Send a JSON response."""
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        """Handle GET requests."""
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            if path == '/api/status':
                self._handle_status()
            elif path == '/api/history':
                self._handle_history()
            elif path == '/api/logs':
                self._handle_logs()
            elif path.startswith('/api/folders/'):
                folder_name = path.split('/api/folders/')[-1]
                self._handle_folder(folder_name)
            elif path == '/api/health':
                self._send_json_response({"status": "ok"})
            elif path == '/api/report':
                self._handle_report()
            elif path == '/':
                self._send_json_response({"message": "Ingest Engine API", "endpoints": ["/api/status", "/api/history", "/api/logs", "/api/folders/{name}", "/api/health", "/api/report"]})
            else:
                self._send_json_response({"error": "Not found"}, 404)
        except Exception as e:
            logging.error(f"API error handling {path}: {e}")
            self._send_json_response({"error": str(e)}, 500)

    def do_DELETE(self):
        """Handle DELETE requests."""
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            if path == '/api/history':
                self._handle_clear_history()
            elif path == '/api/logs':
                self._handle_clear_logs()
            else:
                self._send_json_response({"error": "Not found"}, 404)
        except Exception as e:
            logging.error(f"API error handling DELETE {path}: {e}")
            self._send_json_response({"error": str(e)}, 500)

    def _handle_clear_history(self):
        """Clear all processing history."""
        history_path = os.path.join(_base_dir, _config['Paths']['history_file'])
        try:
            with open(history_path, 'w') as f:
                json.dump([], f)
            logging.info("History cleared via API")
            self._send_json_response({"cleared": True, "message": "History cleared"})
        except Exception as e:
            self._send_json_response({"error": f"Error clearing history: {e}"}, 500)

    def _handle_clear_logs(self):
        """Clear the log file."""
        log_dir = os.path.join(_base_dir, _config['Paths']['logs'])
        log_path = os.path.join(log_dir, 'ingest_engine.log')
        try:
            with open(log_path, 'w') as f:
                f.truncate(0)
            logging.info("Logs cleared via API")
            self._send_json_response({"cleared": True, "message": "Logs cleared"})
        except Exception as e:
            self._send_json_response({"error": f"Error clearing logs: {e}"}, 500)

    def _handle_status(self):
        """Return current processing status."""
        status_path = os.path.join(_base_dir, _config['Paths']['status_file'])
        if not os.path.exists(status_path):
            self._send_json_response({"status": "idle", "file": "None", "progress": 0, "stage": "Idle"})
            return

        try:
            with open(status_path, 'r') as f:
                data = json.load(f)
            self._send_json_response(data)
        except (json.JSONDecodeError, IOError) as e:
            self._send_json_response({"error": f"Error reading status: {e}"}, 500)

    def _handle_history(self):
        """Return processing history."""
        history_path = os.path.join(_base_dir, _config['Paths']['history_file'])
        if not os.path.exists(history_path):
            self._send_json_response([])
            return

        try:
            with open(history_path, 'r') as f:
                data = json.load(f)
            self._send_json_response(data)
        except (json.JSONDecodeError, IOError) as e:
            self._send_json_response({"error": f"Error reading history: {e}"}, 500)

    def _handle_logs(self):
        """Return recent log entries."""
        log_dir = os.path.join(_base_dir, _config['Paths']['logs'])
        log_path = os.path.join(log_dir, 'ingest_engine.log')

        if not os.path.exists(log_path):
            self._send_json_response([])
            return

        logs = []
        log_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (INFO|WARNING|ERROR) - (.*)$")

        try:
            with open(log_path, 'r') as f:
                all_lines = f.readlines()
                for line in all_lines[-100:]:
                    match = log_pattern.match(line)
                    if match:
                        logs.append({
                            "timestamp": match.group(1),
                            "level": match.group(2),
                            "message": match.group(3).strip()
                        })
                    elif logs and line.strip():
                        logs[-1]["message"] += "\n" + line.strip()
            self._send_json_response(logs)
        except IOError as e:
            self._send_json_response({"error": f"Error reading logs: {e}"}, 500)

    def _handle_folder(self, folder_name):
        """Return contents of a specific folder."""
        folder_map = {
            "watch": _config.get('Paths', 'watch', fallback=None),
            "processing": _config.get('Paths', 'processing', fallback=None),
            "processed": _config.get('Paths', 'processed', fallback=None),
            "output": _config.get('Paths', 'output', fallback=None),
            "error": _config.get('Paths', 'error', fallback=None),
        }

        if folder_name not in folder_map or not folder_map[folder_name]:
            self._send_json_response({"error": f"Invalid folder: {folder_name}"}, 400)
            return

        path = os.path.expanduser(folder_map[folder_name])

        if not os.path.isdir(path):
            self._send_json_response({"error": f"Path not found: {path}"}, 404)
            return

        try:
            files_data = []
            for f_name in os.listdir(path):
                full_path = os.path.join(path, f_name)
                if os.path.isfile(full_path) and not f_name.startswith('.'):
                    stat = os.stat(full_path)
                    files_data.append({
                        "name": f_name,
                        "size": stat.st_size,
                        "modified_time": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
                    })
            self._send_json_response({"folder": folder_name, "files": files_data})
        except Exception as e:
            self._send_json_response({"error": f"Error listing folder: {e}"}, 500)

    def _handle_report(self):
        """Generate and return path to PDF report."""
        if not generate_pdf_report:
            self._send_json_response({"error": "PDF report generation not available"}, 501)
            return
        history_path = os.path.join(_base_dir, _config['Paths']['history_file'])
        output_folder = os.path.expanduser(_config['Paths']['output'])

        try:
            pdf_path = generate_pdf_report(history_path, output_folder)
            if pdf_path:
                self._send_json_response({"success": True, "path": pdf_path})
            else:
                self._send_json_response({"success": False, "error": "No history to report"}, 400)
        except Exception as e:
            self._send_json_response({"error": f"Error generating report: {e}"}, 500)


def start_api_server(config: ConfigParser, base_dir: str, host: str = '0.0.0.0', port: int = 8080):
    """
    Start the API server in a background thread.

    Args:
        config: The ConfigParser instance with paths
        base_dir: Base directory of the ingest engine
        host: Host to bind to (default 0.0.0.0 for all interfaces)
        port: Port to listen on (default 8080)
    """
    global _config, _base_dir
    _config = config
    _base_dir = base_dir

    server = HTTPServer((host, port), IngestAPIHandler)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    logging.info(f"API server started on http://{host}:{port}")
    return server
