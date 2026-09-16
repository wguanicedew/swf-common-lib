"""
This module contains the base class for all agents.
"""

import os
import sys
import time
import signal
import socket
import stomp
import requests
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional
from .api_utils import get_next_agent_id, api_request_with_retry
from .config_utils import load_testbed_config, TestbedConfigError


class APIError(Exception):
    """Exception raised for API-related failures."""
    
    def __init__(self, message, response=None, url=None, method=None):
        super().__init__(message)
        self.response = response
        self.url = url
        self.method = method

def setup_environment():
    """Auto-activate venv and load environment variables."""
    script_dir = Path(__file__).resolve().parent.parent.parent.parent / "swf-testbed"
    
    # Auto-activate the dev venv only when running on a bare interpreter.
    # Running inside any venv counts as active — sys.prefix differs from
    # base_prefix — whether or not VIRTUAL_ENV is exported: systemd units
    # launch the deployed venv python with no activation environment, and
    # repointing sys.executable at the dev venv from there sent every
    # production doer subprocess to the dev venv and its editable dev
    # trees (found 2026-07-12).
    already_in_venv = sys.prefix != getattr(sys, 'base_prefix', sys.prefix)
    if not already_in_venv and "VIRTUAL_ENV" not in os.environ:
        venv_path = script_dir / ".venv"
        if venv_path.exists():
            print("🔧 Auto-activating virtual environment...")
            venv_python = venv_path / "bin" / "python"
            if venv_python.exists():
                os.environ["VIRTUAL_ENV"] = str(venv_path)
                os.environ["PATH"] = f"{venv_path}/bin:{os.environ['PATH']}"
                sys.executable = str(venv_python)
        else:
            print("❌ Error: No Python virtual environment found")
            return False
    
    # Load ~/.env environment variables (they're already exported)
    env_file = Path.home() / ".env"
    if env_file.exists():
        print("🔧 Loading environment variables from ~/.env...")
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    if line.startswith('export '):
                        line = line[7:]  # Remove 'export '
                    key, value = line.split('=', 1)
                    value = value.strip('"\'')
                    # Skip entries with unexpanded shell variables (e.g., PATH=$PATH:...)
                    # These are already expanded by shell when it sourced ~/.env
                    if '$' in value:
                        continue
                    os.environ[key] = value

    # Unset proxy variables to prevent localhost routing through proxy
    for proxy_var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
        if proxy_var in os.environ:
            del os.environ[proxy_var]
    
    return True

# Auto-setup environment when module is imported (unless already done)
if not os.getenv('SWF_ENV_LOADED'):
    setup_environment()
    os.environ['SWF_ENV_LOADED'] = 'true'

# Import the centralized logging from swf-common-lib
from swf_common_lib.rest_logging import setup_rest_logging

# Configure base logging level with environment overrides
_quiet = os.getenv('SWF_AGENT_QUIET', 'false').lower() in ('1', 'true', 'yes', 'on')
_level_name = os.getenv('SWF_LOG_LEVEL', 'WARNING' if _quiet else 'INFO').upper()

# Validate log level and provide clear error for invalid values
# Use Python's built-in logging level definitions for maintainability
_valid_levels = set(logging._nameToLevel.keys()) - {'NOTSET'}  # Exclude NOTSET from display
if _level_name not in logging._nameToLevel:
    print(f"WARNING: Invalid SWF_LOG_LEVEL '{_level_name}'. Valid levels: {', '.join(sorted(_valid_levels))}. Using INFO.")
    _level = logging.INFO
else:
    _level = logging._nameToLevel[_level_name]

logging.basicConfig(level=_level, format='%(asctime)s - %(levelname)s - %(name)s - [%(threadName)s] - %(message)s')

# STOMP logging is very chatty; enable only if explicitly requested
stomp_logger = logging.getLogger('stomp')
if os.getenv('SWF_STOMP_DEBUG', 'false').lower() in ('1', 'true', 'yes', 'on'):
    stomp_logger.setLevel(logging.DEBUG)
    _stomp_handler = logging.StreamHandler()
    _stomp_handler.setLevel(logging.DEBUG)
    _stomp_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s'))
    stomp_logger.addHandler(_stomp_handler)
else:
    stomp_logger.setLevel(logging.WARNING)


class BaseAgent(stomp.ConnectionListener):
    """
    A base class for creating standalone STF workflow agents.

    This class handles the common tasks of:
    - Connecting to the ActiveMQ message broker (and inheriting from stomp.ConnectionListener).
    - Communicating with the swf-monitor REST API.
    - Running a persistent process with graceful shutdown.
    """

    # Standard workflow message types
    WORKFLOW_MESSAGE_TYPES = {
        'run_imminent', 'start_run', 'pause_run', 'resume_run', 'end_run',
        'stf_gen', 'stf_ready', 'tf_file_registered', 'slice', 'slice_result',
        'heartbeat'
    }

    def __init__(self, agent_type, subscription_queue, debug=False,
                 config_path: Optional[str] = None):
        """
        Initialize BaseAgent.

        Args:
            agent_type: Type of agent (e.g., 'DATA', 'PROCESSING')
            subscription_queue: ActiveMQ destination with explicit prefix.
                Must start with '/queue/' (anycast) or '/topic/' (multicast).
                Example: '/queue/workflow_control' or '/topic/epictopic'
            debug: Enable debug logging
            config_path: Path to testbed.toml config file

        Raises:
            ValueError: If subscription_queue doesn't have /queue/ or /topic/ prefix
        """
        # Validate destination has explicit prefix (required for Artemis routing)
        if not subscription_queue.startswith('/queue/') and not subscription_queue.startswith('/topic/'):
            raise ValueError(
                f"subscription_queue must start with '/queue/' or '/topic/', got: '{subscription_queue}'. "
                f"Use '/queue/{subscription_queue}' for anycast or '/topic/{subscription_queue}' for multicast."
            )

        self.agent_type = agent_type
        self.subscription_queue = subscription_queue
        self.DEBUG = debug

        # Resolve config path: explicit arg > SWF_TESTBED_CONFIG env var > default
        if config_path is None:
            env_config = os.getenv('SWF_TESTBED_CONFIG')
            if env_config:
                # Env var is filename, resolve to workflows/ directory
                if not env_config.startswith('workflows/') and '/' not in env_config:
                    config_path = f'workflows/{env_config}'
                else:
                    config_path = env_config
            else:
                config_path = 'workflows/testbed.toml'
        self.config_path = config_path  # Store for subclasses

        # Load testbed configuration (namespace)
        self.namespace = None
        try:
            config = load_testbed_config(config_path=config_path)
            self.namespace = config.namespace
            logging.info(f"Namespace: {self.namespace}")
        except TestbedConfigError as e:
            logging.error(f"Configuration error: {e}")
            raise

        # Configuration from environment variables (needed for agent ID API call)
        self.monitor_url = (os.getenv('SWF_MONITOR_URL') or '').rstrip('/')
        self.api_token = os.getenv('SWF_API_TOKEN')

        # Set up API session (needed for agent ID call)
        import requests
        self.api = requests.Session()
        if self.api_token:
            self.api.headers.update({'Authorization': f'Token {self.api_token}'})

        # Create unique agent name with username and sequential ID
        import getpass
        self.username = getpass.getuser()
        agent_id = self.get_next_agent_id()
        self.agent_name = f"{self.agent_type.lower()}-agent-{self.username}-{agent_id}"

        # Workflow context tracking (populated from messages)
        self.current_execution_id = None
        self.current_run_id = None

        # Process identification for agent management
        self.pid = os.getpid()
        self.hostname = socket.gethostname()
        self.operational_state = 'STARTING'  # STARTING, READY, PROCESSING, EXITED

        # Background execution (opt-in via run_in_background). Pools are created
        # lazily per queue, so an agent that never calls run_in_background is
        # unaffected, and independent queues can't starve each other.
        # See swf-testbed/docs/architecture_and_design_choices.md
        # § Blocking Handlers and Background Execution.
        self._bg_executor: dict[str, ThreadPoolExecutor] = {}
        self._bg_max_workers = int(os.getenv('SWF_AGENT_MAX_WORKERS', '4'))
        self._bg_lock = threading.Lock()     # guards _bg_inflight, _bg_keys, _bg_active
        self._bg_inflight = 0                # background tasks currently running (all queues)
        self._bg_keys: set[str] = set()      # in-flight dedup keys
        self._bg_active: dict[str, int] = {}  # queue -> tasks currently executing
        self._send_lock = threading.Lock()   # serialize bus sends across threads

        # Use HTTP URL for REST logging (no auth required)
        self.base_url = (os.getenv('SWF_MONITOR_HTTP_URL') or '').rstrip('/')
        self.mq_host = os.getenv('ACTIVEMQ_HOST', 'localhost')
        self.mq_port = int(os.getenv('ACTIVEMQ_PORT', 61612))  # STOMP port for Artemis on this system
        self.mq_user = os.getenv('ACTIVEMQ_USER', 'admin')
        self.mq_password = os.getenv('ACTIVEMQ_PASSWORD', 'admin')
        
        # SSL configuration
        self.use_ssl = os.getenv('ACTIVEMQ_USE_SSL', 'False').lower() == 'true'
        self.ssl_ca_certs = os.getenv('ACTIVEMQ_SSL_CA_CERTS', '')
        self.ssl_cert_file = os.getenv('ACTIVEMQ_SSL_CERT_FILE', '')
        self.ssl_key_file = os.getenv('ACTIVEMQ_SSL_KEY_FILE', '')
        
        # Set up centralized REST logging
        self.logger = setup_rest_logging('base_agent', self.agent_name, self.base_url)

        # Create connection with proper heartbeat configuration
        self.conn = stomp.Connection(
            host_and_ports=[(self.mq_host, self.mq_port)],
            vhost=self.mq_host,
            try_loopback_connect=False,
            heartbeats=(30000, 30000),  # Enable automatic heartbeat sending
            auto_content_length=False
        )
        
        # Configure SSL if enabled - must be done before set_listener
        if self.use_ssl:
            import ssl
            logging.info(f"Configuring SSL connection with CA certs: {self.ssl_ca_certs}")
            
            if self.ssl_ca_certs:
                # Configure SSL transport
                self.conn.transport.set_ssl(
                    for_hosts=[(self.mq_host, self.mq_port)],
                    ca_certs=self.ssl_ca_certs,
                    ssl_version=ssl.PROTOCOL_TLS_CLIENT
                )
                logging.info("SSL transport configured successfully")
            else:
                logging.warning("SSL enabled but no CA certificate file specified")
        
        self.conn.set_listener('', self)
        
        # For localhost development, disable SSL verification and proxy
        if 'localhost' in self.monitor_url or '127.0.0.1' in self.monitor_url:
            self.api.verify = False
            # Disable proxy for localhost connections
            self.api.proxies = {
                'http': '',
                'https': ''
            }
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _log_extra(self, **kwargs):
        """
        Build extra dict for logging with common context fields.

        Automatically includes username, execution_id, and run_id when set.
        Subclasses can override to add additional fields, calling super()._log_extra().

        Usage:
            self.logger.info("Message", extra=self._log_extra(custom_field=value))
        """
        extra = {'username': self.username}
        if self.current_execution_id:
            extra['execution_id'] = self.current_execution_id
        if self.current_run_id:
            extra['run_id'] = self.current_run_id
        extra.update(kwargs)
        return extra

    def run(self):
        """
        Connects to the message broker and runs the agent's main loop.
        """
        # Register signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            sig_name = signal.Signals(signum).name
            logging.info(f"Received {sig_name}, initiating graceful shutdown...")
            raise KeyboardInterrupt(f"Received {sig_name}")

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGQUIT, signal_handler)

        logging.info(f"Starting {self.agent_name}...")

        # Connect if not already connected (some subclasses connect in __init__)
        if not getattr(self, 'mq_connected', False):
            max_retries = 3
            retry_delay = 5
            for attempt in range(1, max_retries + 1):
                logging.info(f"Connecting to ActiveMQ at {self.mq_host}:{self.mq_port} (attempt {attempt}/{max_retries})")
                try:
                    self.conn.connect(
                        self.mq_user,
                        self.mq_password,
                        wait=True,
                        version='1.1',
                        headers={
                            'client-id': self.agent_name,
                            'heart-beat': '30000,30000'
                        }
                    )
                    self.mq_connected = True
                    break
                except Exception as e:
                    logging.warning(f"Connection attempt {attempt} failed: {e}")
                    if attempt < max_retries:
                        logging.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logging.error(f"Failed to connect after {max_retries} attempts")
                        raise

        try:
            self.conn.subscribe(destination=self.subscription_queue, id=1, ack='auto')
            logging.info(f"Subscribed to queue: '{self.subscription_queue}'")

            # Register as subscriber in monitor
            self.register_subscriber()

            # Agent is now ready and waiting for work
            self.set_ready()

            # Initial registration/heartbeat
            try:
                self.send_heartbeat()
            except Exception:
                logging.warning("Initial heartbeat failed — server may be restarting, will retry")

            logging.info(f"{self.agent_name} is running. Press Ctrl+C to stop.")
            while True:
                time.sleep(60) # Keep the main thread alive, heartbeats can be added here

                # Check connection status and attempt reconnection if needed
                if not self.mq_connected:
                    self._attempt_reconnect()

                try:
                    self.send_heartbeat()
                except Exception:
                    logging.warning("Heartbeat failed — server may be restarting, will retry next cycle")

        except KeyboardInterrupt:
            logging.info(f"Stopping {self.agent_name}...")
        except stomp.exception.ConnectFailedException as e:
            self.mq_connected = False
            logging.error(f"Failed to connect to ActiveMQ: {e}")
            logging.error("Please check the connection details and ensure ActiveMQ is running.")
        except Exception as e:
            self.mq_connected = False
            logging.error(f"An unexpected error occurred: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Drain in-flight background work before reporting EXITED / disconnecting,
            # so credentialed workers finish (and can still notify over the live bus).
            # Bounded in practice by each doer's own subprocess timeout.
            if self._bg_executor:
                logging.info("Draining background worker pools...")
                for pool in self._bg_executor.values():
                    pool.shutdown(wait=True)

            # Report exit status before disconnecting
            try:
                self.operational_state = 'EXITED'
                self.report_agent_status("EXITED", "Agent shutdown")
            except Exception as e:
                logging.warning(f"Failed to report exit status: {e}")

            if self.conn and self.conn.is_connected():
                self.conn.disconnect()
                self.mq_connected = False
                logging.info("Disconnected from ActiveMQ.")

    def on_connected(self, frame):
        """Handle successful connection to ActiveMQ."""
        logging.info(f"Successfully connected to ActiveMQ: {frame.headers}")
        self.mq_connected = True
    
    def on_error(self, frame):
        logging.error(f'Received an error from ActiveMQ: body="{frame.body}", headers={frame.headers}, cmd="{frame.cmd}"')
        self.mq_connected = False
    
    def on_disconnected(self):
        """Handle disconnection from ActiveMQ."""
        logging.warning("Disconnected from ActiveMQ - will attempt reconnection")
        self.mq_connected = False
        # Send heartbeat to update status, but don't let failures crash the receiver thread
        try:
            self.send_heartbeat()
        except Exception as e:
            logging.warning(f"Heartbeat failed during disconnect: {e}")

    def _attempt_reconnect(self):
        """Attempt to reconnect to ActiveMQ."""
        if self.mq_connected:
            return True
            
        try:
            logging.info("Attempting to reconnect to ActiveMQ...")
            if self.conn.is_connected():
                self.conn.disconnect()
            
            self.conn.connect(
                self.mq_user,
                self.mq_password,
                wait=True,
                version='1.1',
                headers={
                    'client-id': self.agent_name,
                    'heart-beat': '30000,30000'  # Send heartbeat every 30sec, expect server every 30sec
                }
            )
            
            self.conn.subscribe(destination=self.subscription_queue, id=1, ack='auto')
            self.mq_connected = True
            logging.info("Successfully reconnected to ActiveMQ")
            return True
            
        except Exception as e:
            logging.warning(f"Reconnection attempt failed: {e}")
            self.mq_connected = False
            return False

    def on_message(self, frame):
        """
        Callback for handling incoming messages.
        This method must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement on_message")

    def log_received_message(self, frame, known_types=None):
        """
        Helper method to log received messages with type information and namespace filtering.
        Agents can call this at the start of their on_message method.

        Namespace filtering:
        - If agent has namespace AND message has different namespace → returns (None, None)
        - If message has no namespace → processes it (backward compat)
        - If agent has no namespace → processes all messages (backward compat)

        Args:
            frame: The STOMP message frame
            known_types: Optional set/list of known message types (defaults to WORKFLOW_MESSAGE_TYPES)

        Returns:
            tuple: (message_data, msg_type) for convenience, or (None, None) if filtered

        Raises:
            RuntimeError: If message parsing fails
        """
        if known_types is None:
            known_types = self.WORKFLOW_MESSAGE_TYPES

        try:
            message_data = json.loads(frame.body)
            msg_type = message_data.get('msg_type', 'unknown')

            # Namespace filtering
            msg_namespace = message_data.get('namespace')
            if self.namespace and msg_namespace and msg_namespace != self.namespace:
                logging.debug(
                    f"Ignoring message from namespace '{msg_namespace}' (ours: '{self.namespace}')"
                )
                return None, None

            if msg_type not in known_types:
                logging.info(f"{self.agent_type} agent received unknown message type: {msg_type}", extra={"msg_type": msg_type})
            else:
                logging.info(f"{self.agent_type} agent received message: {msg_type}")

            return message_data, msg_type
        except json.JSONDecodeError as e:
            logging.error(f"CRITICAL: Failed to parse message JSON: {e}")
            raise RuntimeError(f"Message parsing failed - agent cannot continue: {e}") from e

    # -------------------------------------------------------------------------
    # Processing State API
    # -------------------------------------------------------------------------

    def set_processing(self):
        """
        Declare that the agent is actively doing work.

        Call this when the agent begins a unit of work (which may span multiple
        messages). The agent will report PROCESSING state in heartbeats until
        set_ready() is called.

        Example:
            def on_message(self, frame):
                message_data, msg_type = self.log_received_message(frame)
                if msg_type == 'start_run':
                    self.set_processing()
                    # ... do work ...
        """
        self.operational_state = 'PROCESSING'
        logging.info(f"{self.agent_name} state -> PROCESSING")

    def set_ready(self):
        """
        Declare that the agent is idle, waiting for work.

        Call this when a unit of work is complete and the agent is ready
        to receive new work.

        Example:
            def on_message(self, frame):
                message_data, msg_type = self.log_received_message(frame)
                if msg_type == 'end_run':
                    # ... finalize work ...
                    self.set_ready()
        """
        self.operational_state = 'READY'
        logging.info(f"{self.agent_name} state -> READY")

    def processing(self):
        """
        Context manager for bounded processing work.

        Use this when a unit of work is contained within a single code block.
        Automatically sets PROCESSING on entry and READY on exit.

        Example:
            def on_message(self, frame):
                message_data, msg_type = self.log_received_message(frame)
                if msg_type == 'process_stf':
                    with self.processing():
                        # ... do bounded work ...
                    # automatically back to READY
        """
        agent = self

        class ProcessingContext:
            def __enter__(self):
                agent.set_processing()
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                agent.set_ready()
                return False  # Don't suppress exceptions

        return ProcessingContext()

    def run_in_background(self, fn, *args, dedup_key=None, label=None, queue='default', **kwargs):
        """Run ``fn(*args, **kwargs)`` on one of the agent's bounded worker pools
        and return immediately, freeing the STOMP receiver thread.

        Opt-in: an agent that never calls this is unaffected. Use it for handler
        work that blocks — a subprocess, or a long REST / Rucio / xrootd call —
        so one slow message cannot stall liveness pings or later messages.

        - ``queue`` (optional): name of the worker pool to submit to (default
          ``'default'``). Each distinct queue gets its own lazily-created
          ``ThreadPoolExecutor`` with ``SWF_AGENT_MAX_WORKERS`` threads, so
          slow work on one queue can't starve another.
        - Reentrant PROCESSING: the agent reports PROCESSING while any background
          task is in flight (on any queue), READY when none is.
        - Every exception in ``fn`` is caught and logged; a worker never dies
          silently.
        - ``dedup_key`` (optional): if a task with the same key is already in
          flight, this call is skipped and returns False — closing the
          duplicate-work race that concurrency introduces. Dedup keys are
          tracked across all queues.

        Calls with different keys can run concurrently: each queue's pool has
        ``SWF_AGENT_MAX_WORKERS`` threads (default 4). ``dedup_key`` is not a
        lock and does not preserve the receiver thread's former serial
        semantics. For a safe first migration, set ``SWF_AGENT_MAX_WORKERS=1``.
        With a larger pool, keep per-message state local to ``fn`` and protect
        any non-thread-safe library or CLI with a lock acquired inside the
        background function, never in the receiver-thread handler.

        Do not mix with the inline ``processing()`` context manager in the same
        agent; both drive operational_state. Returns True if enqueued, False if
        deduplicated or the pool refused the task.

        See swf-testbed/docs/architecture_and_design_choices.md
        § Blocking Handlers and Background Execution.
        """
        label = label or getattr(fn, '__name__', 'task')
        with self._bg_lock:
            if dedup_key is not None and dedup_key in self._bg_keys:
                self.logger.info(
                    f"{self.agent_name} background '{label}' skipped: "
                    f"dedup_key {dedup_key!r} already in flight")
                return False
            if queue not in self._bg_executor:
                self._bg_executor[queue] = ThreadPoolExecutor(
                    max_workers=self._bg_max_workers,
                    thread_name_prefix=f"{self.agent_type.lower()}-bg-{queue}")
            if dedup_key is not None:
                self._bg_keys.add(dedup_key)
            self._bg_inflight += 1
            if self._bg_inflight == 1:
                self.set_processing()
            try:
                self._bg_executor[queue].submit(
                    self._bg_run, fn, args, kwargs, dedup_key, label, queue)
            except Exception as e:
                # Pool refused the task — roll back the bookkeeping we just did.
                self._bg_inflight -= 1
                if dedup_key is not None:
                    self._bg_keys.discard(dedup_key)
                if self._bg_inflight == 0:
                    self.set_ready()
                self.logger.error(
                    f"{self.agent_name} could not enqueue background '{label}': {e}")
                return False
        return True

    def _bg_run(self, fn, args, kwargs, dedup_key, label, queue):
        """Worker-thread wrapper: run the task, swallow nothing, and keep the
        in-flight count / PROCESSING state correct on the way out."""
        with self._bg_lock:
            self._bg_active[queue] = self._bg_active.get(queue, 0) + 1
        try:
            fn(*args, **kwargs)
        except Exception as e:
            self.logger.error(
                f"{self.agent_name} background '{label}' raised: {e}", exc_info=True)
        finally:
            with self._bg_lock:
                self._bg_active[queue] -= 1
                self._bg_inflight -= 1
                if dedup_key is not None:
                    self._bg_keys.discard(dedup_key)
                if self._bg_inflight == 0:
                    self.set_ready()

    def get_background_pool_status(self):
        """Return a snapshot of each background worker pool's load.

        Returns a dict keyed by queue name, e.g.::

            {'default': {'max_workers': 4, 'active': 2, 'waiting': 5}}

        - ``active``: tasks currently executing on that queue's pool.
        - ``waiting``: tasks submitted to that queue but not yet picked up by
          a worker thread.

        Only queues that have had at least one ``run_in_background`` call are
        included (pools are created lazily).
        """
        status = {}
        with self._bg_lock:
            for queue, pool in self._bg_executor.items():
                status[queue] = {
                    'max_workers': pool._max_workers,
                    'active': self._bg_active.get(queue, 0),
                    'waiting': pool._work_queue.qsize(),
                }
        return status

    def get_next_agent_id(self):
        """Get the next agent ID from persistent state API."""
        return get_next_agent_id(self.monitor_url, self.api, logging.getLogger(__name__))

    def send_message(self, destination, message_body):
        """
        Sends a JSON message to a specific destination.

        Args:
            destination: ActiveMQ destination with explicit prefix.
                Must start with '/queue/' (anycast) or '/topic/' (multicast).
            message_body: Dict to send as JSON. 'sender' and 'namespace' auto-injected.

        Raises:
            ValueError: If destination doesn't have /queue/ or /topic/ prefix
            Exception: If the send fails and a reconnect-and-resend also
                fails. A lost message is the caller's problem — swallowing
                it here abandons downstream state (run 102780, 2026-07-30).
        """
        # Validate destination has explicit prefix
        if not destination.startswith('/queue/') and not destination.startswith('/topic/'):
            raise ValueError(
                f"destination must start with '/queue/' or '/topic/', got: '{destination}'. "
                f"Use '/queue/{destination}' for anycast or '/topic/{destination}' for multicast."
            )

        # Auto-inject sender and namespace
        message_body['sender'] = self.agent_name
        if self.namespace:
            message_body['namespace'] = self.namespace
        else:
            logging.warning(
                f"Sending message without namespace (msg_type={message_body.get('msg_type', 'unknown')}). "
                "Configure namespace in testbed.toml to enable namespace filtering."
            )

        with self._send_lock:
            try:
                self.conn.send(body=json.dumps(message_body), destination=destination)
                logging.info(f"Sent message to '{destination}': {message_body}")
            except Exception as e:
                # Any send failure warrants one reconnect-and-resend; no
                # error-string filtering — NotConnectedException stringifies
                # without the word 'connection' and was slipping through.
                logging.error(f"Failed to send message to '{destination}': {e}")
                self.mq_connected = False
                time.sleep(1)  # Brief pause before retry
                if not self._attempt_reconnect():
                    logging.error(
                        f"Reconnection failed - could not send to '{destination}'")
                    raise
                try:
                    self.conn.send(body=json.dumps(message_body), destination=destination)
                    logging.info(f"Message sent successfully after reconnection to '{destination}'")
                except Exception as retry_e:
                    logging.error(f"Retry failed after reconnection: {retry_e}")
                    raise

    def _api_request(self, method, endpoint, json_data=None):
        """
        Helper method to make a request to the monitor API.
        Retries on transient failures (connection errors, 502/503/504).
        Fails immediately on 4xx and redirects.
        """
        url = f"{self.monitor_url}/api{endpoint}"
        try:
            response = api_request_with_retry(
                method, url, session=self.api, logger=logging.getLogger(__name__),
                json=json_data, allow_redirects=False,
            )
            if 300 <= response.status_code < 400:
                loc = response.headers.get('Location', 'unknown')
                msg = (f"API redirect (HTTP {response.status_code}) to {loc}. "
                       f"If behind Apache/OIDC, ensure API requests aren't redirected and Authorization is forwarded.")
                logging.error(msg)
                raise APIError(msg, response=response, url=url, method=method.upper())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 400:
                response_text = e.response.text.lower()
                if "already exists" in response_text and "subscriber" in response_text:
                    logging.info(f"Resource already exists (normal): {method.upper()} {url}")
                    return {"status": "already_exists"}

            logging.error(f"API request FAILED: {method.upper()} {url} - {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Response status: {e.response.status_code}")
                logging.error(f"Response body: {e.response.text}")
            raise APIError(f"Critical API failure - agent cannot continue: {method.upper()} {url} - {e}",
                          response=getattr(e, 'response', None), url=url, method=method.upper()) from e

    def send_heartbeat(self):
        """Registers the agent and sends a heartbeat to the monitor."""
        if self.DEBUG:
            logging.info("Sending heartbeat to monitor...")
        
        # Determine overall status based on MQ connection
        status = "OK" if getattr(self, 'mq_connected', False) else "WARNING"
        
        # Build description with connection details
        mq_status = "connected" if getattr(self, 'mq_connected', False) else "disconnected"
        description = f"{self.agent_type} agent. MQ: {mq_status}"
        
        payload = {
            "instance_name": self.agent_name,
            "agent_type": self.agent_type,
            "status": status,
            "description": description,
            "mq_connected": getattr(self, 'mq_connected', False),
            "pid": self.pid,
            "hostname": self.hostname,
            "operational_state": self.operational_state,
        }

        # Include namespace if configured
        if self.namespace:
            payload["namespace"] = self.namespace

        result = self._api_request('post', '/systemagents/heartbeat/', payload)
        if result:
            if self.DEBUG:
                logging.info(f"Heartbeat sent successfully. Status: {status}, MQ: {mq_status}")
        else:
            logging.warning("Failed to send heartbeat to monitor")
    
    def send_enhanced_heartbeat(self, workflow_metadata=None):
        """Send heartbeat with optional workflow metadata."""
        if self.DEBUG:
            logging.info("Sending heartbeat to monitor...")
        
        # Determine overall status based on MQ connection
        status = "OK" if getattr(self, 'mq_connected', False) else "WARNING"
        
        # Build description with connection details
        mq_status = "connected" if getattr(self, 'mq_connected', False) else "disconnected"
        description_parts = [f"{self.agent_type} agent", f"MQ: {mq_status}"]
        
        # Add workflow context if provided
        if workflow_metadata:
            for key, value in workflow_metadata.items():
                description_parts.append(f"{key}: {value}")
        
        description = ". ".join(description_parts)
        
        payload = {
            "instance_name": self.agent_name,
            "agent_type": self.agent_type,
            "status": status,
            "description": description,
            "mq_connected": getattr(self, 'mq_connected', False),
            "pid": self.pid,
            "hostname": self.hostname,
            "operational_state": self.operational_state,
            # Include workflow metadata in agent record
            "workflow_enabled": True if workflow_metadata else False,
            "current_stf_count": workflow_metadata.get('active_tasks', 0) if workflow_metadata else 0,
            "total_stf_processed": workflow_metadata.get('completed_tasks', 0) if workflow_metadata else 0
        }

        # Include namespace if configured
        if self.namespace:
            payload["namespace"] = self.namespace

        result = self._api_request('post', '/systemagents/heartbeat/', payload)
        if result:
            if self.DEBUG:
                logging.info(f"Heartbeat sent successfully")
            return True
        else:
            logging.warning("Failed to send heartbeat to monitor")
            return False

    def report_agent_status(self, status, message=None, error_details=None):
        """Report agent status change to monitor."""
        logging.info(f"Reporting agent status: {status}")

        description_parts = [f"{self.agent_type} agent"]
        if message:
            description_parts.append(message)
        if error_details:
            description_parts.append(f"Error: {error_details}")

        payload = {
            "instance_name": self.agent_name,
            "agent_type": self.agent_type,
            "status": status,
            "description": ". ".join(description_parts),
            "mq_connected": getattr(self, 'mq_connected', False),
            "pid": self.pid,
            "hostname": self.hostname,
            "operational_state": self.operational_state,
        }

        # Include namespace if configured
        if self.namespace:
            payload["namespace"] = self.namespace

        result = self._api_request('post', '/systemagents/heartbeat/', payload)
        if result:
            logging.info(f"Status reported successfully: {status}")
            return True
        else:
            logging.warning(f"Failed to report status: {status}")
            return False

    def check_monitor_health(self):
        """Check if monitor API is available."""
        try:
            result = self._api_request('get', '/systemagents/', None)
            if result is not None:
                logging.info("Monitor API is healthy")
                return True
            else:
                logging.warning("Monitor API is not responding")
                return False
        except Exception as e:
            logging.error(f"Monitor health check failed: {e}")
            return False
    
    def call_monitor_api(self, method, endpoint, json_data=None):
        """Generic monitor API call method for agent-specific implementations."""
        return self._api_request(method.lower(), endpoint, json_data)
    
    def register_subscriber(self):
        """Register this agent as a subscriber to its ActiveMQ queue."""
        logging.info(f"Registering subscriber for queue '{self.subscription_queue}'...")
        
        subscriber_data = {
            "subscriber_name": f"{self.agent_name}-{self.subscription_queue}",
            "description": f"{self.agent_type} agent subscribing to {self.subscription_queue}",
            "is_active": True,
            "fraction": 1.0  # Receives all messages
        }
        
        try:
            result = self._api_request('post', '/subscribers/', subscriber_data)
            if result:
                if result.get('status') == 'already_exists':
                    logging.info(f"Subscriber already registered: {subscriber_data['subscriber_name']}")
                    return True
                else:
                    logging.info(f"Subscriber registered successfully: {result.get('subscriber_name')}")
                    return True
            else:
                logging.error("Failed to register subscriber")
                return False
        except Exception as e:
            # Other registration failures are critical
            logging.error(f"Critical subscriber registration failure: {e}")
            raise e
