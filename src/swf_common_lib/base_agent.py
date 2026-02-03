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
from pathlib import Path
from typing import Optional
from .api_utils import get_next_agent_id
from .config_utils import load_testbed_config, TestbedConfigError
from .mq_comms import mq_comms


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

    # Auto-activate virtual environment if not already active
    if "VIRTUAL_ENV" not in os.environ:
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
                if line and not line.startswith("#") and "=" in line:
                    if line.startswith("export "):
                        line = line[7:]  # Remove 'export '
                    key, value = line.split("=", 1)
                    value = value.strip("\"'")
                    # Skip entries with unexpanded shell variables (e.g., PATH=$PATH:...)
                    # These are already expanded by shell when it sourced ~/.env
                    if "$" in value:
                        continue
                    os.environ[key] = value

    # Unset proxy variables to prevent localhost routing through proxy
    for proxy_var in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
        if proxy_var in os.environ:
            del os.environ[proxy_var]

    return True


# Auto-setup environment when module is imported (unless already done)
if not os.getenv("SWF_ENV_LOADED"):
    setup_environment()
    os.environ["SWF_ENV_LOADED"] = "true"

# Import the centralized logging from swf-common-lib
from swf_common_lib.rest_logging import setup_rest_logging   # noqa: E402

# Configure base logging level with environment overrides
_quiet = os.getenv("SWF_AGENT_QUIET", "false").lower() in ("1", "true", "yes", "on")
_level_name = os.getenv("SWF_LOG_LEVEL", "WARNING" if _quiet else "INFO").upper()

# Validate log level and provide clear error for invalid values
# Use Python's built-in logging level definitions for maintainability
_valid_levels = set(logging._nameToLevel.keys()) - {
    "NOTSET"
}  # Exclude NOTSET from display
if _level_name not in logging._nameToLevel:
    print(
        f"WARNING: Invalid SWF_LOG_LEVEL '{_level_name}'. Valid levels: {', '.join(sorted(_valid_levels))}. Using INFO."
    )
    _level = logging.INFO
else:
    _level = logging._nameToLevel[_level_name]

logging.basicConfig(
    level=_level, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

# STOMP logging is very chatty; enable only if explicitly requested
stomp_logger = logging.getLogger("stomp")
if os.getenv("SWF_STOMP_DEBUG", "false").lower() in ("1", "true", "yes", "on"):
    stomp_logger.setLevel(logging.DEBUG)
    _stomp_handler = logging.StreamHandler()
    _stomp_handler.setLevel(logging.DEBUG)
    _stomp_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    )
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
        "run_imminent",
        "start_run",
        "pause_run",
        "resume_run",
        "end_run",
        "stf_gen",
        "stf_ready",
        "tf_file_registered",
    }

    def __init__(
        self,
        agent_type,
        subscription_queue,
        debug=False,
        config_path: Optional[str] = None,
    ):
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
        if not subscription_queue.startswith(
            "/queue/"
        ) and not subscription_queue.startswith("/topic/"):
            raise ValueError(
                f"subscription_queue must start with '/queue/' or '/topic/', got: '{subscription_queue}'. "
                f"Use '/queue/{subscription_queue}' for anycast or '/topic/{subscription_queue}' for multicast."
            )

        self.agent_type = agent_type
        self.subscription_queue = subscription_queue
        self.DEBUG = debug

        # Resolve config path: explicit arg > SWF_TESTBED_CONFIG env var > default
        if config_path is None:
            env_config = os.getenv("SWF_TESTBED_CONFIG")
            if env_config:
                # Env var is filename, resolve to workflows/ directory
                if not env_config.startswith("workflows/") and "/" not in env_config:
                    config_path = f"workflows/{env_config}"
                else:
                    config_path = env_config
            else:
                config_path = "workflows/testbed.toml"
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
        self.monitor_url = (os.getenv("SWF_MONITOR_URL") or "").rstrip("/")
        self.api_token = os.getenv("SWF_API_TOKEN")

        # Set up API session (needed for agent ID call)
        import requests

        self.api = requests.Session()
        if self.api_token:
            self.api.headers.update({"Authorization": f"Token {self.api_token}"})

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
        self.operational_state = "STARTING"  # STARTING, READY, PROCESSING, EXITED

        # Use HTTP URL for REST logging (no auth required)
        self.base_url = (os.getenv("SWF_MONITOR_HTTP_URL") or "").rstrip("/")

        self.mq_subscriber = self.get_subscriber(self.subscription_queue)

        # Set up centralized REST logging
        self.logger = setup_rest_logging("base_agent", self.agent_name, self.base_url)

        # For localhost development, disable SSL verification and proxy
        if "localhost" in self.monitor_url or "127.0.0.1" in self.monitor_url:
            self.api.verify = False
            # Disable proxy for localhost connections
            self.api.proxies = {"http": "", "https": ""}
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self._subscribers = {}  # Track registered subscribers
        self._subscribers[self.subscription_queue] = self.mq_subscriber
        self._publishers = {}  # Track registered publishers

    def _log_extra(self, **kwargs):
        """
        Build extra dict for logging with common context fields.

        Automatically includes username, execution_id, and run_id when set.
        Subclasses can override to add additional fields, calling super()._log_extra().

        Usage:
            self.logger.info("Message", extra=self._log_extra(custom_field=value))
        """
        extra = {"username": self.username}
        if self.current_execution_id:
            extra["execution_id"] = self.current_execution_id
        if self.current_run_id:
            extra["run_id"] = self.current_run_id
        extra.update(kwargs)
        return extra

    def get_publisher(self, queue_name):
        """
        Create a mq_comms.Sender for sending messages.

        args:
            queue_name: ActiveMQ destination.
        Returns:
            mq_comms.Sender: Configured publisher instance
        """
        publisher = self._publishers.get(queue_name, None)
        if publisher:
            publisher._attempt_reconnect()
            return publisher

        publisher = mq_comms.Sender(client_id=f"{self.agent_name}-publisher")
        publisher.connect()
        self._publishers[queue_name] = publisher
        return publisher

    def get_subscriber(self, queue_name):
        """
        Get the mq_comms.Receiver used by this agent.

        args:
            queue_name: ActiveMQ destination.
        Returns:
            mq_comms.Receiver: Configured subscriber instance
        """
        receiver = self._subscribers.get(queue_name, None)
        if receiver:
            receiver._attempt_reconnect()
            return receiver

        receiver = mq_comms.Receiver(
            listener=self, mq_topic=queue_name, client_id=self.agent_name
        )
        receiver.connect()
        self._subscribers[queue_name] = receiver
        return receiver

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
        if not getattr(self, "mq_connected", False):
            max_retries = 3
            retry_delay = 5
            for attempt in range(1, max_retries + 1):
                logging.info(
                    f"Connecting to ActiveMQ at {self.mq_host}:{self.mq_port} (attempt {attempt}/{max_retries})"
                )
                try:
                    self.mq_subscriber.connect()
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
            # Register as subscriber in monitor
            self.register_subscriber()

            # Agent is now ready and waiting for work
            self.set_ready()

            # Initial registration/heartbeat
            self.send_heartbeat()

            logging.info(f"{self.agent_name} is running. Press Ctrl+C to stop.")
            while True:
                time.sleep(
                    60
                )  # Keep the main thread alive, heartbeats can be added here

                # Check connection status and attempt reconnection if needed
                if not self.mq_connected:
                    self._attempt_reconnect()

                self.send_heartbeat()

        except KeyboardInterrupt:
            logging.info(f"Stopping {self.agent_name}...")
        except stomp.exception.ConnectFailedException as e:
            self.mq_connected = False
            logging.error(f"Failed to connect to ActiveMQ: {e}")
            logging.error(
                "Please check the connection details and ensure ActiveMQ is running."
            )
        except Exception as e:
            self.mq_connected = False
            logging.error(f"An unexpected error occurred: {e}")
            import traceback

            traceback.print_exc()
        finally:
            # Report exit status before disconnecting
            try:
                self.operational_state = "EXITED"
                self.report_agent_status("EXITED", "Agent shutdown")
            except Exception as e:
                logging.warning(f"Failed to report exit status: {e}")

            if self.mq_subscriber:
                self.mq_subscriber.disconnect()

    def on_connected(self, frame):
        """Handle successful connection to ActiveMQ."""
        logging.info(f"Successfully connected to ActiveMQ: {frame.headers}")
        self.mq_connected = True

    def on_error(self, frame):
        logging.error(
            f'Received an error from ActiveMQ: body="{frame.body}", headers={frame.headers}, cmd="{frame.cmd}"'
        )
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

        return self.mq_subscriber._attempt_reconnect()

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
            msg_type = message_data.get("msg_type", "unknown")

            # Namespace filtering
            msg_namespace = message_data.get("namespace")
            if self.namespace and msg_namespace and msg_namespace != self.namespace:
                logging.debug(
                    f"Ignoring message from namespace '{msg_namespace}' (ours: '{self.namespace}')"
                )
                return None, None

            if msg_type not in known_types:
                logging.info(
                    f"{self.agent_type} agent received unknown message type: {msg_type}",
                    extra={"msg_type": msg_type},
                )
            else:
                logging.info(f"{self.agent_type} agent received message: {msg_type}")

            return message_data, msg_type
        except json.JSONDecodeError as e:
            logging.error(f"CRITICAL: Failed to parse message JSON: {e}")
            raise RuntimeError(
                f"Message parsing failed - agent cannot continue: {e}"
            ) from e

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
        self.operational_state = "PROCESSING"
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
        self.operational_state = "READY"
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

    def get_next_agent_id(self):
        """Get the next agent ID from persistent state API."""
        return get_next_agent_id(
            self.monitor_url, self.api, logging.getLogger(__name__)
        )

    def send_message(self, destination, message_body):
        """
        Sends a JSON message to a specific destination.

        Args:
            destination: ActiveMQ destination with explicit prefix.
                Must start with '/queue/' (anycast) or '/topic/' (multicast).
            message_body: Dict to send as JSON. 'sender' and 'namespace' auto-injected.

        Raises:
            ValueError: If destination doesn't have /queue/ or /topic/ prefix
        """
        # Validate destination has explicit prefix
        if not destination.startswith("/queue/") and not destination.startswith(
            "/topic/"
        ):
            raise ValueError(
                f"destination must start with '/queue/' or '/topic/', got: '{destination}'. "
                f"Use '/queue/{destination}' for anycast or '/topic/{destination}' for multicast."
            )

        # Auto-inject sender and namespace
        message_body["sender"] = self.agent_name
        if self.namespace:
            message_body["namespace"] = self.namespace
        else:
            logging.warning(
                f"Sending message without namespace (msg_type={message_body.get('msg_type', 'unknown')}). "
                "Configure namespace in testbed.toml to enable namespace filtering."
            )

        try:
            publisher = self.get_publisher(destination)
            publisher.send(destination=destination, body=json.dumps(message_body))
            logging.info(f"Sent message to '{destination}': {message_body}")
        except Exception as e:
            logging.error(f"Failed to send message to '{destination}': {e}")

            # Check for SSL/connection errors that indicate disconnection
            if any(
                error_type in str(e).lower()
                for error_type in ["ssl", "eof", "connection", "broken pipe"]
            ):
                logging.warning("Connection error detected - attempting recovery")
                self.mq_connected = False
                time.sleep(1)  # Brief pause before retry
                if publisher._attempt_reconnect():
                    try:
                        publisher.send(
                            destination=destination, body=json.dumps(message_body)
                        )
                        logging.info(
                            f"Message sent successfully after reconnection to '{destination}'"
                        )
                    except Exception as retry_e:
                        logging.error(f"Retry failed after reconnection: {retry_e}")
                else:
                    logging.error("Reconnection failed - message lost")

    def _api_request(self, method, endpoint, json_data=None):
        """
        Helper method to make a request to the monitor API.
        FAILS FAST - raises exception on any API error.
        """
        url = f"{self.monitor_url}/api{endpoint}"
        try:
            # Do not follow redirects; 3xx usually indicates upstream auth middleware (e.g., OIDC)
            response = self.api.request(
                method, url, json=json_data, timeout=10, allow_redirects=False
            )
            # Treat redirect as auth/config problem with a clear message
            if 300 <= response.status_code < 400:
                loc = response.headers.get("Location", "unknown")
                msg = (
                    f"API redirect (HTTP {response.status_code}) to {loc}. "
                    f"If behind Apache/OIDC, ensure API requests aren't redirected and Authorization is forwarded."
                )
                logging.error(msg)
                raise APIError(msg, response=response, url=url, method=method.upper())
            response.raise_for_status()  # Raise an exception for bad status codes
            return response.json()
        except requests.exceptions.RequestException as e:
            # Check for "already exists" error in subscriber registration
            if (
                hasattr(e, "response")
                and e.response is not None
                and e.response.status_code == 400
            ):
                response_text = e.response.text.lower()
                if "already exists" in response_text and "subscriber" in response_text:
                    # This is a normal "already exists" case for subscriber registration
                    logging.info(
                        f"Resource already exists (normal): {method.upper()} {url}"
                    )
                    return {"status": "already_exists"}

            logging.error(f"API request FAILED: {method.upper()} {url} - {e}")
            if hasattr(e, "response") and e.response is not None:
                logging.error(f"Response status: {e.response.status_code}")
                logging.error(f"Response body: {e.response.text}")
            raise APIError(
                f"Critical API failure - agent cannot continue: {method.upper()} {url} - {e}",
                response=getattr(e, "response", None),
                url=url,
                method=method.upper(),
            ) from e

    def send_heartbeat(self):
        """Registers the agent and sends a heartbeat to the monitor."""
        if self.DEBUG:
            logging.info("Sending heartbeat to monitor...")

        # Determine overall status based on MQ connection
        status = "OK" if getattr(self, "mq_connected", False) else "WARNING"

        # Build description with connection details
        mq_status = (
            "connected" if getattr(self, "mq_connected", False) else "disconnected"
        )
        description = f"{self.agent_type} agent. MQ: {mq_status}"

        payload = {
            "instance_name": self.agent_name,
            "agent_type": self.agent_type,
            "status": status,
            "description": description,
            "mq_connected": getattr(self, "mq_connected", False),
            "pid": self.pid,
            "hostname": self.hostname,
            "operational_state": self.operational_state,
        }

        # Include namespace if configured
        if self.namespace:
            payload["namespace"] = self.namespace

        result = self._api_request("post", "/systemagents/heartbeat/", payload)
        if result:
            if self.DEBUG:
                logging.info(
                    f"Heartbeat sent successfully. Status: {status}, MQ: {mq_status}"
                )
        else:
            logging.warning("Failed to send heartbeat to monitor")

    def send_enhanced_heartbeat(self, workflow_metadata=None):
        """Send heartbeat with optional workflow metadata."""
        if self.DEBUG:
            logging.info("Sending heartbeat to monitor...")

        # Determine overall status based on MQ connection
        status = "OK" if getattr(self, "mq_connected", False) else "WARNING"

        # Build description with connection details
        mq_status = (
            "connected" if getattr(self, "mq_connected", False) else "disconnected"
        )
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
            "mq_connected": getattr(self, "mq_connected", False),
            "pid": self.pid,
            "hostname": self.hostname,
            "operational_state": self.operational_state,
            # Include workflow metadata in agent record
            "workflow_enabled": True if workflow_metadata else False,
            "current_stf_count": (
                workflow_metadata.get("active_tasks", 0) if workflow_metadata else 0
            ),
            "total_stf_processed": (
                workflow_metadata.get("completed_tasks", 0) if workflow_metadata else 0
            ),
        }

        # Include namespace if configured
        if self.namespace:
            payload["namespace"] = self.namespace

        result = self._api_request("post", "/systemagents/heartbeat/", payload)
        if result:
            if self.DEBUG:
                logging.info("Heartbeat sent successfully")
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
            "mq_connected": getattr(self, "mq_connected", False),
            "pid": self.pid,
            "hostname": self.hostname,
            "operational_state": self.operational_state,
        }

        # Include namespace if configured
        if self.namespace:
            payload["namespace"] = self.namespace

        result = self._api_request("post", "/systemagents/heartbeat/", payload)
        if result:
            logging.info(f"Status reported successfully: {status}")
            return True
        else:
            logging.warning(f"Failed to report status: {status}")
            return False

    def check_monitor_health(self):
        """Check if monitor API is available."""
        try:
            result = self._api_request("get", "/systemagents/", None)
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
            "fraction": 1.0,  # Receives all messages
        }

        try:
            result = self._api_request("post", "/subscribers/", subscriber_data)
            if result:
                if result.get("status") == "already_exists":
                    logging.info(
                        f"Subscriber already registered: {subscriber_data['subscriber_name']}"
                    )
                    return True
                else:
                    logging.info(
                        f"Subscriber registered successfully: {result.get('subscriber_name')}"
                    )
                    return True
            else:
                logging.error("Failed to register subscriber")
                return False
        except Exception as e:
            # Other registration failures are critical
            logging.error(f"Critical subscriber registration failure: {e}")
            raise e
