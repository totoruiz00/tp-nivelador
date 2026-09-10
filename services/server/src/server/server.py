import socket
import signal
import threading
import time
import logger
import protocol
from lottery import Lottery, Bet

SHUTDOWN_TIMEOUT_SECONDS = 4


class Server:
    def __init__(
        self,
        server_host: str,
        server_port: int,
        lottery: Lottery,
        agency_quorum_min: int,
    ) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = lottery
        self.agency_quorum_min = agency_quorum_min

        self.lock = threading.Lock()
        self.quorum_condition = threading.Condition(self.lock)
        self.finished_agencies = set()

        self.shutdown_event = threading.Event()
        self.server_socket = None
        self.client_sockets = set()
        self.client_threads = []

    def _handle_sigterm(self, signum, frame):
        logger.info("sigterm", logger.LogResult.in_progress)
        self.shutdown_event.set()

        with self.quorum_condition:
            self.quorum_condition.notify_all()

        with self.lock:
            for client_socket in self.client_sockets:
                try:
                    client_socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                client_socket.close()

        if self.server_socket is not None:
            self.server_socket.close()

    def _wait_for_quorum(self, agency_id: int) -> bool:
        with self.quorum_condition:
            self.finished_agencies.add(agency_id)

            if len(self.finished_agencies) >= self.agency_quorum_min:
                self.quorum_condition.notify_all()
            else:
                while len(self.finished_agencies) < self.agency_quorum_min:
                    if self.shutdown_event.is_set():
                        return False
                    self.quorum_condition.wait()

            return not self.shutdown_event.is_set()

    def _handle_client(self, client_socket):
        action = "handle-client"
        bets_amount = 0
        with client_socket:
            try:
                logger.info(action, logger.LogResult.in_progress)

                agency_id_payload = protocol.recv_message(client_socket)
                agency_id = int(agency_id_payload)

                while True:
                    payload = protocol.recv_message(client_socket)
                    if payload == protocol.FIN_MESSAGE:
                        break

                    batch_lines = payload.split("\n")
                    batch_bets = []
                    for line in batch_lines:
                        fields = line.split(",")
                        first_name = fields[0]
                        last_name = fields[1]
                        document = int(fields[2])
                        birthdate = fields[3]
                        number = int(fields[4])
                        bet = Bet(agency_id, first_name, last_name, document, birthdate, number)
                        batch_bets.append(bet)

                    with self.lock:
                        self.lottery.store_bets(batch_bets)
                    protocol.send_message(client_socket, protocol.ACK_MESSAGE)
                    bets_amount += len(batch_bets)

                quorum_reached = self._wait_for_quorum(agency_id)
                if not quorum_reached:
                    return

                winning_bets = []
                with self.lock:
                    all_bets = self.lottery.load_bets()
                    for bet in all_bets:
                        if bet.agency_id == agency_id:
                            if self.lottery.has_won(bet):
                                winning_bets.append(bet)

                for bet in winning_bets:
                    row = (
                        bet.first_name
                        + ","
                        + bet.last_name
                        + ","
                        + str(bet.document)
                        + ","
                        + bet.birthdate
                        + ","
                        + str(bet.number)
                    )
                    protocol.send_message(client_socket, row)
                protocol.send_message(client_socket, protocol.FIN_MESSAGE)

                logger.info(
                    action,
                    logger.LogResult.success,
                    "bets-amount",
                    bets_amount,
                    "winners-amount",
                    len(winning_bets),
                )
            except Exception as e:
                logger.error(action, logger.LogResult.fail, "bets-amount", bets_amount)
                raise e
            finally:
                with self.lock:
                    self.client_sockets.discard(client_socket)

    def run(self):
        action = "accept-connection"
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            self.server_socket = server_socket
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except OSError as e:
                    if self.shutdown_event.is_set():
                        break
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                with self.lock:
                    self.client_sockets.add(client_socket)

                client_thread = threading.Thread(
                    target=self._handle_client, args=(client_socket,), daemon=True
                )
                client_thread.start()

                still_running_threads = []
                for thread in self.client_threads:
                    if thread.is_alive():
                        still_running_threads.append(thread)
                self.client_threads = still_running_threads
                self.client_threads.append(client_thread)

        shutdown_deadline = time.monotonic() + SHUTDOWN_TIMEOUT_SECONDS
        threads_still_running = 0
        for client_thread in self.client_threads:
            remaining_seconds = shutdown_deadline - time.monotonic()
            if remaining_seconds > 0:
                client_thread.join(timeout=remaining_seconds)
            if client_thread.is_alive():
                threads_still_running += 1

        if threads_still_running > 0:
            logger.error(
                "sigterm",
                logger.LogResult.fail,
                "threads-still-running",
                threads_still_running,
            )
        else:
            logger.info("sigterm", logger.LogResult.success)
