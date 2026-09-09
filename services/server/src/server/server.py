import socket
import threading
import logger
import protocol
from lottery import Lottery, Bet


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

    def _wait_for_quorum(self, agency_id: int) -> None:
        with self.quorum_condition:
            self.finished_agencies.add(agency_id)

            if len(self.finished_agencies) >= self.agency_quorum_min:
                self.quorum_condition.notify_all()
            else:
                while len(self.finished_agencies) < self.agency_quorum_min:
                    self.quorum_condition.wait()

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

                self._wait_for_quorum(agency_id)

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

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                client_thread = threading.Thread(
                    target=self._handle_client, args=(client_socket,)
                )
                client_thread.start()
