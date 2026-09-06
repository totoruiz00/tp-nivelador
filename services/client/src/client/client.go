package client

import (
	"bufio"
	"errors"
	"net"
	"os"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	const mainAction = "process-bets"
	defer client.conn.Close()

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "err", err)
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("open-output-file", logger.Fail, "err", err)
		return err
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	if err := protocol.SendMessage(client.conn, client.config.AgencyId); err != nil {
		logger.Error("send-agency-id", logger.Fail, "err", err)
		return err
	}

	scanner := bufio.NewScanner(inputFile)
	betsAmount := 0
	batchLines := []string{}
	for scanner.Scan() {
		betLine := scanner.Text()
		batchLines = append(batchLines, betLine)
		betsAmount++

		if len(batchLines) == client.config.BatchSize {
			if err := client.sendBatch(batchLines); err != nil {
				return err
			}
			batchLines = []string{}
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-input-file", logger.Fail, "err", err)
		return err
	}

	if len(batchLines) > 0 {
		if err := client.sendBatch(batchLines); err != nil {
			return err
		}
	}

	if err := protocol.SendMessage(client.conn, protocol.FIN_MESSAGE); err != nil {
		logger.Error("send-fin", logger.Fail, "err", err)
		return err
	}

	winnersAmount := 0
	for {
		winnerRow, err := protocol.RecvMessage(client.conn)
		if err != nil {
			logger.Error("recv-winners", logger.Fail, "agency-id", client.config.AgencyId)
			return err
		}
		if winnerRow == protocol.FIN_MESSAGE {
			break
		}

		if _, err := writer.WriteString(winnerRow); err != nil {
			logger.Error("write-output-file", logger.Fail, "agency-id", client.config.AgencyId)
			return err
		}
		if err := writer.WriteByte('\n'); err != nil {
			logger.Error("write-output-file", logger.Fail, "agency-id", client.config.AgencyId)
			return err
		}

		winnersAmount++
	}

	logger.Info(
		mainAction,
		logger.Success,
		"agency-id", client.config.AgencyId,
		"bets-amount", betsAmount,
		"winners-amount", winnersAmount,
	)

	return nil
}

func (client *Client) sendBatch(batchLines []string) error {
	batchPayload := strings.Join(batchLines, "\n")

	if err := protocol.SendMessage(client.conn, batchPayload); err != nil {
		logger.Error("send-batch", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	ackPayload, err := protocol.RecvMessage(client.conn)
	if err != nil {
		logger.Error("recv-ack", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}
	if ackPayload != protocol.ACK_MESSAGE {
		logger.Error("recv-ack", logger.Fail, "agency-id", client.config.AgencyId)
		return errors.New("el servidor no confirmo el batch")
	}

	return nil
}
