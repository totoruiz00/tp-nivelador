package protocol

import (
	"encoding/binary"
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const LENGTH_HEADER_SIZE = 4

const FIN_MESSAGE = "FIN"

const ACK_MESSAGE = "OK"

func encodeLength(length int) []byte {
	header := make([]byte, LENGTH_HEADER_SIZE)
	binary.BigEndian.PutUint32(header, uint32(length))
	return header
}

func decodeLength(header []byte) int {
	return int(binary.BigEndian.Uint32(header))
}

func SendMessage(conn io.Writer, payload string) error {
	payloadBytes := []byte(payload)
	header := encodeLength(len(payloadBytes))
	message := append(header, payloadBytes...)
	return safe_socket.SendAll(conn, message)
}

func RecvMessage(conn io.Reader) (string, error) {
	header, err := safe_socket.RecvAll(conn, LENGTH_HEADER_SIZE)
	if err != nil {
		return "", err
	}
	payloadLength := decodeLength(header)

	payloadBytes, err := safe_socket.RecvAll(conn, payloadLength)
	if err != nil {
		return "", err
	}
	return string(payloadBytes), nil
}
