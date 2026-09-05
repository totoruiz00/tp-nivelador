package safe_socket

import "io"

func SendAll(socket io.Writer, bytes []byte) error {
	sended := 0

	for sended < len(bytes) {
		n, err := socket.Write(bytes[sended:])
		if err != nil {
			return err
		}
		sended += n
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buffer := make([]byte, size)
	readed := 0

	for readed < size {
		n, err := socket.Read(buffer[readed:])
		readed += n
		if err != nil {
			if err == io.EOF && readed == size {
				break
			}
			return nil, err
		}
	}
	return buffer, nil
}
