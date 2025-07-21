package utils

import (
    "io"
)

// ChunkFile splits the input into fixed-size chunks (e.g., 5MB).
func ChunkFile(reader io.Reader, chunkSize int) ([][]byte, error) {
    var chunks [][]byte
    buffer := make([]byte, chunkSize)

    for {
        n, err := reader.Read(buffer)
        if err != nil && err != io.EOF {
            return nil, err
        }
        if n == 0 {
            break
        }

        // Copy only the read portion to avoid reusing the buffer
        chunk := make([]byte, n)
        copy(chunk, buffer[:n])
        chunks = append(chunks, chunk)
    }

    return chunks, nil
}
