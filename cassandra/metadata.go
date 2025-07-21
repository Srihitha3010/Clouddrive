package cassandra

import (
	"github.com/gocql/gocql"
)

func SaveMetadata(session *gocql.Session, fileID gocql.UUID, fileName string, chunkIDs []string) error {
	return session.Query(`INSERT INTO cloud_drive.files (file_id, file_name, chunk_ids) VALUES (?, ?, ?)`,
		fileID, fileName, chunkIDs).Exec()
}

func GetMetadata(session *gocql.Session, fileID gocql.UUID) (string, []string, error) {
	var fileName string
	var chunkIDs []string
	err := session.Query(`SELECT file_name, chunk_ids FROM cloud_drive.files WHERE file_id = ?`, fileID).
		Scan(&fileName, &chunkIDs)
	return fileName, chunkIDs, err
}

func DeleteMetadata(session *gocql.Session, fileID gocql.UUID) error {
	return session.Query(`DELETE FROM cloud_drive.files WHERE file_id = ?`, fileID).Exec()
}
