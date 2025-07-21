package cassandra

import (
    "log"
    "github.com/gocql/gocql"
)

// CreateSession connects to the Cassandra cluster and returns a session
func CreateSession() (*gocql.Session, error) {
    cluster := gocql.NewCluster("127.0.0.1") // Replace with your Cassandra node IP
    cluster.Keyspace = "clouddrive"          // Make sure this keyspace exists
    cluster.Consistency = gocql.Quorum

    session, err := cluster.CreateSession()
    if err != nil {
        log.Printf("Failed to connect to Cassandra: %v", err)
        return nil, err
    }

    return session, nil
}
