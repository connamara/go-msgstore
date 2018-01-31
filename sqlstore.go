package msgstore

import (
	"database/sql"
	"fmt"
	"time"
)

const (
	// SQLStoreDriver is the driverName that will be passed to database/sql, e.g. "sqlite", "mysql".
	SQLStoreDriver string = "SQLStoreDriver"
	// SQLStoreDataSourceName is the dataSourceName that will be passed to database/sql.
	SQLStoreDataSourceName string = "SQLStoreDataSourceName"
	// SQLStoreConnMaxLifetime is the value that will be passed to database/sql SetConnMaxLifetime.
	SQLStoreConnMaxLifetime string = "SQLStoreConnMaxLifetime"
	// SQLStoreTableNamePrefix will be prepended to the names of the database tables.  Optional.
	SQLStoreTableNamePrefix string = "SQLStoreTableNamePrefix"
)

type sqlStoreFactory struct {
}

type sqlStore struct {
	sessionID          string
	cache              *memoryStore
	sqlDriver          string
	sqlDataSourceName  string
	sqlConnMaxLifetime time.Duration
	sqlTableNamePrefix string
	db                 *sql.DB
}

// NewSQLStoreFactory returns a sql-based implementation of MessageStoreFactory
func NewSQLStoreFactory() MessageStoreFactory {
	return sqlStoreFactory{}
}

// Create creates a new SQLStore implementation of the MessageStore interface
func (f sqlStoreFactory) Create(sessionID string, sessionSettings map[string]string) (msgStore MessageStore, err error) {
	sqlDriver, ok := sessionSettings[SQLStoreDriver]
	if !ok {
		return nil, fmt.Errorf("sessionID: %s: required setting not found: %s", sessionID, SQLStoreDriver)
	}

	sqlDataSourceName, ok := sessionSettings[SQLStoreDataSourceName]
	if !ok {
		return nil, fmt.Errorf("sessionID: %s: required setting not found: %s", sessionID, SQLStoreDataSourceName)
	}

	sqlConnMaxLifetime := 0 * time.Second
	if durationStr, ok := sessionSettings[SQLStoreConnMaxLifetime]; ok {
		sqlConnMaxLifetime, err = time.ParseDuration(durationStr)
		if err != nil {
			return nil, err
		}
	}

	sqlTableNamePrefix, ok := sessionSettings[SQLStoreTableNamePrefix]
	if !ok {
		sqlTableNamePrefix = ""
	}

	return newSQLStore(sessionID, sqlDriver, sqlDataSourceName, sqlConnMaxLifetime, sqlTableNamePrefix)
}

func newSQLStore(sessionID string, driver string, dataSourceName string, connMaxLifetime time.Duration, tableNamePrefix string) (store *sqlStore, err error) {
	store = &sqlStore{
		sessionID:          sessionID,
		cache:              &memoryStore{},
		sqlDriver:          driver,
		sqlDataSourceName:  dataSourceName,
		sqlConnMaxLifetime: connMaxLifetime,
		sqlTableNamePrefix: tableNamePrefix,
	}
	store.cache.Reset()

	if store.db, err = sql.Open(store.sqlDriver, store.sqlDataSourceName); err != nil {
		return nil, err
	}
	store.db.SetConnMaxLifetime(store.sqlConnMaxLifetime)

	if err = store.db.Ping(); err != nil { // ensure immediate connection
		return nil, err
	}
	if err = store.populateCache(); err != nil {
		return nil, err
	}

	return store, nil
}

// Reset deletes the store records and sets the seqnums back to 1
func (store *sqlStore) Reset() error {
	_, err := store.db.Exec(fmt.Sprintf(`DELETE FROM %smessages WHERE session_id=?`, store.sqlTableNamePrefix), store.sessionID)
	if err != nil {
		return err
	}

	if err = store.cache.Reset(); err != nil {
		return err
	}

	_, err = store.db.Exec(fmt.Sprintf(`UPDATE %ssessions SET creation_time=?, incoming_seqnum=?, outgoing_seqnum=? WHERE session_id=?`, store.sqlTableNamePrefix), store.cache.CreationTime(), store.cache.NextTargetMsgSeqNum(), store.cache.NextSenderMsgSeqNum(), store.sessionID)

	return err
}

// Refresh reloads the store from the database
func (store *sqlStore) Refresh() error {
	if err := store.cache.Reset(); err != nil {
		return err
	}
	return store.populateCache()
}

func (store *sqlStore) populateCache() (err error) {
	var creationTime time.Time
	var incomingSeqNum, outgoingSeqNum int
	row := store.db.QueryRow(fmt.Sprintf(`SELECT creation_time, incoming_seqnum, outgoing_seqnum FROM %ssessions WHERE session_id=?`, store.sqlTableNamePrefix), store.sessionID)
	err = row.Scan(&creationTime, &incomingSeqNum, &outgoingSeqNum)

	// session record found, load it
	if err == nil {
		store.cache.creationTime = creationTime
		store.cache.SetNextTargetMsgSeqNum(incomingSeqNum)
		store.cache.SetNextSenderMsgSeqNum(outgoingSeqNum)
		return nil
	}

	// fatal error, give up
	if err != sql.ErrNoRows {
		return err
	}

	// session record not found, create it
	_, err = store.db.Exec(fmt.Sprintf(`INSERT INTO %ssessions (creation_time, incoming_seqnum, outgoing_seqnum, session_id) VALUES(?, ?, ?, ?)`, store.sqlTableNamePrefix), store.cache.creationTime, store.cache.NextTargetMsgSeqNum(), store.cache.NextSenderMsgSeqNum(), store.sessionID)

	return err
}

// NextSenderMsgSeqNum returns the next MsgSeqNum that will be sent
func (store *sqlStore) NextSenderMsgSeqNum() int {
	return store.cache.NextSenderMsgSeqNum()
}

// NextTargetMsgSeqNum returns the next MsgSeqNum that should be received
func (store *sqlStore) NextTargetMsgSeqNum() int {
	return store.cache.NextTargetMsgSeqNum()
}

// SetNextSenderMsgSeqNum sets the next MsgSeqNum that will be sent
func (store *sqlStore) SetNextSenderMsgSeqNum(next int) error {
	_, err := store.db.Exec(fmt.Sprintf(`UPDATE %ssessions SET outgoing_seqnum = ? WHERE session_id=?`, store.sqlTableNamePrefix), next, store.sessionID)
	if err != nil {
		return err
	}
	return store.cache.SetNextSenderMsgSeqNum(next)
}

// SetNextTargetMsgSeqNum sets the next MsgSeqNum that should be received
func (store *sqlStore) SetNextTargetMsgSeqNum(next int) error {
	_, err := store.db.Exec(fmt.Sprintf(`UPDATE %ssessions SET incoming_seqnum = ? WHERE session_id=?`, store.sqlTableNamePrefix), next, store.sessionID)
	if err != nil {
		return err
	}
	return store.cache.SetNextTargetMsgSeqNum(next)
}

// IncrNextSenderMsgSeqNum increments the next MsgSeqNum that will be sent
func (store *sqlStore) IncrNextSenderMsgSeqNum() error {
	store.cache.IncrNextSenderMsgSeqNum()
	return store.SetNextSenderMsgSeqNum(store.cache.NextSenderMsgSeqNum())
}

// IncrNextTargetMsgSeqNum increments the next MsgSeqNum that should be received
func (store *sqlStore) IncrNextTargetMsgSeqNum() error {
	store.cache.IncrNextTargetMsgSeqNum()
	return store.SetNextTargetMsgSeqNum(store.cache.NextTargetMsgSeqNum())
}

// CreationTime returns the creation time of the store
func (store *sqlStore) CreationTime() time.Time {
	return store.cache.CreationTime()
}

func (store *sqlStore) SaveMessage(seqNum int, msg []byte) error {
	_, err := store.db.Exec(fmt.Sprintf(`INSERT INTO %smessages (msgseqnum, message, session_id) VALUES(?, ?, ?)`, store.sqlTableNamePrefix), seqNum, string(msg), store.sessionID)
	return err
}

func (store *sqlStore) GetMessages(beginSeqNum, endSeqNum int) ([][]byte, error) {
	var msgs [][]byte
	rows, err := store.db.Query(fmt.Sprintf(`SELECT message FROM %smessages WHERE session_id=? AND msgseqnum>=? AND msgseqnum<=? ORDER BY msgseqnum`, store.sqlTableNamePrefix), store.sessionID, beginSeqNum, endSeqNum)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var message string
		if err := rows.Scan(&message); err != nil {
			return nil, err
		}
		msgs = append(msgs, []byte(message))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return msgs, nil
}

// Close closes the store's database connection
func (store *sqlStore) Close() error {
	if store.db != nil {
		store.db.Close()
		store.db = nil
	}
	return nil
}
