package msgstore

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"time"
)

type mongoStoreFactory struct {
	dbURL       string
	dbName      string
	tablePrefix string
}

type mongoStore struct {
	sessionID          string
	cache              *memoryStore
	creationTime       time.Time
	dbCtx              *mgo.Session
	dbName             string
	messagesCollection string
	sessionsCollection string
}

// NewMongoStoreFactory returns a transactional, mongo-based implementation of MessageStoreFactory
func NewMongoStoreFactory(dbURL string, dbName string) MessageStoreFactory {
	return NewMongoStoreFactoryWithTablePrefix(dbURL, dbName, "")
}

//NewMongoStoreFactoryWithTablePrefix returns an initialized MessageStoreFactory that will use the provided prefix for table names
func NewMongoStoreFactoryWithTablePrefix(dbURL string, dbName string, tablePrefix string) MessageStoreFactory {
	return mongoStoreFactory{dbURL: dbURL, dbName: dbName, tablePrefix: tablePrefix}
}

// Create creates a new MongoStore implementation of the MessageStore interface
func (f mongoStoreFactory) Create(sessionID string) (msgStore MessageStore, err error) {
	return newmongoStore(f.dbURL, sessionID, f.dbName, f.tablePrefix)
}

type sessionData struct {
	SessionID      string    `bson:"session_id"`
	CreationTime   time.Time `bson:"creation_time,omitempty"`
	IncomingSeqNum int       `bson:"incoming_seq_num,omitempty"`
	OutgoingSeqNum int       `bson:"outgoing_seq_num,omitempty"`
}

type messageData struct {
	SessionID string `bson:"session_id"`
	Message   []byte `bson:"message,omitempty"`
	MsgSeqNum int    `bson:"msg_seq_num,omitempty"`
}

func newmongoStore(dbURL string, sessionID string, dbName string, tablePrefix string) (store *mongoStore, err error) {
	store = &mongoStore{
		sessionID:          sessionID,
		creationTime:       time.Now(),
		dbName:             dbName,
		cache:              &memoryStore{},
		messagesCollection: tablePrefix + "messages",
		sessionsCollection: tablePrefix + "sessions",
	}

	if store.dbCtx, err = mgo.Dial(dbURL); err != nil {
		return
	}

	if err = store.cache.Reset(); err != nil {
		return
	}

	if err = store.populateCache(); err != nil {
		return
	}

	return
}

// Reset deletes the store records and sets the seqnums back to 1
func (store *mongoStore) Reset() (err error) {
	messageFilter := &messageData{SessionID: store.sessionID}

	if _, err = store.dbCtx.DB(store.dbName).C(store.messagesCollection).RemoveAll(messageFilter); err != nil {
		return
	}

	if err = store.cache.Reset(); err != nil {
		return
	}

	store.creationTime = time.Now()
	sessionFilter := &sessionData{SessionID: store.sessionID}
	sessionUpdate := &sessionData{
		SessionID:      store.sessionID,
		CreationTime:   store.creationTime,
		IncomingSeqNum: store.cache.NextTargetMsgSeqNum(),
		OutgoingSeqNum: store.cache.NextSenderMsgSeqNum(),
	}
	err = store.dbCtx.DB(store.dbName).C(store.sessionsCollection).Update(sessionFilter, sessionUpdate)
	return
}

// Refresh reloads the store from the database
func (store *mongoStore) Refresh() error {
	if err := store.cache.Reset(); err != nil {
		return err
	}
	return store.populateCache()
}

func (store *mongoStore) populateCache() error {
	sessionFilter := &sessionData{SessionID: store.sessionID}
	query := store.dbCtx.DB(store.dbName).C(store.sessionsCollection).Find(sessionFilter)
	if cnt, errCnt := query.Count(); errCnt != nil {
		return errCnt
	} else if cnt > 0 {
		// session record found, load it
		sessionData := &sessionData{}
		if errQuery := query.One(sessionData); errQuery != nil {
			return errQuery
		}
		store.creationTime = sessionData.CreationTime
		if errSet := store.cache.SetNextTargetMsgSeqNum(sessionData.IncomingSeqNum); errSet != nil {
			return errSet
		}
		if errSet := store.cache.SetNextSenderMsgSeqNum(sessionData.OutgoingSeqNum); errSet != nil {
			return errSet
		}
	} else {
		sessionFilter.IncomingSeqNum = store.cache.NextTargetMsgSeqNum()
		sessionFilter.OutgoingSeqNum = store.cache.NextSenderMsgSeqNum()
		sessionFilter.CreationTime = store.creationTime
		if errInsert := store.dbCtx.DB(store.dbName).C(store.sessionsCollection).Insert(sessionFilter); errInsert != nil {
			return errInsert
		}
	}
	return nil
}

// NextSenderMsgSeqNum returns the next MsgSeqNum that will be sent
func (store *mongoStore) NextSenderMsgSeqNum() int {
	return store.cache.NextSenderMsgSeqNum()
}

// NextTargetMsgSeqNum returns the next MsgSeqNum that should be received
func (store *mongoStore) NextTargetMsgSeqNum() int {
	return store.cache.NextTargetMsgSeqNum()
}

// SetNextSenderMsgSeqNum sets the next MsgSeqNum that will be sent
func (store *mongoStore) SetNextSenderMsgSeqNum(next int) error {
	sessionFilter := &sessionData{SessionID: store.sessionID}
	sessionUpdate := &sessionData{
		SessionID:      store.sessionID,
		IncomingSeqNum: store.cache.NextTargetMsgSeqNum(),
		OutgoingSeqNum: next,
		CreationTime:   store.creationTime,
	}
	if err := store.dbCtx.DB(store.dbName).C(store.sessionsCollection).Update(sessionFilter, sessionUpdate); err != nil {
		return err
	}
	return store.cache.SetNextSenderMsgSeqNum(next)
}

// SetNextTargetMsgSeqNum sets the next MsgSeqNum that should be received
func (store *mongoStore) SetNextTargetMsgSeqNum(next int) error {
	sessionFilter := &sessionData{SessionID: store.sessionID}
	sessionUpdate := &sessionData{
		SessionID:      store.sessionID,
		IncomingSeqNum: next,
		OutgoingSeqNum: store.cache.NextSenderMsgSeqNum(),
		CreationTime:   store.creationTime,
	}
	if err := store.dbCtx.DB(store.dbName).C(store.sessionsCollection).Update(sessionFilter, sessionUpdate); err != nil {
		return err
	}
	return store.cache.SetNextTargetMsgSeqNum(next)
}

// IncrNextSenderMsgSeqNum increments the next MsgSeqNum that will be sent
func (store *mongoStore) IncrNextSenderMsgSeqNum() error {
	if err := store.cache.IncrNextSenderMsgSeqNum(); err != nil {
		return err
	}
	return store.SetNextSenderMsgSeqNum(store.cache.NextSenderMsgSeqNum())
}

// IncrNextTargetMsgSeqNum increments the next MsgSeqNum that should be received
func (store *mongoStore) IncrNextTargetMsgSeqNum() error {
	if err := store.cache.IncrNextTargetMsgSeqNum(); err != nil {
		return err
	}
	return store.SetNextTargetMsgSeqNum(store.cache.NextTargetMsgSeqNum())
}

// CreationTime returns the creation time of the store
func (store *mongoStore) CreationTime() time.Time {
	return store.creationTime
}

func (store *mongoStore) SaveMessage(seqNum int, msg []byte) (err error) {
	messageInsert := &messageData{
		MsgSeqNum: seqNum,
		Message:   msg,
		SessionID: store.sessionID,
	}
	err = store.dbCtx.DB(store.dbName).C(store.messagesCollection).Insert(messageInsert)
	return
}

func (store *mongoStore) GetMessages(beginSeqNum, endSeqNum int) (msgs [][]byte, err error) {
	msgFilter := &messageData{SessionID: store.sessionID}
	//Marshal into database form
	msgFilterBytes, err := bson.Marshal(msgFilter)
	if err != nil {
		return
	}
	seqFilter := bson.M{}
	err = bson.Unmarshal(msgFilterBytes, &seqFilter)
	if err != nil {
		return
	}
	//Modify the query to use a range for the sequence filter
	seqFilter["msg_seq_num"] = bson.M{
		"$gte": beginSeqNum,
		"$lte": endSeqNum,
	}

	iter := store.dbCtx.DB(store.dbName).C(store.messagesCollection).Find(seqFilter).Sort("msg_seq_num").Iter()
	for iter.Next(msgFilter) {
		msgs = append(msgs, msgFilter.Message)
	}
	err = iter.Close()
	return
}

func (store *mongoStore) Close() error {
	store.dbCtx.Close()
	return nil
}
