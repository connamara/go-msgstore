package msgstore

import (
	"github.com/stretchr/testify/suite"
	"log"
	"os"
	"testing"
)

func TestMongoStoreSuite(t *testing.T) {
	suite.Run(t, new(MongoStoreSuite))
}

type MongoStoreSuite struct {
	MessageStoreTestSuite
	mongoCxn  string
	sessionID string
}

func (s *MongoStoreSuite) SetupTest() {
	s.mongoCxn = os.Getenv("MONGODB_TEST_CXN")
	if len(s.mongoCxn) <= 0 {
		log.Println("MONGODB_TEST_CXN environment arg is not provided, skipping...")
		s.T().SkipNow()
	}

	factory := NewMongoStoreFactory(s.mongoCxn, "automated_testing_mongostore")
	s.sessionID = ""
	msgStore, err := factory.Create(s.sessionID)
	s.Require().Nil(err)
	s.msgStore = msgStore
}

func (s *MongoStoreSuite) TeardownTest() {
	s.msgStore.Close()
}
