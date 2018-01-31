package msgstore

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// SqlStoreTestSuite runs all tests in the MessageStoreTestSuite against the SqlStore implementation
type SQLStoreTestSuite struct {
	MessageStoreTestSuite
	sqlStoreRootPath string
}

func (suite *SQLStoreTestSuite) SetupTest() {
	suite.sqlStoreRootPath = path.Join(os.TempDir(), fmt.Sprintf("SqlStoreTestSuite-%d", os.Getpid()))
	err := os.MkdirAll(suite.sqlStoreRootPath, os.ModePerm)
	require.Nil(suite.T(), err)
	sqlDriver := "sqlite3"
	sqlDsn := path.Join(suite.sqlStoreRootPath, fmt.Sprintf("%d.db", time.Now().UnixNano()))

	// create tables
	db, err := sql.Open(sqlDriver, sqlDsn)
	require.Nil(suite.T(), err)
	ddlFnames, err := filepath.Glob(fmt.Sprintf("_sql/%s/*.sql", sqlDriver))
	require.Nil(suite.T(), err)
	for _, fname := range ddlFnames {
		sqlBytes, err := ioutil.ReadFile(fname)
		require.Nil(suite.T(), err)
		_, err = db.Exec(string(sqlBytes))
		require.Nil(suite.T(), err)
	}

	// create settings
	sessionID := "FIX.4.4-SENDER-TARGET"
	settings := map[string]string{SQLStoreDriver: sqlDriver, SQLStoreDataSourceName: sqlDsn, SQLStoreConnMaxLifetime: "14400s"}

	// create store
	suite.msgStore, err = NewSQLStoreFactory().Create(sessionID, settings)
	require.Nil(suite.T(), err)
}

func (suite *SQLStoreTestSuite) TearDownTest() {
	suite.msgStore.Close()
	os.RemoveAll(suite.sqlStoreRootPath)
}

func TestSqlStoreTestSuite(t *testing.T) {
	suite.Run(t, new(SQLStoreTestSuite))
}
