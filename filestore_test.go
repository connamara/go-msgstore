package msgstore

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// FileStoreTestSuite runs all tests in the MessageStoreTestSuite against the FileStore implementation
type FileStoreTestSuite struct {
	MessageStoreTestSuite
	fileStoreRootPath string
}

func (suite *FileStoreTestSuite) SetupTest() {
	// create settings
	suite.fileStoreRootPath = path.Join(os.TempDir(), fmt.Sprintf("FileStoreTestSuite-%d", os.Getpid()))
	settings := map[string]string{FileStorePath: path.Join(suite.fileStoreRootPath, fmt.Sprintf("%d", time.Now().UnixNano()))}

	// create store
	var err error
	suite.msgStore, err = NewFileStoreFactory(settings).Create("FIX.4.4-SENDER-TARGET")
	require.Nil(suite.T(), err)
}

func (suite *FileStoreTestSuite) TearDownTest() {
	suite.msgStore.Close()
	os.RemoveAll(suite.fileStoreRootPath)
}

func TestFileStoreTestSuite(t *testing.T) {
	suite.Run(t, new(FileStoreTestSuite))
}
