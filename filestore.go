package msgstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"time"
)

const (
	// FileStorePath is the name of the filesystem directory that will be used.
	FileStorePath string = "FileStorePath"
)

type msgDef struct {
	offset int64
	size   int
}

type fileStoreFactory struct {
}

type fileStore struct {
	sessionID          string
	cache              *memoryStore
	offsets            map[int]msgDef
	bodyFname          string
	headerFname        string
	sessionFname       string
	senderSeqNumsFname string
	targetSeqNumsFname string
	bodyFile           *os.File
	headerFile         *os.File
	sessionFile        *os.File
	senderSeqNumsFile  *os.File
	targetSeqNumsFile  *os.File
}

// removeFile behaves like os.Remove, except that no error is returned if the file does not exist
func removeFile(fname string) error {
	err := os.Remove(fname)
	if (err != nil) && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// openOrCreateFile opens a file for reading and writing, creating it if necessary
func openOrCreateFile(fname string, perm os.FileMode) (f *os.File, err error) {
	if f, err = os.OpenFile(fname, os.O_RDWR, perm); err != nil {
		if f, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE, perm); err != nil {
			return nil, fmt.Errorf("error opening or creating file: %s: %s", fname, err.Error())
		}
	}
	return f, nil
}

// closeFile behaves like Close, except that no error is returned if the file does not exist
func closeFile(f *os.File) error {
	if f != nil {
		if err := f.Close(); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// NewFileStoreFactory returns a file-based implementation of MessageStoreFactory
func NewFileStoreFactory() MessageStoreFactory {
	return fileStoreFactory{}
}

// Create creates a new FileStore implementation of the MessageStore interface
func (f fileStoreFactory) Create(sessionID string, sessionSettings map[string]string) (msgStore MessageStore, err error) {
	dirname, ok := sessionSettings[FileStorePath]
	if !ok {
		return nil, fmt.Errorf("sessionID: %s: required setting not found: %s", sessionID, FileStorePath)
	}
	return newFileStore(sessionID, dirname)
}

func newFileStore(sessionID string, dirname string) (*fileStore, error) {
	if err := os.MkdirAll(dirname, os.ModePerm); err != nil {
		return nil, err
	}

	store := &fileStore{
		sessionID:          sessionID,
		cache:              &memoryStore{},
		offsets:            make(map[int]msgDef),
		bodyFname:          path.Join(dirname, fmt.Sprintf("%s.%s", sessionID, "body")),
		headerFname:        path.Join(dirname, fmt.Sprintf("%s.%s", sessionID, "header")),
		sessionFname:       path.Join(dirname, fmt.Sprintf("%s.%s", sessionID, "session")),
		senderSeqNumsFname: path.Join(dirname, fmt.Sprintf("%s.%s", sessionID, "senderseqnums")),
		targetSeqNumsFname: path.Join(dirname, fmt.Sprintf("%s.%s", sessionID, "targetseqnums")),
	}

	if err := store.Refresh(); err != nil {
		return nil, err
	}

	return store, nil
}

// Reset deletes the store files and sets the seqnums back to 1
func (store *fileStore) Reset() error {
	store.cache.Reset()
	if err := store.Close(); err != nil {
		return err
	}
	if err := removeFile(store.bodyFname); err != nil {
		return err
	}
	if err := removeFile(store.headerFname); err != nil {
		return err
	}
	if err := removeFile(store.sessionFname); err != nil {
		return err
	}
	if err := removeFile(store.senderSeqNumsFname); err != nil {
		return err
	}
	if err := removeFile(store.targetSeqNumsFname); err != nil {
		return err
	}
	return store.Refresh()
}

// Refresh closes the store files and then reloads from them
func (store *fileStore) Refresh() (err error) {
	store.cache.Reset()

	if err = store.Close(); err != nil {
		return err
	}

	creationTimePopulated, err := store.populateCache()
	if err != nil {
		return err
	}

	if store.bodyFile, err = openOrCreateFile(store.bodyFname, 0660); err != nil {
		return err
	}
	if store.headerFile, err = openOrCreateFile(store.headerFname, 0660); err != nil {
		return err
	}
	if store.sessionFile, err = openOrCreateFile(store.sessionFname, 0660); err != nil {
		return err
	}
	if store.senderSeqNumsFile, err = openOrCreateFile(store.senderSeqNumsFname, 0660); err != nil {
		return err
	}
	if store.targetSeqNumsFile, err = openOrCreateFile(store.targetSeqNumsFname, 0660); err != nil {
		return err
	}

	if !creationTimePopulated {
		if err := store.setSession(); err != nil {
			return err
		}
	}

	store.SetNextSenderMsgSeqNum(store.NextSenderMsgSeqNum())
	store.SetNextTargetMsgSeqNum(store.NextTargetMsgSeqNum())
	return nil
}

func (store *fileStore) populateCache() (creationTimePopulated bool, err error) {
	if tmpHeaderFile, err := os.Open(store.headerFname); err == nil {
		defer tmpHeaderFile.Close()
		for {
			var seqNum, size int
			var offset int64
			if cnt, err := fmt.Fscanf(tmpHeaderFile, "%d,%d,%d\n", &seqNum, &offset, &size); err != nil || cnt != 3 {
				break
			}
			store.offsets[seqNum] = msgDef{offset: offset, size: size}
		}
	}

	if timeBytes, err := ioutil.ReadFile(store.sessionFname); err == nil {
		var ctime time.Time
		if err := ctime.UnmarshalText(timeBytes); err == nil {
			store.cache.creationTime = ctime
			creationTimePopulated = true
		}
	}

	if senderSeqNumBytes, err := ioutil.ReadFile(store.senderSeqNumsFname); err == nil {
		if senderSeqNum, err := strconv.Atoi(string(senderSeqNumBytes)); err == nil {
			store.cache.SetNextSenderMsgSeqNum(senderSeqNum)
		}
	}

	if targetSeqNumBytes, err := ioutil.ReadFile(store.targetSeqNumsFname); err == nil {
		if targetSeqNum, err := strconv.Atoi(string(targetSeqNumBytes)); err == nil {
			store.cache.SetNextTargetMsgSeqNum(targetSeqNum)
		}
	}

	return creationTimePopulated, nil
}

func (store *fileStore) setSession() error {
	if _, err := store.sessionFile.Seek(0, os.SEEK_SET); err != nil {
		return fmt.Errorf("unable to rewind file: %s: %s", store.sessionFname, err.Error())
	}

	data, err := store.cache.CreationTime().MarshalText()
	if err != nil {
		return fmt.Errorf("unable to marshal session time to file: %s: %s", store.sessionFname, err.Error())
	}
	if _, err := store.sessionFile.Write(data); err != nil {
		return fmt.Errorf("unable to write to file: %s: %s", store.sessionFname, err.Error())
	}
	if err := store.sessionFile.Sync(); err != nil {
		return fmt.Errorf("unable to flush file: %s: %s", store.sessionFname, err.Error())
	}
	return nil
}

func (store *fileStore) setSeqNum(f *os.File, seqNum int) error {
	if _, err := f.Seek(0, os.SEEK_SET); err != nil {
		return fmt.Errorf("unable to rewind file: %s: %s", f.Name(), err.Error())
	}
	if _, err := fmt.Fprintf(f, "%019d", seqNum); err != nil {
		return fmt.Errorf("unable to write to file: %s: %s", f.Name(), err.Error())
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("unable to flush file: %s: %s", f.Name(), err.Error())
	}
	return nil
}

// NextSenderMsgSeqNum returns the next MsgSeqNum that will be sent
func (store *fileStore) NextSenderMsgSeqNum() int {
	return store.cache.NextSenderMsgSeqNum()
}

// NextTargetMsgSeqNum returns the next MsgSeqNum that should be received
func (store *fileStore) NextTargetMsgSeqNum() int {
	return store.cache.NextTargetMsgSeqNum()
}

// SetNextSenderMsgSeqNum sets the next MsgSeqNum that will be sent
func (store *fileStore) SetNextSenderMsgSeqNum(next int) error {
	store.cache.SetNextSenderMsgSeqNum(next)
	return store.setSeqNum(store.senderSeqNumsFile, next)
}

// SetNextTargetMsgSeqNum sets the next MsgSeqNum that should be received
func (store *fileStore) SetNextTargetMsgSeqNum(next int) error {
	store.cache.SetNextTargetMsgSeqNum(next)
	return store.setSeqNum(store.targetSeqNumsFile, next)
}

// IncrNextSenderMsgSeqNum increments the next MsgSeqNum that will be sent
func (store *fileStore) IncrNextSenderMsgSeqNum() error {
	store.cache.IncrNextSenderMsgSeqNum()
	return store.setSeqNum(store.senderSeqNumsFile, store.cache.NextSenderMsgSeqNum())
}

// IncrNextTargetMsgSeqNum increments the next MsgSeqNum that should be received
func (store *fileStore) IncrNextTargetMsgSeqNum() error {
	store.cache.IncrNextTargetMsgSeqNum()
	return store.setSeqNum(store.targetSeqNumsFile, store.cache.NextTargetMsgSeqNum())
}

// CreationTime returns the creation time of the store
func (store *fileStore) CreationTime() time.Time {
	return store.cache.CreationTime()
}

func (store *fileStore) SaveMessage(seqNum int, msg []byte) error {
	offset, err := store.bodyFile.Seek(0, os.SEEK_END)
	if err != nil {
		return fmt.Errorf("unable to seek to end of file: %s: %s", store.bodyFname, err.Error())
	}
	if _, err := store.headerFile.Seek(0, os.SEEK_END); err != nil {
		return fmt.Errorf("unable to seek to end of file: %s: %s", store.headerFname, err.Error())
	}
	if _, err := fmt.Fprintf(store.headerFile, "%d,%d,%d\n", seqNum, offset, len(msg)); err != nil {
		return fmt.Errorf("unable to write to file: %s: %s", store.headerFname, err.Error())
	}

	store.offsets[seqNum] = msgDef{offset: offset, size: len(msg)}

	if _, err := store.bodyFile.Write(msg); err != nil {
		return fmt.Errorf("unable to write to file: %s: %s", store.bodyFname, err.Error())
	}
	if err := store.bodyFile.Sync(); err != nil {
		return fmt.Errorf("unable to flush file: %s: %s", store.bodyFname, err.Error())
	}
	if err := store.headerFile.Sync(); err != nil {
		return fmt.Errorf("unable to flush file: %s: %s", store.headerFname, err.Error())
	}
	return nil
}

func (store *fileStore) getMessage(seqNum int) (msg []byte, found bool, err error) {
	msgInfo, found := store.offsets[seqNum]
	if !found {
		return
	}

	msg = make([]byte, msgInfo.size)
	if _, err = store.bodyFile.ReadAt(msg, msgInfo.offset); err != nil {
		return nil, true, fmt.Errorf("unable to read from file: %s: %s", store.bodyFname, err.Error())
	}

	return msg, true, nil
}

func (store *fileStore) GetMessages(beginSeqNum, endSeqNum int) ([][]byte, error) {
	var msgs [][]byte
	for seqNum := beginSeqNum; seqNum <= endSeqNum; seqNum++ {
		m, found, err := store.getMessage(seqNum)
		if err != nil {
			return nil, err
		}
		if found {
			msgs = append(msgs, m)
		}
	}
	return msgs, nil
}

// Close closes the store's files
func (store *fileStore) Close() error {
	if err := closeFile(store.bodyFile); err != nil {
		return err
	}
	if err := closeFile(store.headerFile); err != nil {
		return err
	}
	if err := closeFile(store.sessionFile); err != nil {
		return err
	}
	if err := closeFile(store.senderSeqNumsFile); err != nil {
		return err
	}
	if err := closeFile(store.targetSeqNumsFile); err != nil {
		return err
	}

	store.bodyFile = nil
	store.headerFile = nil
	store.sessionFile = nil
	store.senderSeqNumsFile = nil
	store.targetSeqNumsFile = nil

	return nil
}
