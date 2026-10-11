package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"gopkg.in/yaml.v3"
)

var pathLocks sync.Map

func canonicalFilePath(path string) string {
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	return filepath.Clean(path)
}

type lockedPath struct {
	path string
	// Used when a directory cannot hold a sidecar/temp file, e.g. an individual
	// bind mount. The inode lock is acquired before any modification.
	file         *os.File
	forceInPlace bool
}

func withPathWriteLock(path string, action func(*lockedPath) error) error {
	path = canonicalFilePath(path)
	value, _ := pathLocks.LoadOrStore(path, new(sync.Mutex))
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	transaction := &lockedPath{path: path}
	if err != nil {
		if _, statErr := os.Stat(path + ".lock"); statErr == nil {
			return fmt.Errorf("open existing persistence lock: %w", err)
		}
		// File-mounted deployments can have a writable file in a read-only
		// parent. Retain their supported in-place writes under the inode lock.
		transaction.forceInPlace = true
	} else {
		defer lock.Close()
		if err := lockFile(lock); err != nil {
			return fmt.Errorf("lock sidecar: %w", err)
		}
		defer unlockFile(lock)
	}
	// Atomic writers also honor the target inode lock, so an in-place file-
	// mount writer and a sidecar writer never operate in separate lock domains.
	for attempt := 0; attempt < 32; attempt++ {
		file, openErr := os.OpenFile(path, os.O_RDWR, 0)
		if os.IsNotExist(openErr) && !transaction.forceInPlace {
			return action(transaction)
		}
		if openErr != nil {
			return fmt.Errorf("open destination: %w", openErr)
		}
		if err := lockFile(file); err != nil {
			file.Close()
			return fmt.Errorf("lock destination: %w", err)
		}
		held, heldErr := file.Stat()
		current, currentErr := os.Stat(path)
		if heldErr == nil && currentErr == nil && os.SameFile(held, current) {
			transaction.file = file
			defer file.Close()
			defer unlockFile(file)
			return action(transaction)
		}
		unlockFile(file)
		file.Close()
		if currentErr != nil && !os.IsNotExist(currentErr) {
			return currentErr
		}
	}
	return fmt.Errorf("destination changed repeatedly while acquiring lock")
}

func readFileWithLock(path string) ([]byte, error) {
	path = canonicalFilePath(path)
	value, _ := pathLocks.LoadOrStore(path, new(sync.Mutex))
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if err := lockFile(file); err != nil {
		return nil, err
	}
	defer unlockFile(file)
	return io.ReadAll(file)
}

func updateConfigFile(path string, update func(*Config)) error {
	return withPathWriteLock(path, func(transaction *lockedPath) error {
		data, err := transaction.read()
		if err != nil {
			return fmt.Errorf("read config: %w", err)
		}
		var saved Config
		if err := yaml.Unmarshal(data, &saved); err != nil {
			return fmt.Errorf("decode config: %w", err)
		}
		update(&saved)
		data, err = yaml.Marshal(&saved)
		if err != nil {
			return fmt.Errorf("encode config: %w", err)
		}
		return transaction.write(data, 0644)
	})
}

func (transaction *lockedPath) read() ([]byte, error) {
	if transaction.file != nil {
		return io.ReadAll(io.NewSectionReader(transaction.file, 0, 1<<63-1))
	}
	return os.ReadFile(transaction.path)
}

func writeFileWithLock(path string, data []byte, perm os.FileMode) error {
	return withPathWriteLock(path, func(transaction *lockedPath) error { return transaction.write(data, perm) })
}

func (transaction *lockedPath) write(data []byte, perm os.FileMode) error {
	if info, err := os.Stat(transaction.path); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("destination is not a regular file")
		}
		if info.Mode().Perm()&0222 == 0 {
			return fmt.Errorf("destination is read-only")
		}
		perm = info.Mode().Perm()
	} else if !os.IsNotExist(err) {
		return err
	}
	if transaction.forceInPlace {
		return transaction.writeInPlace(data, perm)
	}
	temporary, err := os.CreateTemp(filepath.Dir(transaction.path), ".easy-proxies-write-*")
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			return transaction.writeInPlace(data, perm)
		}
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	defer temporary.Close()
	if err := temporary.Chmod(perm); err != nil {
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, transaction.path); err != nil {
		// Replacing a file bind mount returns EBUSY; a writable file in a
		// non-writable directory may return EACCES/EPERM. Never pre-truncate.
		if errors.Is(err, syscall.EBUSY) || errors.Is(err, os.ErrPermission) {
			return transaction.writeInPlace(data, perm)
		}
		return err
	}
	// Atomic replacement is already complete; fsync the parent where supported.
	if directory, err := os.Open(filepath.Dir(transaction.path)); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

func (transaction *lockedPath) writeInPlace(data []byte, perm os.FileMode) error {
	file := transaction.file
	if file == nil {
		var err error
		file, err = os.OpenFile(transaction.path, os.O_CREATE|os.O_RDWR, perm)
		if err != nil {
			return err
		}
		defer file.Close()
		if err := lockFile(file); err != nil {
			return err
		}
		defer unlockFile(file)
	}
	backup, err := io.ReadAll(io.NewSectionReader(file, 0, 1<<63-1))
	if err != nil {
		return err
	}
	writeErr := func() error {
		if _, err := file.WriteAt(data, 0); err != nil {
			return err
		}
		if err := file.Truncate(int64(len(data))); err != nil {
			return err
		}
		return file.Sync()
	}()
	if writeErr != nil {
		// Best-effort recovery on a failed bind-mounted write.
		_, _ = file.WriteAt(backup, 0)
		_ = file.Truncate(int64(len(backup)))
		_ = file.Sync()
	}
	return writeErr
}
