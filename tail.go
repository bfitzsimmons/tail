// Copyright (c) 2013 ActiveState Software Inc. All rights reserved.

package tail

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hpcloud/tail/watch"
	"gopkg.in/tomb.v1"
)

// Errors
var (
	ErrStop                         = errors.New("The tail should now stop")
	ErrInvalidConfig                = errors.New("Invalid Config options")
	ErrSeek                         = errors.New("Seek error")
	ErrFileCreationDetectionFailure = errors.New("Failed to detect creation of file")
	ErrFileOpen                     = errors.New("Unable to open file")
)

// Line contains the line read from the file, along with a timestamp and an error (if necessary).
type Line struct {
	Text string
	Time time.Time
	Err  error
}

// NewLine returns a *Line with the current time.
func NewLine(text string) *Line {
	return &Line{text, time.Now(), nil}
}

// SeekInfo represents arguments to `os.Seek`
type SeekInfo struct {
	Offset int64
	Whence int // os.SEEK_*
}

// Config is used to specify how a file must be tailed.
type Config struct {
	// File-specifc
	Location  *SeekInfo // Seek to this location before tailing
	ReOpen    bool      // Reopen recreated files (tail -F)
	MustExist bool      // Fail early if the file does not exist
	Poll      bool      // Poll for file changes instead of using inotify
	Pipe      bool      // Is a named pipe (mkfifo)

	// Generic I/O.
	Follow bool // Continue looking for new lines (tail -f)
}

// Tail contains info. related to the item being tailed.
type Tail struct {
	Filename string
	Lines    chan *Line
	Config
	file      *os.File
	reader    *bufio.Reader
	watcher   watch.FileWatcher
	changes   *watch.FileChanges
	tomb.Tomb // provides: Done, Kill, Dying
	lock      sync.Mutex
}

// File begins tailing the file. The output stream is made available via the `Tail.Lines` channel. To handle errors
// during tailing, invoke the `Wait` or `Err` method after you finish reading from the `Lines` channel.
func File(filename string, config Config) (*Tail, error) {
	var err error

	if config.ReOpen && !config.Follow {
		return nil, ErrInvalidConfig
	}

	t := &Tail{
		Filename: filename,
		Lines:    make(chan *Line),
		Config:   config,
	}

	if t.Poll {
		t.watcher = watch.NewPollingFileWatcher(filename)
	} else {
		t.watcher = watch.NewInotifyFileWatcher(filename)
	}

	if t.MustExist {
		fmt.Println(t.Filename)
		t.file, err = os.Open(t.Filename)
		if err != nil {
			return nil, err
		}
	}

	// Start tailing the file.
	go t.tailFileSync()

	return t, nil
}

// Tell returns the file's current position, like stdio's ftell(), but this value is not very accurate. It may read one
// line in the chan(tail.Lines), so it may lose one line.
func (tail *Tail) Tell() (offset int64, err error) {
	if tail.file == nil {
		return
	}

	offset, err = tail.file.Seek(0, os.SEEK_CUR)
	if err == nil {
		tail.lock.Lock()
		offset -= int64(tail.reader.Buffered())
		tail.lock.Unlock()
	}
	return
}

// Stop stops the tailing activity.
func (tail *Tail) Stop() error {
	tail.Kill(nil)
	return tail.Wait()
}

func (tail *Tail) close() {
	close(tail.Lines)
	tail.closeFile()
}

func (tail *Tail) closeFile() {
	if tail.file != nil {
		tail.file.Close()
		tail.file = nil
	}
}

func (tail *Tail) reopen() error {
	var err error

	tail.closeFile()

	for {
		tail.file, err = os.Open(tail.Filename)
		if err != nil {
			if os.IsNotExist(err) {
				if err := tail.watcher.BlockUntilExists(&tail.Tomb); err != nil {
					if err == tomb.ErrDying {
						return err
					}
					return ErrFileCreationDetectionFailure
				}
				continue
			}
			return ErrFileOpen
		}
		break
	}

	return nil
}

func (tail *Tail) readLine() (string, error) {
	tail.lock.Lock()
	line, err := tail.reader.ReadString('\n')
	tail.lock.Unlock()
	if err != nil {
		// Note: ReadString "returns the data read before the error" in case of an error -- including `EOF` -- so we
		// return it as-is. The caller is expected to process it if `err` is `EOF`.
		return line, err
	}

	// Trim the newline character off of the line.
	return strings.TrimRight(line, "\n"), nil
}

func (tail *Tail) tailFileSync() {
	defer tail.Done()
	defer tail.close()

	if !tail.MustExist {
		// Deferred first open.
		if err := tail.reopen(); err != nil {
			if err != tomb.ErrDying {
				tail.Kill(err)
			}
			return
		}
	}

	// Seek to requested location on first open of the file.
	if tail.Location != nil {
		_, err := tail.file.Seek(tail.Location.Offset, tail.Location.Whence)
		if err != nil {
			tail.Killf("Seek error on %s: %s", tail.Filename, err.Error())
			return
		}
	}

	tail.openReader()

	var offset int64
	var err error

	// Read line by line.
	for {
		// Do not seek in named pipes.
		if !tail.Pipe {
			// Grab the position in case we need to back up in the event of a half-line.
			offset, err = tail.Tell()
			if err != nil {
				tail.Kill(err)
				return
			}
		}

		line, err := tail.readLine()

		// Process `line` even if `err` is `EOF`.
		if err == nil {
			tail.sendLine(line)
		} else if err != nil {
			if err == io.EOF {
				if !tail.Follow {
					if line != "" {
						tail.sendLine(line)
					}
					return
				}

				if tail.Follow && line != "" {
					fmt.Println(line)
					// This has the potential to never return the last line if it's not followed by a newline; seems
					// like a fair trade.
					if err := tail.seekTo(SeekInfo{Offset: offset, Whence: 0}); err != nil {
						tail.Kill(err)
						return
					}
				}

				// When EOF is reached, wait for more data to become available. The wait strategy is based on the
				// `tail.watcher` implementation (inotify or polling).
				if err := tail.waitForChanges(); err != nil {
					if err != ErrStop {
						tail.Kill(err)
					}
					return
				}
			} else {
				// Non-EOF error.
				tail.Killf("Error reading %s: %s", tail.Filename, err)
				return
			}
		}

		select {
		case <-tail.Dying():
			return
		default:
		}
	}
}

// waitForChanges waits until the file has been appended to, deleted, moved or truncated. When moved or deleted - the
// file will be reopened if ReOpen is true. Truncated files are *always* reopened.
func (tail *Tail) waitForChanges() error {
	if tail.changes == nil {
		// Set our starting position in the file.
		pos, err := tail.file.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}

		// Set up our file changes watcher.
		tail.changes, err = tail.watcher.ChangeEvents(&tail.Tomb, pos)
		if err != nil {
			return err
		}
	}

	// Read the events out of the tail.changes channel.
	select {
	case <-tail.changes.Modified:
		return nil
	case <-tail.changes.Deleted:
		tail.changes = nil
		if tail.ReOpen {
			// ReOpen the file.
			if err := tail.reopen(); err != nil {
				return err
			}

			// Open a new reader on the file.
			tail.openReader()

			return nil
		}

		return ErrStop
	case <-tail.changes.Truncated:
		// Always ReOpen truncated files (Follow is true).
		if err := tail.reopen(); err != nil {
			return err
		}

		// Open a new reader on the file.
		tail.openReader()

		return nil
	case <-tail.Dying():
		return ErrStop
	}
}

func (tail *Tail) openReader() {
	tail.reader = bufio.NewReader(tail.file)
}

func (tail *Tail) seekEnd() error {
	return tail.seekTo(SeekInfo{Offset: 0, Whence: os.SEEK_END})
}

func (tail *Tail) seekTo(pos SeekInfo) error {
	_, err := tail.file.Seek(pos.Offset, pos.Whence)
	if err != nil {
		return ErrSeek
	}

	// Reset the read buffer whenever the file is re-seek'ed.
	tail.reader.Reset(tail.file)
	return nil
}

// sendLine sends the line to the Lines channel.
func (tail *Tail) sendLine(line string) {
	tail.Lines <- &Line{line, time.Now(), nil}
}

// Cleanup removes inotify watches added by the tail package. This function is meant to be invoked from a process's exit
// handler. The Linux kernel may not automatically remove inotify watches after the process exits.
func (tail *Tail) Cleanup() {
	watch.Cleanup(tail.Filename)
}
