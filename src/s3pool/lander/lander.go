package lander

import (
	//	"errors"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type filespec struct {
	ftype         string
	delimiter     string
	quote         string
	escape        string
	nullstr       string
	ignore_header bool
}

var g_devices []string

func Init(devices []string) {
	g_devices = devices
}

func Xrgdiv(bucket string, key string, schemafn string, filespecjs string) (string, error) {
	var fsa []string
	var fspec filespec
	var args []string
	csvp := mapToCsvRelativePath(bucket, key)
	xrgp := mapToXrgRelativePath(bucket, key)
	xrgdir := filepath.Dir(xrgp)
	json.Unmarshal([]byte(filespecjs), &fsa)

	if fsa[0] == "csv" {
		if len(fsa) != 6 {
			return "", fmt.Errorf("csv spec not enough parameters")
		}

		ignore_header, err := strconv.ParseBool(fsa[5])
		if err != nil {
			return "", fmt.Errorf("filespec is invalid. ignore_header is not bool")
		}

		fspec = filespec{fsa[0], fsa[1], fsa[2], fsa[3], fsa[4], ignore_header}
		args = []string{"-i", "csv", "-d", fspec.delimiter, "-q", fspec.quote, "-x", fspec.escape, "-N", fspec.nullstr, "-s", schemafn}
		if fspec.ignore_header {
			args = append(args, "-H")
		}
	} else if fsa[0] == "parquet" {
		fspec = filespec{ftype: fsa[0]}
		args = []string{"-i", "parquet"}
	} else {
		return "", fmt.Errorf("file type %s not supported", fsa[0])
	}

	for _, dev := range g_devices {
		dir := filepath.Join(dev, xrgdir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", err
		}
		args = append(args, "-D", dir)
	}

	args = append(args, csvp)

	cmd := exec.Command("xrgdiv", args...)

	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf

	if err := cmd.Run(); err != nil {
		errstr := string(errbuf.Bytes())
		return "", fmt.Errorf("xrgdiv failed -- %s", errstr)
	}

	zmppath, err := FindZMPFile(bucket, key)
	if err != nil {
		return "", err
	}

	return zmppath, nil
}

func RemoveXrgFile(zmppath string) (err error) {

	lstpath := zmppath[:len(zmppath)-4] + ".list"

	if fileReadable(lstpath) {
		// .list file is a JSON list of file name
		var flist []string
		jsonfile, err := os.Open(lstpath)
		if err != nil {
			return err
		}
		defer jsonfile.Close()
		bytes, _ := ioutil.ReadAll(jsonfile)

		json.Unmarshal(bytes, &flist)

		for i := 0; i < len(flist); i++ {
			err := os.Remove(flist[i])
			if err != nil {
				return err
			}
		}

		// remove .list file
		err = os.Remove(lstpath)
		if err != nil {
			return err
		}
	}

	schemafn := zmppath[:len(zmppath)-4] + ".schema"
	if fileReadable(schemafn) {
		err := os.Remove(schemafn)
		if err != nil {
			return err
		}
	}

	if fileReadable(zmppath) {
		err := os.Remove(zmppath)
		if err != nil {
			return err
		}
	}
	return nil
}

// return absolute path of the zonemap file
func FindZMPFile(bucket string, key string) (zmppath string, err error) {
	path := mapToXrgRelativePath(bucket, key)
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	idx := strings.LastIndex(base, ".csv")
	if idx == -1 {
		idx = strings.LastIndex(base, ".parquet")
	}
	if idx == -1 {
		return "", fmt.Errorf("key is not .csv or .parquet file")
	}

	stem := base[:idx]

	for _, dev := range g_devices {
		fname := stem + ".zmp"
		p := filepath.Join(dev, dir, fname)
		if fileReadable(p) {
			zmppath, err = filepath.Abs(p)
			return
		}
	}

	return "", fmt.Errorf("ZMP file not found")
}

func fileReadable(path string) bool {
	f, err := os.Open(path)
	if err == nil {
		f.Close()
	}
	return err == nil
}

func mapToCsvRelativePath(bucket, key string) (path string) {
	path = fmt.Sprintf("data/%s/%s", bucket, key)
	return
}

func mapToXrgRelativePath(bucket, key string) (path string) {
	path = filepath.Join(bucket, key)
	return
}
