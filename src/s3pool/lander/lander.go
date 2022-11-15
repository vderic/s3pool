package lander

import (
	//	"errors"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var g_devices []string
var g_csv_delimiter = ","
var g_csv_quote = "\""
var g_csv_nullstr = ""
var g_csv_esc = "\""
var g_csv_ignore_header = false

func Init(devices []string) {
	g_devices = devices
}

func InitCsvSpec(delimiter string, quote string, nullstr string, esc string, ignore_header bool) {
	g_csv_delimiter = delimiter
	g_csv_quote = quote
	g_csv_nullstr = nullstr
	g_csv_esc = esc
	g_csv_ignore_header = ignore_header
}

func Csv2Xrg(bucket string, key string, schemafn string) (string, error) {
	args := []string{"-i", "csv", "-d", g_csv_delimiter, "-q", g_csv_quote, "-x", g_csv_esc, "-N", g_csv_nullstr, "-s", schemafn}
	for _, dev := range g_devices {
		args = append(args, "-D", dev)
	}

	if g_csv_ignore_header {
		args = append(args, "-H")
	}

	csvp := mapToRelativePath(bucket, key)

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

	if !fileReadable(lstpath) {
		// .list file is a JSON list of file name

	}
	return
}

// return absolute path of the zonemap file
func FindZMPFile(bucket string, key string) (zmppath string, err error) {
	path := mapToRelativePath(bucket, key)
	idx := strings.LastIndex(path, ".csv")
	if idx == -1 {
		idx = strings.LastIndex(path, ".parquet")
	}
	if idx == -1 {
		return "", fmt.Errorf("key is not .csv or .parquet file")
	}

	var stem = ""
	var dir = ""
	slashidx := strings.LastIndex(path, "/")
	if slashidx == -1 {
		// simple filename without directory
		stem = path[:idx]
		dir = ""
	} else {
		stem = path[slashidx+1:idx]
		dir  = path[:slashidx]
	}

	for _, dev := range g_devices {
		fname := stem + ".zmp"
		p := filepath.Join(dev, dir, stem, fname)
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

func mapToRelativePath(bucket, key string) (path string) {
	path = fmt.Sprintf("data/%s/%s", bucket, key)
	return
}
