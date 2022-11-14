package lander

import (
//	"errors"
	"fmt"
	"os"
	"os/exec"
	"bytes"
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

func Csv2Xrg(bucket string, path string, schemafn string) (string, error) {
	args := []string{"-i", "csv", "-d", g_csv_delimiter, "-q", g_csv_quote, "-x", g_csv_esc, "-N", g_csv_nullstr, "-s", schemafn}
	for _, dev := range g_devices {
		args = append(args, "-D", dev)
	}

	if g_csv_ignore_header {
		args = append(args, "-H")
	}

	csvp := mapToPath(bucket, path)

	args = append(args, csvp)

	cmd := exec.Command("xrgdiv", args...)

	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf

	if err := cmd.Run() ; err != nil {
		errstr := string(errbuf.Bytes())
		return "", fmt.Errorf("xrgdiv failed -- %s", errstr)
	}

	zmppath, err := FindZMPFile(bucket, path)
	if err != nil {
		return "", err
	}

	return zmppath, nil
}

func RemoveXrgFile(bucket string, path string) (err error) {

	zmppath, err := FindZMPFile(bucket, path)
	if err != nil {
		// not found
		return
	}

	lstpath := zmppath[:len(zmppath)-4] + ".lst"

	if !fileReadable(lstpath) {
		// read all xrg files and delete it

	}
	return
}

func FindZMPFile(bucket string, path string) (zmppath string, err error) {
	//localpath := mapToPath(bucket, path)
	return "", nil
}


func fileReadable(path string) bool {
	f, err := os.Open(path)
	if err == nil {
		f.Close()
	}
	return err == nil
}

func mapToPath(bucket, key string) (path string) {
	path = fmt.Sprintf("data/%s/%s", bucket, key)
	return
}
