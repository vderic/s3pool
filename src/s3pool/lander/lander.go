package lander

import (
//	"errors"
	"fmt"
	"os"
//	"os/exec"
)

var g_devices []string
var g_csv_delimiter = ","
var g_csv_quote = "\""
var g_csv_nullstr = ""

func Init(devices []string) {
	g_devices = devices
}

func InitCsvSpec(delimiter string, quote string, nullstr string) {
	g_csv_delimiter = delimiter
	g_csv_quote = quote
	g_csv_nullstr = nullstr
}

func Csv2Xrg(path string, schemafn string) (string, error) {

	return "", nil
}

func RemoveXrgFile(path string) (err error) {

	zmppath, err := FindZMPFile(path)
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

func FindZMPFile(path string) (zmppath string, err error) {

	err = fmt.Errorf("ZMP File not found")
	zmppath = ""
	return
}


func fileReadable(path string) bool {
	f, err := os.Open(path)
	if err == nil {
		f.Close()
	}
	return err == nil
}

