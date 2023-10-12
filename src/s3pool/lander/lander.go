package lander

import (
	//	"errors"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"s3pool/conf"
)

type Csvspec struct {
	Delim       string
	Quote       string
	Escape      string
	Nullstr     string
	Header_line bool
}
type Filespec struct {
	Fmt     string
	Csvspec Csvspec
}

type ColumnDesc struct {
    Name string
    Type string
    Precision int
    Scale int
}

var g_devices []string
var g_rows_per_group int

func Init(devices []string, rows_per_group int) {
	g_devices = devices
	g_rows_per_group = rows_per_group
}

func Xrgdiv(bucket string, key string, schemafn string, filespecjs string) (string, error) {
	var fspec Filespec
	var args []string
	csvp := mapToCsvRelativePath(bucket, key)
	xrgp := mapToXrgRelativePath(bucket, key)
	xrgdir := filepath.Dir(xrgp)
	json.Unmarshal([]byte(filespecjs), &fspec)

	if fspec.Fmt == "csv" {

		args = []string{"-i", "csv", "-d", fspec.Csvspec.Delim, "-q", fspec.Csvspec.Quote, "-x", fspec.Csvspec.Escape, "-N", fspec.Csvspec.Nullstr, "-s", schemafn}
		if fspec.Csvspec.Header_line {
			args = append(args, "-H")
		}
	} else if fspec.Fmt == "parquet" {
		args = []string{"-l", "-i", "parquet", "-s", schemafn}
	} else {
		return "", fmt.Errorf("file type %s not supported", fspec.Fmt)
	}

	if g_rows_per_group > 0 {
		args = append(args, "-n", strconv.Itoa(g_rows_per_group))
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

func Stem(base string) string {

	var stem string
	isgzip := strings.HasSuffix(base, ".gz")
	if isgzip {
		stem = base[:len(base)-3]
	} else {
		stem = base
	}
	idx := strings.LastIndex(stem, ".")
	if idx <= 0 {
		return stem
	}

	return stem[:idx]
}

// return absolute path of the zonemap file
func FindZMPFile(bucket string, key string) (zmppath string, err error) {
	path := mapToXrgRelativePath(bucket, key)
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	stem := Stem(base)

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
	if conf.DfsMode == conf.DFS_LOCAL {
		path = fmt.Sprintf("/%s/%s", bucket, key)
	} else {
		path = fmt.Sprintf("data/%s/%s", bucket, key)
	} 
	return
}

func mapToXrgRelativePath(bucket, key string) (path string) {
	path = filepath.Join(bucket, key)
	return
}

func CheckSchema(schema io.Reader, zmpfile string, filespecjs string) (bool, error) {
	var fspec Filespec
	dir := filepath.Dir(zmpfile)
	base := filepath.Base(zmpfile)
	fname := Stem(base) + ".schema"
	p := filepath.Join(dir, fname)
	json.Unmarshal([]byte(filespecjs), &fspec)

	// compare two schema file
	xrgschema, err := os.Open(p)
	if err != nil {
		return false, fmt.Errorf("source xrg schema file cannot be open")
	}
	defer xrgschema.Close()

	equals := false
	if (fspec.Fmt == "parquet") {
		// TODO: loosely check the decimal and skip for now
		equals, err = checkSchemaLoosely(schema, xrgschema)
	} else {
		equals, err = jsonEquals(schema, xrgschema)
	}
	if err != nil {
		return false, err
	}

	return equals, nil
}

func jsonEquals(a, b io.Reader) (bool, error) {
	var j, j2 interface{}
	d := json.NewDecoder(a)
	if err := d.Decode(&j); err != nil {
		return false, err
	}
	d = json.NewDecoder(b)
	if err := d.Decode(&j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j2, j), nil
}


func parseSchema(r io.Reader) ([]ColumnDesc, error) {
    s0 := []ColumnDesc{}

    d0 := json.NewDecoder(r)
    t0, err := d0.Token()
    if err != nil || t0 != json.Delim('[') {
        return nil, fmt.Errorf("not open bracket [")
    }
    for d0.More() {
        var c ColumnDesc
        err := d0.Decode(&c)
        if err != nil {
            return nil, fmt.Errorf("column decode failed")
        }

        s0 = append(s0, c)
    }
    t0, err = d0.Token()
    if err != nil || t0 != json.Delim(']') {
        return nil, fmt.Errorf("not open bracket ]")
    }
    return s0, nil
}

func checkSchemaLoosely(a, b io.Reader) (bool, error) {
    s0, err := parseSchema(a)
    if err != nil {
        return false, fmt.Errorf("input schema not in JSON")
    }

    s1, err := parseSchema(b)
    if err != nil {
        return false, fmt.Errorf("xrg schema not in JSON")
    }

    if len(s0) != len(s1) {
        return false, fmt.Errorf("schems size not match")
    }

    for i := 0 ; i < len(s0) ; i++ {
        cd0 := s0[i]
        cd1 := s1[i]

        if (cd0.Name != cd1.Name) {
            return false, fmt.Errorf("column name not match %s %s", cd0.Name, cd1.Name)
        }
        if (cd0.Type != cd1.Type) {
            return false, fmt.Errorf("column name not match %s %s", cd0.Type, cd1.Type)
        }
    }

    return true, nil
}


