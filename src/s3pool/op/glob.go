/*
 *  S3pool - S3 cache on local disk
 *  Copyright (c) 2019 CK Tan
 *  cktanx@gmail.com
 *
 *  S3Pool can be used for free under the GNU General Public License
 *  version 3, where anything released into public must be open source,
 *  or under a commercial license. The commercial license does not
 *  cover derived or ported versions created by third parties under
 *  GPL. To inquire about commercial license, please send email to
 *  cktan@gmail.com.
 */
package op

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"github.com/gobwas/glob"
	"bufio"
)



func Glob(args []string) (reply string, err error) {
	if len(args) != 2 {
		err = errors.New("expects 2 arguments for GLOB")
		return
	}
	bucket, pattern := args[0], args[1]

	var sep []rune
	sep = append(sep, '/')
	g, err := glob.Compile(pattern, sep...)
	if err != nil {
		return
	}
	fmt.Println("pattern", pattern)
	
	file, err := os.Open(fmt.Sprintf("data/%s/__list__", bucket))
	if err != nil {
		return
	}
	defer file.Close()

	var replyBuilder strings.Builder
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		matched := g.Match(line)
		fmt.Println(line, matched)
		if matched {
			replyBuilder.WriteString(line)
			replyBuilder.WriteString("\n")
		}
	}

	if err = scanner.Err(); err != nil {
		return
	}

	reply = replyBuilder.String()
	return
}
