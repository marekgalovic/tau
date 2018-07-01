package utils

import (
    "fmt";
    "regexp";
    "strconv";
)

var SeqIdRegexp = regexp.MustCompile(`\-n(\d+)`)

func ParseSeqId(znode string) (int64, error) {
    seqIdMatch := SeqIdRegexp.FindStringSubmatch(znode)
    if len(seqIdMatch) != 2 {
        return 0, fmt.Errorf("Invalid znode `%s`", znode)
    }

    return strconv.ParseInt(seqIdMatch[1], 10, 64)
}
