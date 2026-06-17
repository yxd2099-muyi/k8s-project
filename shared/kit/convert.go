package kit

import "strconv"

func StringToUint64(s string) (uint64, error) {
	num, err := strconv.ParseUint(s, 10, 64)
	return num, err
}
func StringToInt64(s string) (int64, error) {
	num, err := strconv.ParseInt(s, 10, 64)
	return num, err
}
func Uint64ToString(num uint64) string {
	str := strconv.FormatUint(num, 10)
	return str
}
