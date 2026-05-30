package byteconv

import "math"

func SplitFields(line []byte, out [][]byte) int {
	n, start := 0, 0
	for i, b := range line {
		if b == ',' && n < len(out)-1 {
			out[n] = line[start:i]
			n++
			start = i + 1
		}
	}
	if n < len(out) {
		end := len(line)
		if end > 0 && line[end-1] == '\r' {
			end--
		}
		out[n] = line[start:end]
		n++
	}
	return n
}

func ParseTimestampUnix(s []byte) (int64, bool) {
	if len(s) < 16 || s[4] != '/' || s[7] != '/' || s[10] != ' ' || s[13] != ':' {
		return 0, false
	}
	y := int64(s[0]-'0')*1000 + int64(s[1]-'0')*100 + int64(s[2]-'0')*10 + int64(s[3]-'0')
	mo := int64(s[5]-'0')*10 + int64(s[6]-'0')
	d := int64(s[8]-'0')*10 + int64(s[9]-'0')
	h := int64(s[11]-'0')*10 + int64(s[12]-'0')
	mi := int64(s[14]-'0')*10 + int64(s[15]-'0')
	return unixSeconds(y, mo, d, h, mi), true
}

func unixSeconds(year, month, day, hour, minute int64) int64 {
	y, m := year, month
	if m <= 2 {
		y--
		m += 9
	} else {
		m -= 3
	}
	era := y / 400
	yoe := y - era*400
	doy := (153*m+2)/5 + day - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return (era*146097+doe-719468)*86400 + hour*3600 + minute*60
}

func AppendUint8(dst []byte, v uint8) []byte {
	return append(dst, v)
}

func AppendUint32(dst []byte, v uint32) []byte {
	return append(dst, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func AppendUint16(dst []byte, v uint16) []byte {
	return append(dst, byte(v>>8), byte(v))
}

func AppendUint64(dst []byte, v uint64) []byte {
	return append(dst,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v),
	)
}

func AppendInt64(dst []byte, v int64) []byte     { return AppendUint64(dst, uint64(v)) }
func AppendFloat64(dst []byte, v float64) []byte { return AppendUint64(dst, math.Float64bits(v)) }

func AppendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, 1)
	}
	return append(dst, 0)
}

func AppendString(dst []byte, s string) []byte {
	dst = AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}
