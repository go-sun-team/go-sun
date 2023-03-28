package msstrings

import (
	"fmt"
	"reflect"
	"strings"
)

func JoinStrings(data ...any) string {
	var sb strings.Builder
	for _, v := range data {
		sb.WriteString(check(v))
	}
	return sb.String()
}

func check(v any) string {
	value := reflect.ValueOf(v)
	switch value.Kind() {
	case reflect.String:
		return v.(string)
	default:
		return fmt.Sprintf("%v", v)
	}
}
