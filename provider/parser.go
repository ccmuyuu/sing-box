package provider

import (
	"strings"

	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

func newParser(content string) ([]option.Outbound, error) {
	if strings.Contains(content, "\"outbounds\"") {
		var options option.Options
		err := options.UnmarshalJSON([]byte(content))
		if err != nil {
			return nil, E.Cause(err, "decode config at ")
		}
		return options.Outbounds, nil
	} else if strings.Contains(content, "proxies") {
		return newClashParser(content)
	}
	return newNativeURIParser(content)
}
