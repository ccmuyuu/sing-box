package provider

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

func New(ctx context.Context, router adapter.Router, logger log.ContextLogger, options option.OutboundProvider) (adapter.OutboundProvider, error) {
	if options.Path == "" {
		return nil, E.New("provider path missing")
	}
	if options.HealthcheckUrl == "" {
		options.HealthcheckUrl = "https://www.gstatic.com/generate_204"
	}
	switch options.Type {
	case C.TypeFileProvider:
		return NewFileProvider(ctx, router, logger, options)
	case C.TypeHTTPProvider:
		return NewHTTPProvider(ctx, router, logger, options)
	default:
		return nil, E.New("invalid provider type")
	}
}
