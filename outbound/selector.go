package outbound

import (
	"context"
	"net"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

var (
	_ adapter.Outbound      = (*Selector)(nil)
	_ adapter.OutboundGroup = (*Selector)(nil)
)

type Selector struct {
	myOutboundAdapter
	myGroupAdapter
	defaultTag                   string
	outbounds                    []adapter.Outbound
	outboundByTag                map[string]adapter.Outbound
	selected                     adapter.Outbound
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
}

func NewSelector(router adapter.Router, logger log.ContextLogger, tag string, options option.SelectorOutboundOptions) (*Selector, error) {
	outbound := &Selector{
		myOutboundAdapter: myOutboundAdapter{
			protocol:     C.TypeSelector,
			router:       router,
			logger:       logger,
			tag:          tag,
			dependencies: options.Outbounds,
		},
		myGroupAdapter: myGroupAdapter{
			tags:      options.Outbounds,
			uses:      options.Providers,
			includes:  options.Includes,
			excludes:  options.Excludes,
			types:     options.Types,
			ports:     make(map[int]bool),
			providers: make(map[string]adapter.OutboundProvider),
		},
		defaultTag:                   options.Default,
		outbounds:                    []adapter.Outbound{},
		outboundByTag:                make(map[string]adapter.Outbound),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: options.InterruptExistConnections,
	}
	if len(outbound.tags) == 0 && len(outbound.uses) == 0 {
		return nil, E.New("missing tags and uses")
	}
	portMap, err := CreatePortsMap(options.Ports)
	if err != nil {
		return nil, err
	}
	outbound.ports = portMap
	return outbound, nil
}

func (s *Selector) Network() []string {
	if s.selected == nil {
		return []string{N.NetworkTCP, N.NetworkUDP}
	}
	return s.selected.Network()
}

func (s *Selector) Start() error {
	for i, tag := range s.tags {
		detour, loaded := s.router.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		s.outbounds = append(s.outbounds, detour)
		s.outboundByTag[tag] = detour
	}

	for i, tag := range s.uses {
		provider, loaded := s.router.OutboundProvider(tag)
		if !loaded {
			return E.New("outbound provider ", i, " not found: ", tag)
		}
		if _, ok := s.providers[tag]; !ok {
			s.providers[tag] = provider
		}
		for _, outbound := range provider.Outbounds() {
			if s.OutboundFilter(outbound) {
				tag := outbound.Tag()
				s.outbounds = append(s.outbounds, outbound)
				s.outboundByTag[tag] = outbound
			}
		}
	}

	if len(s.outbounds) == 0 {
		OUTBOUNDLESS, _ := s.router.Outbound("OUTBOUNDLESS")
		s.outbounds = append(s.outbounds, OUTBOUNDLESS)
		s.outboundByTag["OUTBOUNDLESS"] = OUTBOUNDLESS
		s.selected = OUTBOUNDLESS
		return nil
	}

	if s.tag != "" {
		if clashServer := s.router.ClashServer(); clashServer != nil && clashServer.StoreSelected() {
			selected := clashServer.CacheFile().LoadSelected(s.tag)
			if selected != "" {
				detour, loaded := s.outboundByTag[selected]
				if loaded {
					s.selected = detour
					return nil
				}
			}
		}
	}

	if s.defaultTag != "" {
		detour, loaded := s.outboundByTag[s.defaultTag]
		if !loaded {
			return E.New("default outbound not found: ", s.defaultTag)
		}
		s.selected = detour
		return nil
	}

	s.selected = s.outbounds[0]
	return nil
}

func (s *Selector) UpdateOutbounds(tag string) error {
	if _, ok := s.providers[tag]; ok {
		backupOutbounds := s.outbounds
		backupOutboundByTag := s.outboundByTag
		s.outbounds = []adapter.Outbound{}
		s.outboundByTag = make(map[string]adapter.Outbound, 0)
		err := s.Start()
		if err != nil {
			s.outbounds = backupOutbounds
			s.outboundByTag = backupOutboundByTag
			return E.New("update oubounds failed: ", s.tag)
		}
	}
	return nil
}

func (s *Selector) Now() string {
	return s.selected.Tag()
}

func (s *Selector) All() []string {
	all := []string{}
	for _, outbound := range s.outbounds {
		all = append(all, outbound.Tag())
	}
	return all
}

func (s *Selector) SelectOutbound(tag string) bool {
	detour, loaded := s.outboundByTag[tag]
	if !loaded {
		return false
	}
	if s.selected == detour {
		return true
	}
	s.selected = detour
	if s.tag != "" {
		if clashServer := s.router.ClashServer(); clashServer != nil && clashServer.StoreSelected() {
			err := clashServer.CacheFile().StoreSelected(s.tag, tag)
			if err != nil {
				s.logger.Error("store selected: ", err)
			}
		}
	}
	s.interruptGroup.Interrupt(s.interruptExternalConnections)
	return true
}

func (s *Selector) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	conn, err := s.selected.DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	return s.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func (s *Selector) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	conn, err := s.selected.ListenPacket(ctx, destination)
	if err != nil {
		return nil, err
	}
	return s.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func (s *Selector) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext) error {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	return s.selected.NewConnection(ctx, conn, metadata)
}

func (s *Selector) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext) error {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	return s.selected.NewPacketConnection(ctx, conn, metadata)
}

func RealTag(detour adapter.Outbound) string {
	if group, isGroup := detour.(adapter.OutboundGroup); isGroup {
		return group.Now()
	}
	return detour.Tag()
}
