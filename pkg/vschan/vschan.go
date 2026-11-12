// Package vschan — VSети: каналы и супергруппы поверх обычных групп Teamgram.
//
// Внутри сервера канал хранится как обычная группа (PEER_CHAT, таблица chats,
// available_reactions_type >= 100). На границе с клиентом:
//   - в ответах и пушах peerChat(id канала) → peerChannel, messageActionChatCreate →
//     messageActionChannelCreate, посты в канале (broadcast) → post=true без from_id;
//   - в запросах inputPeerChannel → inputPeerChat, peerChannel → peerChat.
package vschan

import (
	"context"
	"sync"
	"time"

	"github.com/teamgram/proto/mtproto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Kind: 0 — обычная группа, mtproto.VsKindMegagroup, mtproto.VsKindBroadcast.
type KindFunc func(ctx context.Context, chatId int64) int32

type cacheItem struct {
	kind int32
	at   time.Time
}

// Resolver кэширует вид чата (вид канала не меняется никогда).
type Resolver struct {
	mu    sync.RWMutex
	m     map[int64]cacheItem
	fetch KindFunc
}

func NewResolver(fetch KindFunc) *Resolver {
	return &Resolver{m: make(map[int64]cacheItem), fetch: fetch}
}

func (r *Resolver) Kind(ctx context.Context, chatId int64) int32 {
	if r == nil || chatId == 0 {
		return 0
	}
	r.mu.RLock()
	it, ok := r.m[chatId]
	r.mu.RUnlock()
	if ok && (it.kind > 0 || time.Since(it.at) < 2*time.Minute) {
		return it.kind
	}
	k := r.fetch(ctx, chatId)
	r.mu.Lock()
	r.m[chatId] = cacheItem{kind: k, at: time.Now()}
	r.mu.Unlock()
	return k
}

// Remember — сразу после создания канала, чтобы не ждать кэш.
func (r *Resolver) Remember(chatId int64, kind int32) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.m[chatId] = cacheItem{kind: kind, at: time.Now()}
	r.mu.Unlock()
}

var (
	peerDesc      = (&mtproto.Peer{}).ProtoReflect().Descriptor()
	inputPeerDesc = (&mtproto.InputPeer{}).ProtoReflect().Descriptor()
	messageDesc   = (&mtproto.Message{}).ProtoReflect().Descriptor()
	actionDesc    = (&mtproto.MessageAction{}).ProtoReflect().Descriptor()
	dialogDesc    = (&mtproto.Dialog{}).ProtoReflect().Descriptor()
	chatDesc      = (&mtproto.Chat{}).ProtoReflect().Descriptor()
	chatFullDesc  = (&mtproto.ChatFull{}).ProtoReflect().Descriptor()
)

// RewriteOut — ответ/пуш клиенту.
func (r *Resolver) RewriteOut(ctx context.Context, v any) {
	if r == nil || v == nil {
		return
	}
	m, ok := v.(proto.Message)
	if !ok || m == nil {
		return
	}
	r.walkOut(ctx, m.ProtoReflect(), 0)
}

func (r *Resolver) walkOut(ctx context.Context, m protoreflect.Message, depth int) {
	if depth > 64 || !m.IsValid() {
		return
	}
	switch m.Descriptor().FullName() {
	case peerDesc.FullName():
		if p, ok := m.Interface().(*mtproto.Peer); ok {
			r.outPeer(ctx, p)
		}
		return
	case messageDesc.FullName():
		if msg, ok := m.Interface().(*mtproto.Message); ok {
			r.outMessage(ctx, msg)
		}
	case chatDesc.FullName():
		// @username канала (его не знают msg/biz, которые собирают объект чата)
		if ch, ok := m.Interface().(*mtproto.Chat); ok && ch.GetPredicateName() == mtproto.Predicate_channel &&
			ch.Username == nil && mtproto.VsChannelUsernameFunc != nil {
			if u := mtproto.VsChannelUsernameFunc(ch.Id); u != "" {
				ch.Username = mtproto.MakeFlagsString(u)
			}
		}
	}
	m.Range(func(fd protoreflect.FieldDescriptor, val protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
			return true
		}
		switch {
		case fd.IsList():
			l := val.List()
			for i := 0; i < l.Len(); i++ {
				r.walkOut(ctx, l.Get(i).Message(), depth+1)
			}
		case fd.IsMap():
			val.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				if fd.MapValue().Kind() == protoreflect.MessageKind {
					r.walkOut(ctx, mv.Message(), depth+1)
				}
				return true
			})
		default:
			r.walkOut(ctx, val.Message(), depth+1)
		}
		return true
	})	// реакции разрешены во всех группах и каналах (свой набор VSети)
	if m.Descriptor().FullName() == chatFullDesc.FullName() {
		if cf, ok := m.Interface().(*mtproto.ChatFull); ok && cf.AvailableReactions_FLAGCHATREACTIONS == nil {
			cf.AvailableReactions_FLAGCHATREACTIONS = mtproto.MakeTLChatReactionsAll(&mtproto.ChatReactions{}).To_ChatReactions()
		}
	}
	// диалог канала: клиенту нужен pts канала
	if m.Descriptor().FullName() == dialogDesc.FullName() {
		if d, ok := m.Interface().(*mtproto.Dialog); ok && d.GetPeer().GetChannelId() != 0 && d.Pts == nil {
			d.Pts = wrapperspb.Int32(1)
		}
	}
}

func (r *Resolver) outPeer(ctx context.Context, p *mtproto.Peer) {
	if p == nil || p.ChatId == 0 {
		return
	}
	if p.PredicateName != mtproto.Predicate_peerChat && !(p.PredicateName == "" && p.Constructor == mtproto.CRC32_peerChat) {
		return
	}
	if r.Kind(ctx, p.ChatId) == 0 {
		return
	}
	p.PredicateName = mtproto.Predicate_peerChannel
	p.Constructor = mtproto.CRC32_peerChannel
	p.ChannelId = p.ChatId
	p.ChatId = 0
}

func (r *Resolver) outMessage(ctx context.Context, msg *mtproto.Message) {
	pid := msg.GetPeerId()
	if pid == nil || pid.GetChatId() == 0 {
		return
	}
	kind := r.Kind(ctx, pid.GetChatId())
	if kind == 0 {
		return
	}
	if a := msg.GetAction(); a != nil && a.GetPredicateName() == mtproto.Predicate_messageActionChatCreate {
		title := a.GetTitle_STRING()
		if title == "" {
			title = a.GetTitle_FLAGSTRING().GetValue()
		}
		msg.Action = mtproto.MakeTLMessageActionChannelCreate(&mtproto.MessageAction{
			Title_STRING: title,
		}).To_MessageAction()
	}
	if kind == mtproto.VsKindBroadcast {
		// пост канала: автор — сам канал
		msg.Post = true
		msg.FromId = nil
		// глазик просмотров: клиент сам спросит точное число (messages.getMessagesViews)
		if msg.Views == nil && msg.GetPredicateName() == mtproto.Predicate_message {
			msg.Views = wrapperspb.Int32(1)
		}
		// views и forwards в схеме под одним флагом — без forwards Windows-клиент не разбирает ответ
		if msg.Views != nil && msg.Forwards == nil {
			msg.Forwards = wrapperspb.Int32(0)
		}
	}
}

// RewriteIn — запрос от клиента: inputPeerChannel/peerChannel → чат.
func RewriteIn(v any) {
	m, ok := v.(proto.Message)
	if !ok || m == nil {
		return
	}
	walkIn(m.ProtoReflect(), 0)
}

func walkIn(m protoreflect.Message, depth int) {
	if depth > 32 || !m.IsValid() {
		return
	}
	switch m.Descriptor().FullName() {
	case inputPeerDesc.FullName():
		if p, ok := m.Interface().(*mtproto.InputPeer); ok && p.GetPredicateName() == mtproto.Predicate_inputPeerChannel && p.ChannelId != 0 {
			p.PredicateName = mtproto.Predicate_inputPeerChat
			p.Constructor = mtproto.CRC32_inputPeerChat
			p.ChatId = p.ChannelId
			p.ChannelId = 0
			p.AccessHash = 0
		}
	case peerDesc.FullName():
		if p, ok := m.Interface().(*mtproto.Peer); ok && p.GetPredicateName() == mtproto.Predicate_peerChannel && p.ChannelId != 0 {
			p.PredicateName = mtproto.Predicate_peerChat
			p.Constructor = mtproto.CRC32_peerChat
			p.ChatId = p.ChannelId
			p.ChannelId = 0
		}
		return
	}
	m.Range(func(fd protoreflect.FieldDescriptor, val protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind {
			return true
		}
		if fd.IsList() {
			l := val.List()
			for i := 0; i < l.Len(); i++ {
				walkIn(l.Get(i).Message(), depth+1)
			}
		} else if !fd.IsMap() {
			walkIn(val.Message(), depth+1)
		}
		return true
	})
}

// Default — общий резолвер процесса (bff, sync).
var Default *Resolver
