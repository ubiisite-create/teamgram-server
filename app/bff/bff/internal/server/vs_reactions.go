// VSети: реакции на сообщения (свой набор значков — vs_reactions_data.go).
// Хранение: redis-хэш vs_react:{dialog1}:{dialog2}:{dialog_message_id} поле=userId значение=эмодзи.
package server

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/teamgram/proto/mtproto"
	sync_client "github.com/teamgram/teamgram-server/app/messenger/sync/client"
	syncpb "github.com/teamgram/teamgram-server/app/messenger/sync/sync"
	chatpb "github.com/teamgram/teamgram-server/app/service/biz/chat/chat"
	messagepb "github.com/teamgram/teamgram-server/app/service/biz/message/message"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var vsSyncCli sync_client.SyncClient

type vsReactions struct {
	mtproto.UnimplementedRPCReactionsServer
}

var vsKindNames = [7]string{"static.webp", "appear.tgs", "select.tgs", "activate.tgs", "effect.tgs", "around.tgs", "center.tgs"}

func vsReactDocument(d vsReactDoc, alt string, kind int) *mtproto.Document {
	stype := mtproto.TLConstructor_CRC32_storage_fileUnknown
	if d.mime == "image/webp" {
		stype = mtproto.TLConstructor_CRC32_storage_fileWebp
	}
	attrs := []*mtproto.DocumentAttribute{
		mtproto.MakeTLDocumentAttributeImageSize(&mtproto.DocumentAttribute{W: d.wh, H: d.wh}).To_DocumentAttribute(),
		mtproto.MakeTLDocumentAttributeSticker(&mtproto.DocumentAttribute{
			Alt:        alt,
			Stickerset: mtproto.MakeTLInputStickerSetEmpty(nil).To_InputStickerSet(),
		}).To_DocumentAttribute(),
		mtproto.MakeTLDocumentAttributeFilename(&mtproto.DocumentAttribute{FileName: vsKindNames[kind]}).To_DocumentAttribute(),
	}
	return mtproto.MakeTLDocument(&mtproto.Document{
		Id:            d.id,
		AccessHash:    int64(stype)<<32 | (d.id & 0xffffffff),
		FileReference: []byte{},
		Date:          1794400000,
		MimeType:      d.mime,
		Size2_INT64:   d.size,
		Size2_INT32:   int32(d.size),
		Size2:         d.size,
		Thumbs:        []*mtproto.PhotoSize{},
		VideoThumbs:   []*mtproto.VideoSize{},
		DcId:          1,
		Attributes:    attrs,
	}).To_Document()
}

var vsReactAllowed = func() map[string]bool {
	m := map[string]bool{}
	for _, d := range vsReactDefs {
		m[d.emoticon] = true
	}
	return m
}()

func (vsReactions) MessagesGetAvailableReactions(ctx context.Context, in *mtproto.TLMessagesGetAvailableReactions) (*mtproto.Messages_AvailableReactions, error) {
	const hash = int32(0x56530001)
	if in.GetHash() == hash {
		return mtproto.MakeTLMessagesAvailableReactionsNotModified(nil).To_Messages_AvailableReactions(), nil
	}
	list := make([]*mtproto.AvailableReaction, 0, len(vsReactDefs))
	for _, d := range vsReactDefs {
		list = append(list, mtproto.MakeTLAvailableReaction(&mtproto.AvailableReaction{
			Reaction:          d.emoticon,
			Title:             d.title,
			StaticIcon:        vsReactDocument(d.docs[0], d.emoticon, 0),
			AppearAnimation:   vsReactDocument(d.docs[1], d.emoticon, 1),
			SelectAnimation:   vsReactDocument(d.docs[2], d.emoticon, 2),
			ActivateAnimation: vsReactDocument(d.docs[3], d.emoticon, 3),
			EffectAnimation:   vsReactDocument(d.docs[4], d.emoticon, 4),
			AroundAnimation:   vsReactDocument(d.docs[5], d.emoticon, 5),
			CenterIcon:        vsReactDocument(d.docs[6], d.emoticon, 6),
		}).To_AvailableReaction())
	}
	return mtproto.MakeTLMessagesAvailableReactions(&mtproto.Messages_AvailableReactions{
		Hash:      hash,
		Reactions: list,
	}).To_Messages_AvailableReactions(), nil
}

func vsEmojiReactions(n int) []*mtproto.Reaction {
	r := []*mtproto.Reaction{}
	for i, d := range vsReactDefs {
		if i >= n {
			break
		}
		r = append(r, mtproto.MakeTLReactionEmoji(&mtproto.Reaction{Emoticon: d.emoticon}).To_Reaction())
	}
	return r
}

func (vsReactions) MessagesGetTopReactions(ctx context.Context, in *mtproto.TLMessagesGetTopReactions) (*mtproto.Messages_Reactions, error) {
	return mtproto.MakeTLMessagesReactions(&mtproto.Messages_Reactions{Hash: 0x5653, Reactions: vsEmojiReactions(12)}).To_Messages_Reactions(), nil
}

func (vsReactions) MessagesGetRecentReactions(ctx context.Context, in *mtproto.TLMessagesGetRecentReactions) (*mtproto.Messages_Reactions, error) {
	return mtproto.MakeTLMessagesReactions(&mtproto.Messages_Reactions{Hash: 0x5653, Reactions: vsEmojiReactions(8)}).To_Messages_Reactions(), nil
}

func (vsReactions) MessagesClearRecentReactions(ctx context.Context, in *mtproto.TLMessagesClearRecentReactions) (*mtproto.Bool, error) {
	return mtproto.BoolTrue, nil
}

func (vsReactions) MessagesSetDefaultReaction(ctx context.Context, in *mtproto.TLMessagesSetDefaultReaction) (*mtproto.Bool, error) {
	return mtproto.BoolTrue, nil
}

func (vsReactions) MessagesReportReaction(ctx context.Context, in *mtproto.TLMessagesReportReaction) (*mtproto.Bool, error) {
	return mtproto.BoolTrue, nil
}

func (vsReactions) MessagesGetUnreadReactions(ctx context.Context, in *mtproto.TLMessagesGetUnreadReactions) (*mtproto.Messages_Messages, error) {
	return mtproto.MakeTLMessagesMessages(&mtproto.Messages_Messages{
		Messages: []*mtproto.Message{}, Chats: []*mtproto.Chat{}, Users: []*mtproto.User{}, Topics: []*mtproto.ForumTopic{},
	}).To_Messages_Messages(), nil
}

func (vsReactions) MessagesReadReactions(ctx context.Context, in *mtproto.TLMessagesReadReactions) (*mtproto.Messages_AffectedHistory, error) {
	return mtproto.MakeTLMessagesAffectedHistory(&mtproto.Messages_AffectedHistory{Pts: 0, PtsCount: 0, Offset: 0}).To_Messages_AffectedHistory(), nil
}

func (vsReactions) MessagesSetChatAvailableReactions(ctx context.Context, in *mtproto.TLMessagesSetChatAvailableReactions) (*mtproto.Updates, error) {
	return mtproto.MakeTLUpdates(&mtproto.Updates{Updates: []*mtproto.Update{}, Users: []*mtproto.User{}, Chats: []*mtproto.Chat{}, Date: int32(time.Now().Unix())}).To_Updates(), nil
}

func (vsReactions) MessagesGetMessageReactionsList(ctx context.Context, in *mtproto.TLMessagesGetMessageReactionsList) (*mtproto.Messages_MessageReactionsList, error) {
	return mtproto.MakeTLMessagesMessageReactionsList(&mtproto.Messages_MessageReactionsList{
		Count: 0, Reactions: []*mtproto.MessagePeerReaction{}, Chats: []*mtproto.Chat{}, Users: []*mtproto.User{},
	}).To_Messages_MessageReactionsList(), nil
}

func vsReactKey(b *mtproto.MessageBox) string {
	return fmt.Sprintf("vs_react:%d:%d:%d", b.GetDialogId1(), b.GetDialogId2(), b.GetDialogMessageId())
}

// vsBuildReactions — счётчики для конкретного зрителя (chosen — его реакция).
func vsBuildReactions(all map[string]string, viewer int64) *mtproto.MessageReactions {
	counts := map[string]int32{}
	for _, e := range all {
		counts[e]++
	}
	mine := all[strconv.FormatInt(viewer, 10)]
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}
		return keys[i] < keys[j]
	})
	res := make([]*mtproto.ReactionCount, 0, len(keys))
	for _, k := range keys {
		rc := &mtproto.ReactionCount{
			Reaction_REACTION: mtproto.MakeTLReactionEmoji(&mtproto.Reaction{Emoticon: k}).To_Reaction(),
			Count:             counts[k],
		}
		if k == mine {
			rc.ChosenOrder = wrapperspb.Int32(0)
			rc.Chosen = true
		}
		res = append(res, mtproto.MakeTLReactionCount(rc).To_ReactionCount())
	}
	return mtproto.MakeTLMessageReactions(&mtproto.MessageReactions{
		Results:         res,
		RecentReactions: []*mtproto.MessagePeerReaction{},
		TopReactors:     []*mtproto.MessageReactor{},
	}).To_MessageReactions()
}

func vsUpdateReactions(peer *mtproto.Peer, msgId int32, r *mtproto.MessageReactions) *mtproto.Update {
	return mtproto.MakeTLUpdateMessageReactions(&mtproto.Update{
		Peer_PEER:                  peer,
		MsgId_INT32:                msgId,
		Reactions_MESSAGEREACTIONS: r,
	}).To_Update()
}

// vsBoxPeer — собеседник/чат с точки зрения владельца ящика.
func vsBoxPeer(b *mtproto.MessageBox) *mtproto.Peer {
	if b.GetPeerType() == mtproto.PEER_CHAT {
		return mtproto.MakePeerChat(b.GetPeerId())
	}
	return mtproto.MakePeerUser(b.GetPeerId())
}

func (vsReactions) MessagesSendReaction(ctx context.Context, in *mtproto.TLMessagesSendReaction) (*mtproto.Updates, error) {
	md := vsMD(ctx)
	boxes, err := vsMessageCli.MessageGetUserMessageList(ctx, &messagepb.TLMessageGetUserMessageList{
		UserId: md.UserId, IdList: []int32{in.GetMsgId()},
	})
	if err != nil || boxes == nil || len(boxes.GetDatas()) == 0 {
		return nil, mtproto.ErrMsgIdInvalid
	}
	box := boxes.GetDatas()[0]
	key := vsReactKey(box)
	field := strconv.FormatInt(md.UserId, 10)

	emo := ""
	if rl := in.GetReaction_FLAGVECTORREACTION(); len(rl) > 0 {
		emo = rl[len(rl)-1].GetEmoticon()
	} else if s := in.GetReaction_FLAGSTRING(); s != nil {
		emo = s.GetValue()
	}
	if emo != "" && !vsReactAllowed[emo] {
		return nil, mtproto.ErrReactionInvalid
	}
	if emo == "" {
		_, _ = vsKV.Hdel(key, field)
	} else if err := vsKV.Hset(key, field, emo); err != nil {
		logx.WithContext(ctx).Errorf("sendReaction - redis: %v", err)
		return nil, mtproto.ErrInternalServerError
	}
	all, _ := vsKV.Hgetall(key)

	// разослать остальным участникам (у каждого свой номер сообщения)
	var users []int64
	if box.GetPeerType() == mtproto.PEER_CHAT {
		if chat, err := vsChatCli.ChatGetMutableChat(ctx, &chatpb.TLChatGetMutableChat{ChatId: box.GetPeerId()}); err == nil {
			chat.Walk(func(uid int64, p *mtproto.ImmutableChatParticipant) error {
				if uid != md.UserId && p.IsChatMemberStateNormal() {
					users = append(users, uid)
				}
				return nil
			})
		}
	} else if box.GetPeerType() == mtproto.PEER_USER && box.GetPeerId() != md.UserId {
		users = []int64{box.GetPeerId()}
	}
	if len(users) > 0 && vsSyncCli != nil {
		others, _ := vsMessageCli.MessageGetUserMessageListByDataIdUserIdList(ctx, &messagepb.TLMessageGetUserMessageListByDataIdUserIdList{
			Id: box.GetDialogMessageId(), UserIdList: users,
		})
		for _, ob := range others.GetDatas() {
			if ob.GetDialogId1() != box.GetDialogId1() || ob.GetDialogId2() != box.GetDialogId2() {
				continue
			}
			_, _ = vsSyncCli.SyncPushUpdates(ctx, &syncpb.TLSyncPushUpdates{
				UserId: ob.GetUserId(),
				Updates: mtproto.MakeTLUpdates(&mtproto.Updates{
					Updates: []*mtproto.Update{vsUpdateReactions(vsBoxPeer(ob), ob.GetMessageId(), vsBuildReactions(all, ob.GetUserId()))},
					Users:   []*mtproto.User{}, Chats: []*mtproto.Chat{}, Date: int32(time.Now().Unix()),
				}).To_Updates(),
			})
		}
	}
	return mtproto.MakeTLUpdates(&mtproto.Updates{
		Updates: []*mtproto.Update{vsUpdateReactions(vsBoxPeer(box), box.GetMessageId(), vsBuildReactions(all, md.UserId))},
		Users:   []*mtproto.User{}, Chats: []*mtproto.Chat{}, Date: int32(time.Now().Unix()),
	}).To_Updates(), nil
}

func (vsReactions) MessagesGetMessagesReactions(ctx context.Context, in *mtproto.TLMessagesGetMessagesReactions) (*mtproto.Updates, error) {
	md := vsMD(ctx)
	upd := mtproto.MakeTLUpdates(&mtproto.Updates{
		Updates: []*mtproto.Update{}, Users: []*mtproto.User{}, Chats: []*mtproto.Chat{}, Date: int32(time.Now().Unix()),
	}).To_Updates()
	if len(in.GetId()) == 0 {
		return upd, nil
	}
	boxes, err := vsMessageCli.MessageGetUserMessageList(ctx, &messagepb.TLMessageGetUserMessageList{
		UserId: md.UserId, IdList: in.GetId(),
	})
	if err != nil || boxes == nil {
		return upd, nil
	}
	for _, b := range boxes.GetDatas() {
		all, _ := vsKV.Hgetall(vsReactKey(b))
		upd.Updates = append(upd.Updates, vsUpdateReactions(vsBoxPeer(b), b.GetMessageId(), vsBuildReactions(all, md.UserId)))
	}
	return upd, nil
}
