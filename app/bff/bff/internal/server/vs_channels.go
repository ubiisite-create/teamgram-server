// VSети: каналы и супергруппы поверх обычных групп Teamgram (см. pkg/vschan).
package server

import (
	"github.com/teamgram/teamgram-server/pkg/vswebpage"
	sync_client "github.com/teamgram/teamgram-server/app/messenger/sync/client"
	kafka "github.com/teamgram/marmota/pkg/mq"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/kv"
	messagepb "github.com/teamgram/teamgram-server/app/service/biz/message/message"
	message_client "github.com/teamgram/teamgram-server/app/service/biz/message/client"
	"math/rand"

	msgpb "github.com/teamgram/teamgram-server/app/messenger/msg/msg/msg"
	msg_client "github.com/teamgram/teamgram-server/app/messenger/msg/msg/client"
	"context"

	"github.com/teamgram/marmota/pkg/net/rpcx"
	"github.com/teamgram/proto/mtproto"
	"github.com/teamgram/proto/mtproto/rpc/metadata"
	"github.com/teamgram/teamgram-server/app/bff/bff/internal/config"
	chatpb "github.com/teamgram/teamgram-server/app/service/biz/chat/chat"
	chat_client "github.com/teamgram/teamgram-server/app/service/biz/chat/client"
	dialogpb "github.com/teamgram/teamgram-server/app/service/biz/dialog/dialog"
	dialog_client "github.com/teamgram/teamgram-server/app/service/biz/dialog/client"
	userpb "github.com/teamgram/teamgram-server/app/service/biz/user/user"
	user_client "github.com/teamgram/teamgram-server/app/service/biz/user/client"
	"github.com/teamgram/teamgram-server/pkg/vschan"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	vsKV        kv.Store
	vsMessageCli message_client.MessageClient
	vsMsgCli    msg_client.MsgClient
	vsChatCli   chat_client.ChatClient
	vsDialogCli dialog_client.DialogClient
	vsUserCli   user_client.UserClient
)

// initVsChannels — клиенты, резолвер и перехватчик запросов/ответов bff.
func initVsChannels(srv *zrpc.RpcServer, c config.Config) {
	cli := rpcx.GetCachedRpcClient(c.BizServiceClient)
	vsChatCli = chat_client.NewChatClient(cli)
	vsMessageCli = message_client.NewMessageClient(cli)
	vsKV = kv.NewStore(c.KV)
	vswebpage.SetDfsEtcd(c.DfsClient.Etcd.Hosts)
	if c.SyncClient != nil {
		vsSyncCli = sync_client.NewSyncMqClient(kafka.MustKafkaProducer(c.SyncClient))
	}
	vsMsgCli = msg_client.NewMsgClient(rpcx.GetCachedRpcClient(c.MsgClient))
	vsDialogCli = dialog_client.NewDialogClient(cli)
	vsUserCli = user_client.NewUserClient(cli)
	mtproto.VsSetChannelUsernameFetcher(func(id int64) string {
		u, err := vsUserCli.UserGetChannelUsername(context.Background(), &userpb.TLUserGetChannelUsername{ChannelId: id})
		if err != nil || u == nil {
			return ""
		}
		return u.GetUsername()
	})
	vschan.Default = vschan.NewResolver(func(ctx context.Context, chatId int64) int32 {
		chat, err := vsChatCli.ChatGetMutableChat(context.Background(), &chatpb.TLChatGetMutableChat{ChatId: chatId})
		if err != nil || chat == nil {
			return 0
		}
		return mtproto.VsKind(chat.GetChat().GetAvailableReactionsType())
	})
	srv.AddUnaryInterceptors(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		vschan.RewriteIn(req)
		// VSети: просмотры постов канала
		if r, ok := req.(*mtproto.TLMessagesGetMessagesViews); ok {
			if rv, handled := vsGetViews(ctx, r); handled {
				vschan.Default.RewriteOut(ctx, rv)
				return rv, nil
			}
		}
		resp, err := handler(ctx, req)
		if err == nil && resp != nil {
			vschan.Default.RewriteOut(ctx, resp)
		}
		return resp, err
	})
}

type vsChannels struct {
	mtproto.UnimplementedRPCChannelsServer
	chats    mtproto.RPCChatsServer
	messages mtproto.RPCMessagesServer
}

func newVsChannels(chats mtproto.RPCChatsServer, messages mtproto.RPCMessagesServer) *vsChannels {
	return &vsChannels{chats: chats, messages: messages}
}

func vsMD(ctx context.Context) *metadata.RpcMetadata { return metadata.RpcMetadataFromIncoming(ctx) }

func vsInputChannelId(ch *mtproto.InputChannel) int64 {
	if ch == nil {
		return 0
	}
	return ch.GetChannelId()
}

func vsInputPeerChat(id int64) *mtproto.InputPeer {
	return mtproto.MakeTLInputPeerChat(&mtproto.InputPeer{ChatId: id}).To_InputPeer()
}

func (s *vsChannels) getChannel(ctx context.Context, ch *mtproto.InputChannel) (*mtproto.MutableChat, error) {
	id := vsInputChannelId(ch)
	if id == 0 {
		return nil, mtproto.ErrChannelInvalid
	}
	chat, err := vsChatCli.ChatGetMutableChat(ctx, &chatpb.TLChatGetMutableChat{ChatId: id})
	if err != nil || chat == nil || !chat.IsVsChannel() {
		return nil, mtproto.ErrChannelInvalid
	}
	return chat, nil
}

// ChannelsCreateChannel — канал (broadcast) или супергруппа (megagroup), сразу без участников.
func (s *vsChannels) ChannelsCreateChannel(ctx context.Context, in *mtproto.TLChannelsCreateChannel) (*mtproto.Updates, error) {
	prefix, kind := mtproto.VsTitlePrefixMegagroup, mtproto.VsKindMegagroup
	if in.GetBroadcast() {
		prefix, kind = mtproto.VsTitlePrefixBroadcast, mtproto.VsKindBroadcast
	}
	if in.GetTitle() == "" {
		return nil, mtproto.ErrChatTitleEmpty
	}
	r, err := s.chats.MessagesCreateChat92CEDDD4(ctx, &mtproto.TLMessagesCreateChat92CEDDD4{
		Title:     prefix + in.GetTitle(),
		TtlPeriod: in.GetTtlPeriod(),
	})
	if err != nil {
		logx.WithContext(ctx).Errorf("channels.createChannel - error: %v", err)
		return nil, err
	}
	upd := r.GetUpdates()
	for _, ch := range upd.GetChats() {
		vschan.Default.Remember(ch.GetId(), kind)
		if in.GetAbout() != "" {
			_, _ = s.chats.MessagesEditChatAbout(ctx, &mtproto.TLMessagesEditChatAbout{
				Peer:  vsInputPeerChat(ch.GetId()),
				About: in.GetAbout(),
			})
		}
	}
	return upd, nil
}

func (s *vsChannels) ChannelsGetChannels(ctx context.Context, in *mtproto.TLChannelsGetChannels) (*mtproto.Messages_Chats, error) {
	md := vsMD(ctx)
	rV := mtproto.MakeTLMessagesChats(&mtproto.Messages_Chats{Chats: []*mtproto.Chat{}}).To_Messages_Chats()
	for _, ch := range in.GetId() {
		if chat, err := s.getChannel(ctx, ch); err == nil {
			rV.Chats = append(rV.Chats, chat.ToUnsafeChat(md.UserId))
		}
	}
	return rV, nil
}

func (s *vsChannels) ChannelsGetFullChannel(ctx context.Context, in *mtproto.TLChannelsGetFullChannel) (*mtproto.Messages_ChatFull, error) {
	md := vsMD(ctx)
	chat, err := s.getChannel(ctx, in.GetChannel())
	if err != nil {
		return nil, err
	}
	var (
		count, admins int32
		idList        []int64
	)
	chat.Walk(func(userId int64, p *mtproto.ImmutableChatParticipant) error {
		if p.IsChatMemberStateNormal() {
			count++
			idList = append(idList, p.UserId)
			if p.IsChatMemberCreator() || p.IsChatMemberAdmin() {
				admins++
			}
		}
		return nil
	})

	full := &mtproto.ChatFull{
		CanViewParticipants: true,
		CanSetUsername:      true,
		CanDeleteChannel:    chat.GetChat().GetCreator() == md.UserId,
		Id:                  chat.Id(),
		About:               chat.About(),
		ParticipantsCount:   &wrapperspb.Int32Value{Value: count},
		AdminsCount:         &wrapperspb.Int32Value{Value: admins},
		ChatPhoto:           chat.Photo(),
		Pts:                 1,
	}
	if full.ChatPhoto == nil {
		full.ChatPhoto = mtproto.MakeTLPhotoEmpty(nil).To_Photo()
	}
	// прочитано/не прочитано — из диалога пользователя
	if dl, _ := vsDialogCli.DialogGetDialogsByIdList(ctx, &dialogpb.TLDialogGetDialogsByIdList{
		UserId: md.UserId,
		IdList: []int64{mtproto.MakePeerDialogId(mtproto.PEER_CHAT, chat.Id())},
	}); dl != nil && len(dl.Datas) > 0 {
		d := dl.Datas[0].GetDialog()
		full.ReadInboxMaxId = d.GetReadInboxMaxId()
		full.ReadOutboxMaxId = d.GetReadOutboxMaxId()
		full.UnreadCount = d.GetUnreadCount()
		full.FolderId = d.GetFolderId()
	}
	if ns, _ := vsUserCli.UserGetNotifySettings(ctx, &userpb.TLUserGetNotifySettings{
		UserId: md.UserId, PeerType: mtproto.PEER_CHAT, PeerId: chat.Id(),
	}); ns != nil {
		full.NotifySettings = ns
	} else {
		full.NotifySettings = mtproto.MakeTLPeerNotifySettings(nil).To_PeerNotifySettings()
	}
	if me, ok := chat.GetImmutableChatParticipant(md.UserId); ok && me.Link != "" &&
		(me.IsChatMemberCreator() || me.IsChatMemberAdmin()) {
		full.ExportedInvite, _ = vsChatCli.ChatGetExportedChatInvite(ctx, &chatpb.TLChatGetExportedChatInvite{
			ChatId: chat.Id(), Link: me.Link,
		})
	}
	rV := mtproto.MakeTLMessagesChatFull(&mtproto.Messages_ChatFull{
		FullChat: mtproto.MakeTLChannelFull(full).To_ChatFull(),
		Chats:    []*mtproto.Chat{chat.ToUnsafeChat(md.UserId)},
		Users:    []*mtproto.User{},
	}).To_Messages_ChatFull()
	if mUsers, _ := vsUserCli.UserGetMutableUsers(ctx, &userpb.TLUserGetMutableUsers{Id: idList}); mUsers != nil {
		rV.Users = mUsers.GetUserListByIdList(md.UserId, idList...)
	}
	return rV, nil
}

func vsChannelParticipant(p *mtproto.ImmutableChatParticipant, selfId int64) *mtproto.ChannelParticipant {
	cp := &mtproto.ChannelParticipant{
		UserId:              p.UserId,
		Date:                int32(p.InvitedAt),
		InviterId_INT64:     p.InviterUserId,
		InviterId_FLAGINT64: mtproto.MakeFlagsInt64(p.InviterUserId),
		PromotedBy:          p.InviterUserId,
		Self:                p.UserId == selfId,
	}
	if cp.Date == 0 {
		cp.Date = int32(p.Date)
	}
	switch {
	case p.IsChatMemberCreator():
		cp.AdminRights = mtproto.VsFullAdminRights(true)
		return mtproto.MakeTLChannelParticipantCreator(cp).To_ChannelParticipant()
	case p.IsChatMemberAdmin():
		cp.AdminRights = p.GetAdminRights()
		if cp.AdminRights == nil {
			cp.AdminRights = mtproto.VsFullAdminRights(false)
		}
		cp.CanEdit = true
		return mtproto.MakeTLChannelParticipantAdmin(cp).To_ChannelParticipant()
	case p.UserId == selfId:
		return mtproto.MakeTLChannelParticipantSelf(cp).To_ChannelParticipant()
	}
	return mtproto.MakeTLChannelParticipant(cp).To_ChannelParticipant()
}

func (s *vsChannels) ChannelsGetParticipants(ctx context.Context, in *mtproto.TLChannelsGetParticipants) (*mtproto.Channels_ChannelParticipants, error) {
	md := vsMD(ctx)
	chat, err := s.getChannel(ctx, in.GetChannel())
	if err != nil {
		return nil, err
	}
	adminsOnly := in.GetFilter().GetPredicateName() == mtproto.Predicate_channelParticipantsAdmins
	if fp := in.GetFilter().GetPredicateName(); fp == mtproto.Predicate_channelParticipantsKicked ||
		fp == mtproto.Predicate_channelParticipantsBanned || fp == mtproto.Predicate_channelParticipantsBots {
		return mtproto.MakeTLChannelsChannelParticipants(&mtproto.Channels_ChannelParticipants{
			Participants: []*mtproto.ChannelParticipant{}, Chats: []*mtproto.Chat{}, Users: []*mtproto.User{},
		}).To_Channels_ChannelParticipants(), nil
	}
	var (
		all    []*mtproto.ChannelParticipant
		idList []int64
	)
	chat.Walk(func(userId int64, p *mtproto.ImmutableChatParticipant) error {
		if !p.IsChatMemberStateNormal() {
			return nil
		}
		if adminsOnly && !p.IsChatMemberCreator() && !p.IsChatMemberAdmin() {
			return nil
		}
		all = append(all, vsChannelParticipant(p, md.UserId))
		idList = append(idList, p.UserId)
		return nil
	})
	count := int32(len(all))
	off, lim := int(in.GetOffset()), int(in.GetLimit())
	if off > len(all) {
		off = len(all)
	}
	if lim <= 0 || off+lim > len(all) {
		lim = len(all) - off
	}
	page, pageIds := all[off:off+lim], idList[off:off+lim]
	rV := mtproto.MakeTLChannelsChannelParticipants(&mtproto.Channels_ChannelParticipants{
		Count: count, Participants: page, Chats: []*mtproto.Chat{}, Users: []*mtproto.User{},
	}).To_Channels_ChannelParticipants()
	if mUsers, _ := vsUserCli.UserGetMutableUsers(ctx, &userpb.TLUserGetMutableUsers{Id: pageIds}); mUsers != nil {
		rV.Users = mUsers.GetUserListByIdList(md.UserId, pageIds...)
	}
	return rV, nil
}

func (s *vsChannels) ChannelsGetParticipant(ctx context.Context, in *mtproto.TLChannelsGetParticipant) (*mtproto.Channels_ChannelParticipant, error) {
	md := vsMD(ctx)
	chat, err := s.getChannel(ctx, in.GetChannel())
	if err != nil {
		return nil, err
	}
	uid := in.GetParticipant().GetUserId()
	if in.GetParticipant().GetPredicateName() == mtproto.Predicate_inputPeerSelf || uid == 0 {
		uid = md.UserId
	}
	p, ok := chat.GetImmutableChatParticipant(uid)
	if !ok || !p.IsChatMemberStateNormal() {
		return nil, mtproto.ErrUserNotParticipant
	}
	rV := mtproto.MakeTLChannelsChannelParticipant(&mtproto.Channels_ChannelParticipant{
		Participant: vsChannelParticipant(p, md.UserId), Chats: []*mtproto.Chat{}, Users: []*mtproto.User{},
	}).To_Channels_ChannelParticipant()
	if mUsers, _ := vsUserCli.UserGetMutableUsers(ctx, &userpb.TLUserGetMutableUsers{Id: []int64{uid}}); mUsers != nil {
		rV.Users = mUsers.GetUserListByIdList(md.UserId, uid)
	}
	return rV, nil
}

func (s *vsChannels) ChannelsReadHistory(ctx context.Context, in *mtproto.TLChannelsReadHistory) (*mtproto.Bool, error) {
	_, err := s.messages.MessagesReadHistory(ctx, &mtproto.TLMessagesReadHistory{
		Peer:  vsInputPeerChat(vsInputChannelId(in.GetChannel())),
		MaxId: in.GetMaxId(),
	})
	if err != nil {
		return nil, err
	}
	return mtproto.BoolTrue, nil
}

func (s *vsChannels) ChannelsReadMessageContents(ctx context.Context, in *mtproto.TLChannelsReadMessageContents) (*mtproto.Bool, error) {
	return mtproto.BoolTrue, nil
}

func (s *vsChannels) ChannelsGetMessages(ctx context.Context, in *mtproto.TLChannelsGetMessages) (*mtproto.Messages_Messages, error) {
	return s.messages.MessagesGetMessages(ctx, &mtproto.TLMessagesGetMessages{
		Id_VECTORINPUTMESSAGE: in.GetId_VECTORINPUTMESSAGE(),
		Id_VECTORINT32:        in.GetId_VECTORINT32(),
	})
}

func (s *vsChannels) ChannelsDeleteMessages(ctx context.Context, in *mtproto.TLChannelsDeleteMessages) (*mtproto.Messages_AffectedMessages, error) {
	return s.messages.MessagesDeleteMessages(ctx, &mtproto.TLMessagesDeleteMessages{
		Revoke: true,
		Id:     in.GetId(),
	})
}

func (s *vsChannels) ChannelsEditTitle(ctx context.Context, in *mtproto.TLChannelsEditTitle) (*mtproto.Updates, error) {
	return s.chats.MessagesEditChatTitle(ctx, &mtproto.TLMessagesEditChatTitle{
		ChatId: vsInputChannelId(in.GetChannel()),
		Title:  in.GetTitle(),
	})
}

func (s *vsChannels) ChannelsEditPhoto(ctx context.Context, in *mtproto.TLChannelsEditPhoto) (*mtproto.Updates, error) {
	return s.chats.MessagesEditChatPhoto(ctx, &mtproto.TLMessagesEditChatPhoto{
		ChatId: vsInputChannelId(in.GetChannel()),
		Photo:  in.GetPhoto(),
	})
}

func (s *vsChannels) ChannelsLeaveChannel(ctx context.Context, in *mtproto.TLChannelsLeaveChannel) (*mtproto.Updates, error) {
	return s.chats.MessagesDeleteChatUser(ctx, &mtproto.TLMessagesDeleteChatUser{
		ChatId: vsInputChannelId(in.GetChannel()),
		UserId: mtproto.MakeTLInputUserSelf(nil).To_InputUser(),
	})
}

func (s *vsChannels) ChannelsDeleteChannel(ctx context.Context, in *mtproto.TLChannelsDeleteChannel) (*mtproto.Updates, error) {
	if _, err := s.chats.MessagesDeleteChat(ctx, &mtproto.TLMessagesDeleteChat{
		ChatId: vsInputChannelId(in.GetChannel()),
	}); err != nil {
		return nil, err
	}
	return mtproto.MakeTLUpdates(&mtproto.Updates{
		Updates: []*mtproto.Update{}, Users: []*mtproto.User{}, Chats: []*mtproto.Chat{},
	}).To_Updates(), nil
}

func (s *vsChannels) ChannelsEditAdmin(ctx context.Context, in *mtproto.TLChannelsEditAdmin) (*mtproto.Updates, error) {
	isAdmin := mtproto.BoolFalse
	if r := in.GetAdminRights(); r != nil && (r.ChangeInfo || r.PostMessages || r.EditMessages || r.DeleteMessages ||
		r.BanUsers || r.InviteUsers || r.PinMessages || r.AddAdmins || r.ManageCall || r.Other) {
		isAdmin = mtproto.BoolTrue
	}
	if _, err := s.chats.MessagesEditChatAdmin(ctx, &mtproto.TLMessagesEditChatAdmin{
		ChatId:  vsInputChannelId(in.GetChannel()),
		UserId:  in.GetUserId(),
		IsAdmin: isAdmin,
	}); err != nil {
		return nil, err
	}
	md := vsMD(ctx)
	upd := mtproto.MakeTLUpdates(&mtproto.Updates{
		Updates: []*mtproto.Update{}, Users: []*mtproto.User{}, Chats: []*mtproto.Chat{},
	}).To_Updates()
	if chat, err := s.getChannel(ctx, in.GetChannel()); err == nil {
		upd.Chats = append(upd.Chats, chat.ToUnsafeChat(md.UserId))
	}
	return upd, nil
}

func (s *vsChannels) ChannelsExportMessageLink(ctx context.Context, in *mtproto.TLChannelsExportMessageLink) (*mtproto.ExportedMessageLink, error) {
	return mtproto.MakeTLExportedMessageLink(&mtproto.ExportedMessageLink{
		Link: "https://me.vsseti.ru/",
		Html: "",
	}).To_ExportedMessageLink(), nil
}

func (s *vsChannels) ChannelsGetAdminedPublicChannels(ctx context.Context, in *mtproto.TLChannelsGetAdminedPublicChannels) (*mtproto.Messages_Chats, error) {
	return mtproto.MakeTLMessagesChats(&mtproto.Messages_Chats{Chats: []*mtproto.Chat{}}).To_Messages_Chats(), nil
}

func (s *vsChannels) ChannelsGetInactiveChannels(ctx context.Context, in *mtproto.TLChannelsGetInactiveChannels) (*mtproto.Messages_InactiveChats, error) {
	return mtproto.MakeTLMessagesInactiveChats(&mtproto.Messages_InactiveChats{
		Dates: []int32{}, Chats: []*mtproto.Chat{}, Users: []*mtproto.User{},
	}).To_Messages_InactiveChats(), nil
}

func (s *vsChannels) ChannelsToggleSignatures(ctx context.Context, in *mtproto.TLChannelsToggleSignatures) (*mtproto.Updates, error) {
	return mtproto.MakeTLUpdates(&mtproto.Updates{Updates: []*mtproto.Update{}, Users: []*mtproto.User{}, Chats: []*mtproto.Chat{}}).To_Updates(), nil
}

// vsJoin — пользователь сам вступает в канал/супергруппу (по @ссылке).
func (s *vsChannels) vsJoin(ctx context.Context, chatId int64) (*mtproto.Updates, error) {
	md := vsMD(ctx)
	chat, err := vsChatCli.ChatGetMutableChat(ctx, &chatpb.TLChatGetMutableChat{ChatId: chatId})
	if err != nil || chat == nil || !chat.IsVsChannel() {
		return nil, mtproto.ErrChannelInvalid
	}
	if p, ok := chat.GetImmutableChatParticipant(md.UserId); ok && p.IsChatMemberStateNormal() {
		return nil, mtproto.ErrUserAlreadyParticipant
	}
	// вступить сам можно только в публичный (с @именем)
	if u, _ := vsUserCli.UserGetChannelUsername(ctx, &userpb.TLUserGetChannelUsername{ChannelId: chatId}); u == nil || u.GetUsername() == "" {
		return nil, mtproto.ErrChannelPrivate
	}
	chat, err = vsChatCli.ChatAddChatUser(ctx, &chatpb.TLChatAddChatUser{
		ChatId:    chatId,
		InviterId: chat.Creator(),
		UserId:    md.UserId,
	})
	if err != nil {
		return nil, err
	}
	vschan.Default.Remember(chatId, mtproto.VsKind(chat.GetChat().GetAvailableReactionsType()))
	return vsMsgCli.MsgSendMessageV2(ctx, &msgpb.TLMsgSendMessageV2{
		UserId:    md.UserId,
		AuthKeyId: md.PermAuthKeyId,
		PeerType:  mtproto.PEER_CHAT,
		PeerId:    chatId,
		Message: []*msgpb.OutboxMessage{
			msgpb.MakeTLOutboxMessage(&msgpb.OutboxMessage{
				NoWebpage: true,
				RandomId:  rand.Int63(),
				Message:   chat.MakeMessageService(md.UserId, mtproto.MakeMessageActionChatAddUser(md.UserId)),
			}).To_OutboxMessage(),
		},
	})
}

func (s *vsChannels) ChannelsJoinChannel7F6A1E22(ctx context.Context, in *mtproto.TLChannelsJoinChannel7F6A1E22) (*mtproto.Messages_ChatInviteJoinResult, error) {
	upd, err := s.vsJoin(ctx, vsInputChannelId(in.GetChannel()))
	if err != nil {
		return nil, err
	}
	return mtproto.MakeTLMessagesChatInviteJoinResultOk(&mtproto.Messages_ChatInviteJoinResult{
		Updates: upd,
	}).To_Messages_ChatInviteJoinResult(), nil
}

func (s *vsChannels) ChannelsJoinChannel24B524C5(ctx context.Context, in *mtproto.TLChannelsJoinChannel24B524C5) (*mtproto.Updates, error) {
	return s.vsJoin(ctx, vsInputChannelId(in.GetChannel()))
}

// vsInvite — админ добавляет людей в канал/супергруппу.
func (s *vsChannels) vsInvite(ctx context.Context, ch *mtproto.InputChannel, users []*mtproto.InputUser) (*mtproto.Messages_InvitedUsers, error) {
	chatId := vsInputChannelId(ch)
	var last *mtproto.Updates
	var firstErr error
	for _, u := range users {
		r, err := s.chats.MessagesAddChatUserCBC6D107(ctx, &mtproto.TLMessagesAddChatUserCBC6D107{
			ChatId: chatId,
			UserId: u,
		})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		last = r.GetUpdates()
	}
	if last == nil {
		if firstErr != nil {
			return nil, firstErr
		}
		last = mtproto.MakeTLUpdates(&mtproto.Updates{Updates: []*mtproto.Update{}, Users: []*mtproto.User{}, Chats: []*mtproto.Chat{}}).To_Updates()
	}
	return mtproto.MakeTLMessagesInvitedUsers(&mtproto.Messages_InvitedUsers{
		Updates:         last,
		MissingInvitees: []*mtproto.MissingInvitee{},
	}).To_Messages_InvitedUsers(), nil
}

func (s *vsChannels) ChannelsInviteToChannelC9E33D54(ctx context.Context, in *mtproto.TLChannelsInviteToChannelC9E33D54) (*mtproto.Messages_InvitedUsers, error) {
	return s.vsInvite(ctx, in.GetChannel(), in.GetUsers())
}

func (s *vsChannels) ChannelsInviteToChannel199F3A6C(ctx context.Context, in *mtproto.TLChannelsInviteToChannel199F3A6C) (*mtproto.Updates, error) {
	r, err := s.vsInvite(ctx, in.GetChannel(), in.GetUsers())
	if err != nil {
		return nil, err
	}
	return r.GetUpdates(), nil
}

// VSети: «фрагмент»-методы имён (несколько @имён у Telegram). У нас одно имя — подтверждаем.
type vsFragment struct {
	mtproto.UnimplementedRPCFragmentServer
}

func (vsFragment) ChannelsDeactivateAllUsernames(ctx context.Context, in *mtproto.TLChannelsDeactivateAllUsernames) (*mtproto.Bool, error) {
	return mtproto.BoolTrue, nil
}

func (vsFragment) ChannelsReorderUsernames(ctx context.Context, in *mtproto.TLChannelsReorderUsernames) (*mtproto.Bool, error) {
	return mtproto.BoolTrue, nil
}

func (vsFragment) ChannelsToggleUsername(ctx context.Context, in *mtproto.TLChannelsToggleUsername) (*mtproto.Bool, error) {
	return mtproto.BoolTrue, nil
}

func (vsFragment) AccountReorderUsernames(ctx context.Context, in *mtproto.TLAccountReorderUsernames) (*mtproto.Bool, error) {
	return mtproto.BoolTrue, nil
}

func (vsFragment) AccountToggleUsername(ctx context.Context, in *mtproto.TLAccountToggleUsername) (*mtproto.Bool, error) {
	return mtproto.BoolTrue, nil
}

// vsGetViews — просмотры постов канала: уникальные зрители (redis set по общему номеру поста).
func vsGetViews(ctx context.Context, in *mtproto.TLMessagesGetMessagesViews) (*mtproto.Messages_MessageViews, bool) {
	md := vsMD(ctx)
	chatId := in.GetPeer().GetChatId()
	if chatId == 0 || vschan.Default.Kind(ctx, chatId) == 0 || len(in.GetId()) == 0 {
		return nil, false
	}
	views := make([]*mtproto.MessageViews, len(in.GetId()))
	for i := range views {
		views[i] = mtproto.MakeTLMessageViews(&mtproto.MessageViews{}).To_MessageViews()
	}
	boxes, err := vsMessageCli.MessageGetUserMessageList(ctx, &messagepb.TLMessageGetUserMessageList{
		UserId: md.UserId,
		IdList: in.GetId(),
	})
	if err == nil && boxes != nil {
		idx := map[int32]int{}
		for i, id := range in.GetId() {
			idx[id] = i
		}
		boxes.Walk(func(_ int, b *mtproto.MessageBox) {
			i, ok := idx[b.GetMessageId()]
			if !ok {
				return
			}
			key := fmt.Sprintf("vs_views:%d:%d", chatId, b.GetDialogMessageId())
			if mtproto.FromBool(in.GetIncrement()) {
				_, _ = vsKV.Sadd(key, md.UserId)
			}
			n, _ := vsKV.Scard(key)
			if n < 1 {
				n = 1
			}
			views[i].Views = &wrapperspb.Int32Value{Value: int32(n)}
		})
	}
	return mtproto.MakeTLMessagesMessageViews(&mtproto.Messages_MessageViews{
		Views: views,
		Chats: []*mtproto.Chat{},
		Users: []*mtproto.User{},
	}).To_Messages_MessageViews(), true
}
