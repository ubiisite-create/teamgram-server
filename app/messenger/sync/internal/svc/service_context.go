/*
 * Created from 'scheme.tl' by 'mtprotoc'
 *
 * Copyright (c) 2021-present,  Teamgram Studio (https://teamgram.io).
 *  All rights reserved.
 *
 * Author: teamgramio (teamgram.io@gmail.com)
 */

package svc

import (
	"github.com/teamgram/marmota/pkg/net/rpcx"
	user_client "github.com/teamgram/teamgram-server/app/service/biz/user/client"
	userpb "github.com/teamgram/teamgram-server/app/service/biz/user/user"
	"context"

	"github.com/teamgram/proto/mtproto"
	chatpb "github.com/teamgram/teamgram-server/app/service/biz/chat/chat"
	"github.com/teamgram/teamgram-server/pkg/vschan"
	"github.com/teamgram/teamgram-server/app/messenger/sync/internal/config"
	"github.com/teamgram/teamgram-server/app/messenger/sync/internal/dao"
)

type ServiceContext struct {
	Config config.Config
	*dao.Dao
}

func NewServiceContext(c config.Config) *ServiceContext {
	d := dao.New(c)
	// VSети: резолвер каналов для пушей
	vschan.Default = vschan.NewResolver(func(ctx context.Context, chatId int64) int32 {
		chat, err := d.ChatClient.ChatGetMutableChat(context.Background(), &chatpb.TLChatGetMutableChat{ChatId: chatId})
		if err != nil || chat == nil {
			return 0
		}
		return mtproto.VsKind(chat.GetChat().GetAvailableReactionsType())
	})
	vsUserCli := user_client.NewUserClient(rpcx.GetCachedRpcClient(c.ChatClient))
	mtproto.VsSetChannelUsernameFetcher(func(id int64) string {
		u, err := vsUserCli.UserGetChannelUsername(context.Background(), &userpb.TLUserGetChannelUsername{ChannelId: id})
		if err != nil || u == nil {
			return ""
		}
		return u.GetUsername()
	})
	return &ServiceContext{
		Config: c,
		Dao:    d,
	}
}
