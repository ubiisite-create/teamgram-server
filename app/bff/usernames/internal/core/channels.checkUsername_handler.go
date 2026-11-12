// Copyright 2022 Teamgram Authors
//  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: teamgramio (teamgram.io@gmail.com)
//

package core

import (
	"github.com/teamgram/marmota/pkg/strings2"
	"github.com/teamgram/marmota/pkg/utils"
	"github.com/teamgram/proto/mtproto"
	chatpb "github.com/teamgram/teamgram-server/app/service/biz/chat/chat"
	"github.com/teamgram/teamgram-server/app/service/biz/user/user"
)

// ChannelsCheckUsername
// channels.checkUsername#10e6bd2c channel:InputChannel username:string = Bool;
func (c *UsernamesCore) ChannelsCheckUsername(in *mtproto.TLChannelsCheckUsername) (*mtproto.Bool, error) {
	if !vsUsernameValid(in.GetUsername()) {
		return nil, mtproto.ErrUsernameInvalid
	}
	existed, err := c.svcCtx.Dao.UserClient.UserCheckChannelUsername(c.ctx, &user.TLUserCheckChannelUsername{
		ChannelId: in.GetChannel().GetChannelId(),
		Username:  in.GetUsername(),
	})
	if err != nil {
		return nil, err
	}
	if existed.GetPredicateName() == user.Predicate_usernameExistedNotMe {
		return mtproto.BoolFalse, nil
	}
	return mtproto.BoolTrue, nil
}

// VSети: канал = группа с признаком (см. pkg/vschan); имя хранится как PEER_CHANNEL.
func (c *UsernamesCore) vsChannelForEdit(ch *mtproto.InputChannel) (*mtproto.MutableChat, error) {
	chat, err := c.svcCtx.Dao.ChatClient.ChatGetMutableChat(c.ctx, &chatpb.TLChatGetMutableChat{ChatId: ch.GetChannelId()})
	if err != nil || chat == nil || !chat.IsVsChannel() {
		return nil, mtproto.ErrChannelInvalid
	}
	me, ok := chat.GetImmutableChatParticipant(c.MD.UserId)
	if !ok || !(me.IsChatMemberCreator() || me.IsChatMemberAdmin()) {
		return nil, mtproto.ErrChatAdminRequired
	}
	return chat, nil
}

func vsUsernameValid(u string) bool {
	return len(u) >= user.MinUsernameLen && len(u) <= 32 && strings2.IsAlNumString(u) && !utils.IsNumber(u[0])
}
