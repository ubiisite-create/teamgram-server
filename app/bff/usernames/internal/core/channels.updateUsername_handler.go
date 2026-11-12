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
	"github.com/teamgram/proto/mtproto"
	"github.com/teamgram/teamgram-server/app/service/biz/user/user"
)

// ChannelsUpdateUsername
// channels.updateUsername#3514b3de channel:InputChannel username:string = Bool;
func (c *UsernamesCore) ChannelsUpdateUsername(in *mtproto.TLChannelsUpdateUsername) (*mtproto.Bool, error) {
	chat, err := c.vsChannelForEdit(in.GetChannel())
	if err != nil {
		return nil, err
	}
	name := in.GetUsername()
	if name != "" && !vsUsernameValid(name) {
		return nil, mtproto.ErrUsernameInvalid
	}
	if name != "" {
		if ok, _ := c.ChannelsCheckUsername(&mtproto.TLChannelsCheckUsername{Channel: in.GetChannel(), Username: name}); !mtproto.FromBool(ok) {
			return nil, mtproto.ErrUsernameOccupied
		}
	}
	_, _ = c.svcCtx.Dao.UserClient.UserDeleteUsernameByPeer(c.ctx, &user.TLUserDeleteUsernameByPeer{
		PeerType: mtproto.PEER_CHANNEL, PeerId: chat.Id(),
	})
	if name != "" {
		r, err := c.svcCtx.Dao.UserClient.UserUpdateUsernameByPeer(c.ctx, &user.TLUserUpdateUsernameByPeer{
			PeerType: mtproto.PEER_CHANNEL, PeerId: chat.Id(), Username: name,
		})
		if err != nil {
			return nil, err
		}
		if !mtproto.FromBool(r) {
			return nil, mtproto.ErrUsernameOccupied
		}
	}
	mtproto.VsForgetChannelUsername(chat.Id())
	return mtproto.BoolTrue, nil
}
