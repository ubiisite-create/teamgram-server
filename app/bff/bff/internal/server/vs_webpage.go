// VSети: предпросмотр ссылки, пока человек набирает сообщение.
package server

import (
	"context"

	"github.com/teamgram/proto/mtproto"
	"github.com/teamgram/teamgram-server/pkg/vswebpage"
	"mvdan.cc/xurls/v2"
)

type vsWebPage struct {
	mtproto.UnimplementedRPCWebPageServer
}

func vsPreviewMedia(ctx context.Context, text string) *mtproto.MessageMedia {
	u := xurls.Relaxed().FindString(text)
	if u == "" {
		return mtproto.MakeTLMessageMediaEmpty(nil).To_MessageMedia()
	}
	wp, err := vswebpage.Preview(ctx, u)
	if err != nil || wp == nil {
		return mtproto.MakeTLMessageMediaEmpty(nil).To_MessageMedia()
	}
	return mtproto.MakeTLMessageMediaWebPage(&mtproto.MessageMedia{Webpage: wp}).To_MessageMedia()
}

func (vsWebPage) MessagesGetWebPagePreview570D6F6F(ctx context.Context, in *mtproto.TLMessagesGetWebPagePreview570D6F6F) (*mtproto.Messages_WebPagePreview, error) {
	return mtproto.MakeTLMessagesWebPagePreview(&mtproto.Messages_WebPagePreview{
		Media: vsPreviewMedia(ctx, in.GetMessage()),
		Chats: []*mtproto.Chat{},
		Users: []*mtproto.User{},
	}).To_Messages_WebPagePreview(), nil
}

func (vsWebPage) MessagesGetWebPagePreview8B68B0CC(ctx context.Context, in *mtproto.TLMessagesGetWebPagePreview8B68B0CC) (*mtproto.MessageMedia, error) {
	return vsPreviewMedia(ctx, in.GetMessage()), nil
}
