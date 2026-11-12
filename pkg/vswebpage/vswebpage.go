// VSети: предпросмотр ссылок (карточка с названием, описанием и картинкой), как в Telegram.
package vswebpage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"hash/fnv"
	"html"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/teamgram/proto/mtproto"
	dfs_client "github.com/teamgram/teamgram-server/app/service/dfs/client"
	dfspb "github.com/teamgram/teamgram-server/app/service/dfs/dfs"

	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	vsEtcdHosts []string
	vsDfsOnce   sync.Once
	vsDfs       dfs_client.DfsClient

	vsWpMu    sync.Mutex
	vsWpCache = map[string]vsWpItem{}

	vsHttp = &http.Client{Timeout: 8 * time.Second}
)

type vsWpItem struct {
	wp *mtproto.WebPage
	at time.Time
}

func SetDfsEtcd(hosts []string) { vsEtcdHosts = hosts }

// SetDfsClient — если у процесса уже есть клиент хранилища.
func SetDfsClient(c dfs_client.DfsClient) { vsDfsOnce.Do(func() {}); vsDfs = c }

func vsDfsClient() dfs_client.DfsClient {
	vsDfsOnce.Do(func() {
		cli, err := zrpc.NewClient(zrpc.RpcClientConf{Etcd: discov.EtcdConf{Hosts: vsEtcdHosts, Key: "service.dfs"}, NonBlock: true})
		if err != nil {
			logx.Errorf("vs webpage: dfs client: %v", err)
			return
		}
		vsDfs = dfs_client.NewDfsClient(cli)
	})
	return vsDfs
}

var (
	reMeta  = regexp.MustCompile(`(?is)<meta\s[^>]*>`)
	reAttr  = regexp.MustCompile(`(?is)(property|name|content|itemprop)\s*=\s*("([^"]*)"|'([^']*)')`)
	reTitle = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
)

func vsGet(ctx context.Context, u string, limit int64) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, "", err
	}
	// так представляются боты предпросмотра — сайты отдают им og-разметку
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; TelegramBot (like TwitterBot); VSetiPreview/1.0)")
	req.Header.Set("Accept-Language", "ru,en;q=0.8")
	resp, err := vsHttp.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, "", io.ErrUnexpectedEOF
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	return b, resp.Header.Get("Content-Type"), err
}

func vsParseMeta(page string) map[string]string {
	m := map[string]string{}
	head := page
	if i := strings.Index(strings.ToLower(head), "</head>"); i > 0 {
		head = head[:i]
	}
	for _, tag := range reMeta.FindAllString(head, -1) {
		var key, val string
		for _, a := range reAttr.FindAllStringSubmatch(tag, -1) {
			v := a[3] + a[4]
			switch strings.ToLower(a[1]) {
			case "property", "name", "itemprop":
				key = strings.ToLower(v)
			case "content":
				val = v
			}
		}
		if key != "" && val != "" {
			if _, ok := m[key]; !ok {
				m[key] = strings.TrimSpace(html.UnescapeString(val))
			}
		}
	}
	if t := reTitle.FindStringSubmatch(head); len(t) > 1 {
		m["<title>"] = strings.TrimSpace(html.UnescapeString(t[1]))
	}
	return m
}

func vsFirst(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}

func vsCut(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}

// vsUploadPhoto — картинку превью кладём в хранилище как обычное фото.
func vsUploadPhoto(ctx context.Context, data []byte) *mtproto.Photo {
	d := vsDfsClient()
	if d == nil || len(data) == 0 || len(data) > 512*1024*10 {
		return nil
	}
	const creator = int64(777000)
	fileId := rand.Int63()
	const part = 512 * 1024
	parts := (len(data) + part - 1) / part
	for i := 0; i < parts; i++ {
		end := (i + 1) * part
		if end > len(data) {
			end = len(data)
		}
		if _, err := d.DfsWriteFilePartData(ctx, &dfspb.TLDfsWriteFilePartData{
			Creator: creator, FileId: fileId, FilePart: int32(i), Bytes: data[i*part : end],
		}); err != nil {
			logx.WithContext(ctx).Errorf("vs webpage: write part: %v", err)
			return nil
		}
	}
	sum := md5.Sum(data)
	photo, err := d.DfsUploadPhotoFileV2(ctx, &dfspb.TLDfsUploadPhotoFileV2{
		Creator: creator,
		File: mtproto.MakeTLInputFile(&mtproto.InputFile{
			Id_INT64: fileId, Parts: int32(parts), Name: "preview.jpg", Md5Checksum: hex.EncodeToString(sum[:]),
		}).To_InputFile(),
	})
	if err != nil {
		logx.WithContext(ctx).Errorf("vs webpage: upload photo: %v", err)
		return nil
	}
	return photo
}

func Preview(ctx context.Context, rawUrl string) (*mtproto.WebPage, error) {
	u := rawUrl
	if !strings.Contains(u, "://") {
		u = "https://" + u
	}
	pu, err := url.Parse(u)
	if err != nil || pu.Host == "" || (pu.Scheme != "http" && pu.Scheme != "https") {
		return nil, mtproto.ErrMethodNotImpl
	}
	vsWpMu.Lock()
	if it, ok := vsWpCache[u]; ok && time.Since(it.at) < 6*time.Hour {
		vsWpMu.Unlock()
		if it.wp == nil {
			return nil, mtproto.ErrMethodNotImpl
		}
		return it.wp, nil
	}
	vsWpMu.Unlock()

	wp := vsBuild(ctx, u, pu)
	vsWpMu.Lock()
	vsWpCache[u] = vsWpItem{wp: wp, at: time.Now()}
	vsWpMu.Unlock()
	if wp == nil {
		return nil, mtproto.ErrMethodNotImpl
	}
	return wp, nil
}

func vsBuild(ctx context.Context, u string, pu *url.URL) *mtproto.WebPage {
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	body, ctype, err := vsGet(ctx, u, 2*1024*1024)
	if err != nil {
		logx.Infof("vs webpage: %s: %v", u, err)
		return nil
	}
	if !strings.Contains(strings.ToLower(ctype), "html") {
		return nil
	}
	m := vsParseMeta(string(body))
	title := vsFirst(m, "og:title", "twitter:title", "<title>")
	desc := vsFirst(m, "og:description", "twitter:description", "description")
	site := vsFirst(m, "og:site_name")
	if site == "" {
		site = strings.TrimPrefix(pu.Host, "www.")
	}
	img := vsFirst(m, "og:image:secure_url", "og:image", "twitter:image", "twitter:image:src", "thumbnailurl")
	if title == "" && desc == "" && img == "" {
		return nil
	}
	h := fnv.New64a()
	h.Write([]byte(u))
	id := int64(h.Sum64() & 0x7fffffffffffffff)

	wtype := "article"
	if strings.Contains(m["og:type"], "video") {
		wtype = "video"
	}
	wp := &mtproto.WebPage{
		Id:            id,
		Url_STRING:    u,
		DisplayUrl:    strings.TrimPrefix(strings.TrimPrefix(u, "https://"), "http://"),
		Hash:          int32(id),
		Type:          wrapperspb.String(wtype),
		SiteName:      wrapperspb.String(vsCut(site, 64)),
		Attributes:    []*mtproto.WebPageAttribute{},
		HasLargeMedia: wtype == "video",
	}
	if title != "" {
		wp.Title = wrapperspb.String(vsCut(title, 256))
	}
	if desc != "" {
		wp.Description = wrapperspb.String(vsCut(desc, 512))
	}
	if img != "" {
		if iu, err := pu.Parse(img); err == nil {
			if data, ct, err := vsGet(ctx, iu.String(), 5*1024*1024); err == nil && strings.HasPrefix(strings.ToLower(ct), "image/") && !bytes.HasPrefix(data, []byte("<")) {
				wp.Photo = vsUploadPhoto(ctx, data)
			}
		}
	}
	return mtproto.MakeTLWebPage(wp).To_WebPage()
}
