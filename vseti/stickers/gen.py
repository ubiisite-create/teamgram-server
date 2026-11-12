# VSети: наборы стикеров из Noto Animated Emoji (Google, CC BY 4.0).
# Скачивает Lottie, приводит к формату .tgs (512x512), пишет bundle/manifest.json и bundle/files/*.tgs
import gzip, json, os, sys, urllib.request, concurrent.futures as cf

OUT = sys.argv[1] if len(sys.argv) > 1 else 'bundle'
API = 'https://googlefonts.github.io/noto-emoji-animation/data/api.json'
LOT = 'https://fonts.gstatic.com/s/e/notoemoji/latest/{}/lottie.json'
SKIN = ('1f3fb', '1f3fc', '1f3fd', '1f3fe', '1f3ff')

def get(url):
    for _ in range(4):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return r.read()
        except Exception as e:
            err = e
    raise err

def emoji(cp):
    return ''.join(chr(int(x, 16)) for x in cp.split('_') if x != 'fe0f')

def tgs(raw):
    j = json.loads(raw)
    w, h = j.get('w', 512), j.get('h', 512)
    if (w, h) != (512, 512):
        comp = {'id': 'vs_root', 'layers': j['layers']}
        j['assets'] = j.get('assets', []) + [comp]
        j['layers'] = [{
            'ddd': 0, 'ind': 1, 'ty': 0, 'nm': 'vs', 'refId': 'vs_root', 'sr': 1,
            'ks': {'o': {'a': 0, 'k': 100}, 'r': {'a': 0, 'k': 0}, 'p': {'a': 0, 'k': [256, 256, 0]},
                   'a': {'a': 0, 'k': [w / 2, h / 2, 0]}, 's': {'a': 0, 'k': [51200 / w, 51200 / h, 100]}},
            'ao': 0, 'w': w, 'h': h, 'ip': j.get('ip', 0), 'op': j['op'], 'st': 0, 'bm': 0}]
        j['w'], j['h'] = 512, 512
    j['tgs'] = 1
    return gzip.compress(json.dumps(j, separators=(',', ':')).encode(), 9, mtime=0)

icons = [i for i in json.loads(get(API))['icons'] if not any(s in i['codepoint'] for s in SKIN)]
by = {}
for i in icons:
    by.setdefault(i['categories'][0], []).append(i['codepoint'])
smi = by.get('Smileys and emotions', [])
cut = smi.index('1f64a') + 1 if '1f64a' in smi else 130
SETS = [
    ('VSetiSmiles', 'VSети: Смайлы', smi[:cut // 2]),
    ('VSetiSmiles2', 'VSети: Смайлы 2', smi[cut // 2:cut]),
    ('VSetiHearts', 'VSети: Сердечки и жесты', smi[cut:] + by.get('People', [])),
    ('VSetiNature', 'VSети: Природа и животные', by.get('Animals and nature', [])),
    ('VSetiFood', 'VSети: Еда и напитки', by.get('Food and drink', [])),
    ('VSetiFun', 'VSети: Праздник и спорт', by.get('Activities and events', []) + by.get('Travel and places', [])),
    ('VSetiThings', 'VSети: Вещи и символы', by.get('Objects', []) + by.get('Symbols', []) + by.get('Flags', [])),
]
os.makedirs(os.path.join(OUT, 'files'), exist_ok=True)
allcp = [cp for _, _, l in SETS for cp in l]

def fetch(cp):
    try:
        data = tgs(get(LOT.format(cp)))
    except Exception as e:
        print('skip', cp, e, file=sys.stderr)
        return cp, 0
    with open(os.path.join(OUT, 'files', cp + '.tgs'), 'wb') as f:
        f.write(data)
    return cp, len(data)

sizes = {}
with cf.ThreadPoolExecutor(16) as ex:
    for cp, n in ex.map(fetch, allcp):
        sizes[cp] = n
man = {'source': 'Noto Animated Emoji (Google), CC BY 4.0', 'sets': []}
for short, title, l in SETS:
    man['sets'].append({'short_name': short, 'title': title, 'stickers': [
        {'file': 'files/%s.tgs' % cp, 'key': cp, 'emoji': emoji(cp), 'mime': 'application/x-tgsticker', 'size': sizes[cp]} for cp in [c for c in l if sizes.get(c)][:120]]})
json.dump(man, open(os.path.join(OUT, 'manifest.json'), 'w'), ensure_ascii=False, indent=1)
print('sets', [(s['short_name'], len(s['stickers'])) for s in man['sets']], 'bytes', sum(sizes.values()))
