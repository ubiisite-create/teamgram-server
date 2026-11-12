#!/bin/bash
# VSети: стикеры и GIF — установка наборов, настройка bff и маршрутов session, обновление bff.
set -e
C=teamgram-server-teamgram-1
R=https://github.com/ubiisite-create/teamgram-server/releases/download/server-latest
B=/opt/vseti-backup/stk-$(date +%Y%m%d-%H%M%S)
W=$(mktemp -d)
sudo mkdir -p $B
echo "сборка: $(curl -fsSL -m 60 $R/commit.txt)"
curl -fsSL -m 600 -o $W/stickers.tar $R/stickers.tar
curl -fsSL -m 60 -o $W/stickers.sha $R/stickers.sha
[ "$(sha256sum $W/stickers.tar | cut -c1-64)" = "$(cat $W/stickers.sha)" ] || { echo BADSHA stickers; exit 1; }
curl -fsSL -m 300 -o $W/vsstickers.xz $R/vsstickers.xz
curl -fsSL -m 60 -o $W/vsstickers.sha $R/vsstickers.sha
xz -dc $W/vsstickers.xz > $W/vsstickers
[ "$(sha256sum $W/vsstickers | cut -c1-64)" = "$(cat $W/vsstickers.sha)" ] || { echo BADSHA vsstickers; exit 1; }
chmod +x $W/vsstickers
tar -C $W -xf $W/stickers.tar

# 1) наборы: файлы в S3, документы и таблицы в MySQL
sudo docker exec $C sh -c 'rm -rf /tmp/vsb && mkdir -p /tmp/vsb'
sudo docker cp $W/bundle $C:/tmp/vsb/bundle
sudo docker cp $W/vsstickers $C:/app/bin/vsstickers
sudo docker exec $C sh -c 'cd /app/bin && ./vsstickers -bundle /tmp/vsb/bundle -media ../etc2/media.yaml -dfs ../etc2/dfs.yaml' | tail -12

# 2) bff.yaml: база для стикеров (та же, что у media)
sudo docker cp $C:/app/etc2/bff.yaml $B/bff.yaml
sudo docker cp $C:/app/etc2/session.yaml $B/session.yaml
sudo cp $B/bff.yaml $W/bff.yaml; sudo chown $(id -u) $W/bff.yaml
if ! grep -q '^VsMysqlDSN:' $W/bff.yaml; then
  DSN=$(sudo docker exec $C sh -c "grep -A3 '^Mysql:' /app/etc2/media.yaml | grep 'DSN:' | head -1 | sed 's/^ *DSN: *//'")
  [ -n "$DSN" ] || { echo "нет DSN в media.yaml"; exit 1; }
  printf '\nVsMysqlDSN: "%s"\n' "$DSN" >> $W/bff.yaml
  sudo docker cp $W/bff.yaml $C:/app/etc2/bff.yaml
  echo "bff.yaml: добавлен VsMysqlDSN"
fi

# 3) session.yaml: маршруты стикеров и GIF → bff
sudo cp $B/session.yaml $W/session.yaml; sudo chown $(id -u) $W/session.yaml
sed -i -e 's|#"/mtproto.RPCStickers": "bff.bff"|"/mtproto.RPCStickers": "bff.bff"|' \
       -e 's|#"/mtproto.RPCGifs": "bff.bff"|"/mtproto.RPCGifs": "bff.bff"|' $W/session.yaml
CHANGED_SESSION=0
if ! cmp -s $W/session.yaml $B/session.yaml; then
  sudo docker cp $W/session.yaml $C:/app/etc2/session.yaml
  CHANGED_SESSION=1
fi
grep -n 'RPCStickers\|RPCGifs' $W/session.yaml

# 4) новый bff и перезапуск session
/usr/local/bin/vseti-update.sh bff
if [ $CHANGED_SESSION = 1 ]; then
  sudo docker exec $C sh -c 'pkill -x session; sleep 1'
  sudo docker exec -d $C sh -c 'cd /app/bin && exec ./session -f=../etc2/session.yaml >>/app/logs/session-vs.log 2>&1'
  sleep 3
  echo "session перезапущен"
fi
sleep 2
sudo docker exec $C sh -c 'tail -200 /app/logs/bff-vs.log | grep -i "VSети стикеры" | tail -3' || true
sudo docker exec $C ps -eo comm | tr '\n' ' '; echo
rm -rf $W
echo "STICKERS INSTALLED (откат конфигов: $B)"
