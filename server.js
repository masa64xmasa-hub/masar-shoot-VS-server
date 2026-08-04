// ─────────────────────────────────────────────────────────
// MASAR SHOOT VS - オンライン対戦・掲示板・個人チャット用サーバー
// 💡 追加ポイント：
//   - 掲示板（公開チャット）の履歴を保存し、後から接続した人にも配信する
//   - 個人チャット(DM)を、相手がオフラインの間はキューに保存しておき、
//     相手が次にアプリを開いた（identifyを送ってきた）瞬間にまとめて配信する
//   - データは data.json に保存し、サーバーが再起動しても残るようにする
//   - 🌍 【新規追加】ステージバトルの世界記録（最高到達ステージ・記録保持者名）を保存し、
//     新記録が出るたびに全員へ配信する
// ─────────────────────────────────────────────────────────

const WebSocket = require('ws');
const http = require('http'); // 💡 追加：/health エンドポイント（外部ping死活監視用）を提供するために必要
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 10000;
const DATA_FILE = path.join(__dirname, 'data.json');

// メッセージの保持期間（クライアント側の「1週間保存」と合わせている）
const RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
// 掲示板の履歴として保持しておく最大件数（増えすぎないように上限を設ける）
const MAX_PUBLIC_HISTORY = 500;

// ─── 永続化データの読み込み ───
// publicHistory: 掲示板に投稿された過去メッセージ（配列）
// dmQueues: { フレンドコード: [そのフレンドコード宛にまだ届けていないDMの配列] }
// stageRecord: 🌍 新規追加：ステージバトルの世界記録 { stage: 最高到達ステージ番号, name: 記録保持者名 }
function loadData() {
  try {
    const raw = fs.readFileSync(DATA_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    return {
      publicHistory: Array.isArray(parsed.publicHistory) ? parsed.publicHistory : [],
      dmQueues: parsed.dmQueues && typeof parsed.dmQueues === 'object' ? parsed.dmQueues : {},
      stageRecord: parsed.stageRecord && typeof parsed.stageRecord === 'object'
        ? parsed.stageRecord
        : { stage: 0, name: '' }
    };
  } catch (e) {
    return { publicHistory: [], dmQueues: {}, stageRecord: { stage: 0, name: '' } };
  }
}

let store = loadData();
let saveTimer = null;

// 書き込みが集中しても毎回ディスクI/Oが走らないよう、少し遅延させてまとめて保存する
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    fs.writeFile(DATA_FILE, JSON.stringify(store), (err) => {
      if (err) console.error('データ保存エラー:', err);
    });
  }, 500);
}

// 保持期限（1週間）を過ぎたデータを間引く
function pruneOldData() {
  const cutoff = Date.now() - RETENTION_MS;

  store.publicHistory = store.publicHistory
    .filter((m) => (m.sentAt ? m.sentAt * 1000 : 0) >= cutoff)
    .slice(-MAX_PUBLIC_HISTORY);

  for (const code of Object.keys(store.dmQueues)) {
    store.dmQueues[code] = (store.dmQueues[code] || []).filter(
      (m) => (m.sentAt ? m.sentAt * 1000 : 0) >= cutoff
    );
    if (store.dmQueues[code].length === 0) {
      delete store.dmQueues[code];
    }
  }
  // 💡 stageRecord は期限の概念がないため間引き対象外（世界記録は上書きされるまで残る）
}

// 1時間おきに古いデータを掃除する
setInterval(() => {
  pruneOldData();
  scheduleSave();
}, 60 * 60 * 1000);

// ─── HTTPサーバー本体（/health は死活監視用、それ以外はWebSocketにアップグレード） ───
// 💡 これによりUptimeRobotなどの外部ping（HTTPリクエスト）に応答できるようになり、
// Renderの無料プランでもスリープしにくくなる
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('OK');
    return;
  }
  res.writeHead(404);
  res.end();
});

// ─── WebSocketサーバー本体（HTTPサーバーに相乗りさせる） ───
const wss = new WebSocket.Server({ server: httpServer });

httpServer.listen(PORT, () => {
  console.log(`✅ サーバー起動：ポート ${PORT}（/health で死活監視も受け付けます）`);
});

// 接続中のソケットを friendCode で引けるように管理
// 同じ人が複数端末で開いている可能性も考え、1つのfriendCodeに複数ソケットを許容する
const socketsByFriendCode = new Map(); // friendCode -> Set<ws>

function registerSocket(ws, friendCode) {
  if (!friendCode) return;
  ws.friendCode = friendCode;
  if (!socketsByFriendCode.has(friendCode)) {
    socketsByFriendCode.set(friendCode, new Set());
  }
  socketsByFriendCode.get(friendCode).add(ws);
}

function unregisterSocket(ws) {
  if (ws.friendCode && socketsByFriendCode.has(ws.friendCode)) {
    socketsByFriendCode.get(ws.friendCode).delete(ws);
    if (socketsByFriendCode.get(ws.friendCode).size === 0) {
      socketsByFriendCode.delete(ws.friendCode);
    }
  }
}

function sendJSON(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcastToAll(obj, exceptWs) {
  const json = JSON.stringify(obj);
  wss.clients.forEach((client) => {
    if (client !== exceptWs && client.readyState === WebSocket.OPEN) {
      client.send(json);
    }
  });
}

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch (e) {
      return; // 壊れたメッセージは無視
    }

    // ─── ① identify：クライアントが自分のフレンドコードを名乗ってきた ───
    // ここで、オフラインの間に溜まっていたDMと、掲示板の履歴をまとめて送り返す
    if (data.messageType === 'identify' && data.senderFriendCode) {
      registerSocket(ws, data.senderFriendCode);

      // 💡 掲示板の履歴を配信（1週間以内の投稿すべて）
      for (const historyMsg of store.publicHistory) {
        sendJSON(ws, historyMsg);
      }

      // 🌍 新規追加：現在のステージバトル世界記録を配信する
      // （記録がまだ0件でも送っておく。クライアント側は stage > 0 の時だけカードを表示するので問題ない）
      sendJSON(ws, {
        messageType: 'stage_record_update',
        recordStage: store.stageRecord.stage,
        recordHolderName: store.stageRecord.name
      });

      // 💡 このフレンドコード宛に溜まっていたDMを配信し、届けたらキューから削除する
      const queued = store.dmQueues[data.senderFriendCode];
      if (queued && queued.length > 0) {
        for (const dm of queued) {
          sendJSON(ws, dm);
        }
        delete store.dmQueues[data.senderFriendCode];
        scheduleSave();
      }
      return; // identify自体は他のクライアントに転送しない
    }

    // ─── ② public_chat：掲示板への投稿 ───
    if (data.messageType === 'public_chat') {
      // 履歴として保存（次に誰かが開いた時に配信するため）
      store.publicHistory.push(data);
      if (store.publicHistory.length > MAX_PUBLIC_HISTORY) {
        store.publicHistory = store.publicHistory.slice(-MAX_PUBLIC_HISTORY);
      }
      scheduleSave();
      // 今つながっている全員（自分以外）にライブ配信
      broadcastToAll(data, ws);
      return;
    }

    // ─── ③ dm_chat：個人チャット ───
    if (data.messageType === 'dm_chat' && data.dmTarget) {
      const targetSockets = socketsByFriendCode.get(data.dmTarget);
      const targetIsOnline = targetSockets && targetSockets.size > 0;

      if (targetIsOnline) {
        // 相手がオンラインなら即配信
        targetSockets.forEach((sock) => sendJSON(sock, data));
      } else {
        // 💡 相手がオフラインならキューに保存しておき、次にidentifyしてきた時に配信する
        if (!store.dmQueues[data.dmTarget]) {
          store.dmQueues[data.dmTarget] = [];
        }
        store.dmQueues[data.dmTarget].push(data);
        scheduleSave();
      }
      return;
    }

    // ─── ④ フレンド申請・承認：宛先がオンラインならその場で届ける ───
    // （こちらはオフラインキューの対象外。相手がオンラインの時に再度申請してもらう想定）
    if (data.messageType === 'friend_request' || data.messageType === 'friend_accept') {
      if (data.dmTarget) {
        const targetSockets = socketsByFriendCode.get(data.dmTarget);
        if (targetSockets) {
          targetSockets.forEach((sock) => sendJSON(sock, data));
          return;
        }
      }
      // 宛先が特定できない場合は今までどおり全員に流す（フォールバック）
      broadcastToAll(data, ws);
      return;
    }

    // ─── ⑤ 🌍 新規追加：ステージバトルの新記録を報告 ───
    if (data.messageType === 'stage_record_submit') {
      const stage = data.recordStage;
      const name = (data.recordHolderName || '名無し').toString().slice(0, 20);
      // 現在の世界記録より大きいステージ番号の場合のみ更新する（不正な巻き戻しや同値更新を防ぐ）
      if (typeof stage === 'number' && stage > (store.stageRecord.stage || 0)) {
        store.stageRecord = { stage, name };
        scheduleSave();
        // 全員（送信者含む）に新しい世界記録を通知し、リアルタイムで表示を更新させる
        const updateMsg = {
          messageType: 'stage_record_update',
          recordStage: store.stageRecord.stage,
          recordHolderName: store.stageRecord.name
        };
        wss.clients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(updateMsg));
        });
      }
      return;
    }

    // ─── ⑥ それ以外（対戦の位置同期・勝敗結果など）は今までどおり全員へブロードキャスト ───
    broadcastToAll(data, ws);
  });

  ws.on('close', () => {
    unregisterSocket(ws);
  });
});
