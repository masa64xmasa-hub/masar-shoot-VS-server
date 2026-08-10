// ─────────────────────────────────────────────────────────
// MASAR SHOOT VS - オンライン対戦・掲示板・個人チャット用サーバー
// 💡 追加ポイント：
//   - 掲示板（公開チャット）の履歴を保存し、後から接続した人にも配信する
//   - 個人チャット(DM)を、相手がオフラインの間はキューに保存しておき、
//     相手が次にアプリを開いた（identifyを送ってきた）瞬間にまとめて配信する
//   - データは data.json に保存し、サーバーが再起動しても残るようにする
//   - 🏆【追加】レートランキング：rate_submit を受けたら最新レートを保存し、
//     leaderboard_request が来たら本人にだけトップ10を返す
//   - 🏔【修正】ステージ世界記録：今までは stage_record_submit をそのまま
//     転送するだけで、クライアントが待っている stage_record_update に
//     変換していなかったため機能していなかった。ここで正しく変換・保存・
//     全員への配信を行うようにした
// ─────────────────────────────────────────────────────────

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 10000;
const DATA_FILE = path.join(__dirname, 'data.json');

// メッセージの保持期間（クライアント側の「1週間保存」と合わせている）
const RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
// 掲示板の履歴として保持しておく最大件数（増えすぎないように上限を設ける）
const MAX_PUBLIC_HISTORY = 500;
// ランキングに載せる人数
const LEADERBOARD_SIZE = 10;

// ─── 永続化データの読み込み ───
// publicHistory: 掲示板に投稿された過去メッセージ（配列）
// dmQueues: { フレンドコード: [そのフレンドコード宛にまだ届けていないDMの配列] }
// leaderboard: { フレンドコード: { name, rate } } ← そのプレイヤーの最新レート
// stageRecord: { stage, name } ← ステージバトルの世界記録
function loadData() {
  try {
    const raw = fs.readFileSync(DATA_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    return {
      publicHistory: Array.isArray(parsed.publicHistory) ? parsed.publicHistory : [],
      dmQueues: parsed.dmQueues && typeof parsed.dmQueues === 'object' ? parsed.dmQueues : {},
      leaderboard: parsed.leaderboard && typeof parsed.leaderboard === 'object' ? parsed.leaderboard : {},
      stageRecord: parsed.stageRecord && typeof parsed.stageRecord === 'object'
        ? parsed.stageRecord
        : { stage: 0, name: '' }
    };
  } catch (e) {
    return { publicHistory: [], dmQueues: {}, leaderboard: {}, stageRecord: { stage: 0, name: '' } };
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
  // leaderboard・stageRecord は「現在の最新値」を保持するだけのデータなので
  // 時間経過では間引かない（次に上書きされるまでずっと有効な記録として残す）
}

// 1時間おきに古いデータを掃除する
setInterval(() => {
  pruneOldData();
  scheduleSave();
}, 60 * 60 * 1000);

// 🔧 Swift側の PlayerData は x/y/direction/isShooting/hp/characterId が必須(非Optional)なので、
// サーバーから新規に組み立てて送るメッセージには必ずこのダミー値を含めておく必要がある
function baseFields() {
  return { x: 0, y: 0, direction: 0, isShooting: false, hp: 0, characterId: '' };
}

// フレンドコードごとの最新レートから、上位 LEADERBOARD_SIZE 件を組み立てる
function buildLeaderboardEntries() {
  return Object.keys(store.leaderboard)
    .map((code) => ({
      name: store.leaderboard[code].name,
      rate: store.leaderboard[code].rate,
      friendCode: code
    }))
    .sort((a, b) => b.rate - a.rate)
    .slice(0, LEADERBOARD_SIZE);
}

// ─── WebSocketサーバー本体 ───
const wss = new WebSocket.Server({ port: PORT });
console.log(`✅ サーバー起動：ポート ${PORT}`);

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

      // 💡 このフレンドコード宛に溜まっていたDMを配信し、届けたらキューから削除する
      const queued = store.dmQueues[data.senderFriendCode];
      if (queued && queued.length > 0) {
        for (const dm of queued) {
          sendJSON(ws, dm);
        }
        delete store.dmQueues[data.senderFriendCode];
        scheduleSave();
      }

      // 🏔 現在のステージ世界記録があれば、接続直後に送っておく
      if (store.stageRecord && store.stageRecord.stage > 0) {
        sendJSON(ws, {
          ...baseFields(),
          messageType: 'stage_record_update',
          recordStage: store.stageRecord.stage,
          recordHolderName: store.stageRecord.name
        });
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

    // ─── ⑤ rate_submit：対戦後の最新レートをランキングとして保存する ───
    // フレンドコードごとに「最新の1件」だけを持つので、同じ人が何度提出しても増殖しない
    if (data.messageType === 'rate_submit' && data.senderFriendCode) {
      const rate = typeof data.submittedRate === 'number' ? data.submittedRate : null;
      if (rate !== null) {
        store.leaderboard[data.senderFriendCode] = {
          name: data.playerName || '名無し',
          rate: rate
        };
        scheduleSave();
      }
      return; // ランキング提出は他プレイヤーへ転送しない
    }

    // ─── ⑥ leaderboard_request：トップ10を要求してきた本人にだけ返す ───
    if (data.messageType === 'leaderboard_request') {
      sendJSON(ws, {
        ...baseFields(),
        messageType: 'leaderboard_update',
        leaderboardEntries: buildLeaderboardEntries()
      });
      return;
    }

    // ─── ⑦ stage_record_submit：ステージバトルの世界記録を更新する ───
    // 🔧【修正】以前はここが無く、そのまま⑧の全員ブロードキャストに落ちていたため、
    // クライアントが待っている stage_record_update に変換されず機能していなかった
    if (data.messageType === 'stage_record_submit') {
      const stage = typeof data.recordStage === 'number' ? data.recordStage : 0;
      if (stage > (store.stageRecord.stage || 0)) {
        store.stageRecord = { stage: stage, name: data.recordHolderName || '名無し' };
        scheduleSave();
      }
      // 更新の有無にかかわらず、現在の世界記録を送信者含む全員に配信して表示を最新化する
      broadcastToAll({
        ...baseFields(),
        messageType: 'stage_record_update',
        recordStage: store.stageRecord.stage,
        recordHolderName: store.stageRecord.name
      }, null);
      return;
    }

    // ─── ⑧ それ以外（対戦の位置同期・勝敗結果など）は今までどおり全員へブロードキャスト ───
    broadcastToAll(data, ws);
  });

  ws.on('close', () => {
    unregisterSocket(ws);
  });
});
