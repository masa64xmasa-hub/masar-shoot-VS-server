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
const crypto = require('crypto');

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
// claimedNames: { プレイヤー名: フレンドコード } ← 名前の重複を防ぐための登録簿
// charStats: { キャラID: { wins, losses } } ← 【追加】バランス調整用のキャラ別勝敗集計（オンライン対戦のみ）
// accounts: { ユーザー名: { salt, hash, data } } ← 【追加】ログイン式データ同期用のアカウント（dataはJSON文字列）
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
        : { stage: 0, name: '' },
      claimedNames: parsed.claimedNames && typeof parsed.claimedNames === 'object' ? parsed.claimedNames : {},
      charStats: parsed.charStats && typeof parsed.charStats === 'object' ? parsed.charStats : {},
      accounts: parsed.accounts && typeof parsed.accounts === 'object' ? parsed.accounts : {}
    };
  } catch (e) {
    return { publicHistory: [], dmQueues: {}, leaderboard: {}, stageRecord: { stage: 0, name: '' }, claimedNames: {}, charStats: {}, accounts: {} };
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

// 🔐 ログイン式データ同期用：パスワードはランダムなsaltを付けてハッシュ化してから保存する
// （平文のまま保存しない。scryptは追加パッケージ不要でNode標準に入っている）
function hashPassword(password, salt) {
  return crypto.scryptSync(password, salt, 64).toString('hex');
}

function generateSalt() {
  return crypto.randomBytes(16).toString('hex');
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

// ─── 🥊 マッチメイキング ───
// レートの近い者同士をペアリングし、「ルーム」という単位に分けて、その2人だけの通信に絞る。
// これにより複数組が同時にオンライン対戦していても、他の組の通信が混ざらなくなる。
const matchmakingQueue = []; // { ws, friendCode, name, rate, joinedAt }
const socketsByRoom = new Map(); // roomId -> Set<ws>

function generateRoomId() {
  return Math.random().toString(36).slice(2, 10);
}

function removeFromQueue(ws) {
  for (let i = matchmakingQueue.length - 1; i >= 0; i--) {
    if (matchmakingQueue[i].ws === ws) matchmakingQueue.splice(i, 1);
  }
}

function leaveRoom(ws) {
  if (ws.roomId && socketsByRoom.has(ws.roomId)) {
    socketsByRoom.get(ws.roomId).delete(ws);
    if (socketsByRoom.get(ws.roomId).size === 0) {
      socketsByRoom.delete(ws.roomId);
    }
  }
  ws.roomId = null;
}

// 待機時間が長いほど許容するレート差を広げていく（すぐ見つかる時は近いレート、なかなか見つからない時は広めに探す）
function attemptMatchmaking() {
  const now = Date.now();
  for (let i = 0; i < matchmakingQueue.length; i++) {
    const a = matchmakingQueue[i];
    if (a.ws.readyState !== WebSocket.OPEN) continue;
    const threshold = 80 + Math.floor((now - a.joinedAt) / 2000) * 60;

    let bestIndex = -1;
    let bestDiff = Infinity;
    for (let j = 0; j < matchmakingQueue.length; j++) {
      if (i === j) continue;
      const b = matchmakingQueue[j];
      if (b.ws.readyState !== WebSocket.OPEN) continue;
      const diff = Math.abs(a.rate - b.rate);
      if (diff <= threshold && diff < bestDiff) {
        bestDiff = diff;
        bestIndex = j;
      }
    }

    if (bestIndex !== -1) {
      const b = matchmakingQueue[bestIndex];
      const roomId = generateRoomId();
      removeFromQueue(a.ws);
      removeFromQueue(b.ws);
      a.ws.roomId = roomId;
      b.ws.roomId = roomId;
      socketsByRoom.set(roomId, new Set([a.ws, b.ws]));
      sendJSON(a.ws, { ...baseFields(), messageType: 'match_found', roomId, playerName: b.name, submittedRate: b.rate });
      sendJSON(b.ws, { ...baseFields(), messageType: 'match_found', roomId, playerName: a.name, submittedRate: a.rate });
      // キューの中身が変わったので最初からやり直す
      attemptMatchmaking();
      return;
    }
  }
}

// 誰かが長時間キューで待っている場合に備えて、定期的にも再挑戦する
setInterval(attemptMatchmaking, 2000);

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

    // ─── ①' claim_name：プレイヤー名の重複を防ぐための登録リクエスト ───
    // 同じ名前をすでに「別のフレンドコード」が使っていたら拒否し、空いていれば登録して確保する
    if (data.messageType === 'claim_name' && data.senderFriendCode) {
      const desired = typeof data.playerName === 'string' ? data.playerName.trim() : '';
      if (!desired) {
        sendJSON(ws, { ...baseFields(), messageType: 'name_claim_result', playerName: desired, nameAvailable: false });
        return;
      }

      const owner = store.claimedNames[desired];
      if (owner && owner !== data.senderFriendCode) {
        // 既に他の人が使用中
        sendJSON(ws, { ...baseFields(), messageType: 'name_claim_result', playerName: desired, nameAvailable: false });
        return;
      }

      // 同じ人が以前確保していた名前があれば解放してから、新しい名前を確保する
      for (const key of Object.keys(store.claimedNames)) {
        if (store.claimedNames[key] === data.senderFriendCode) delete store.claimedNames[key];
      }
      store.claimedNames[desired] = data.senderFriendCode;
      scheduleSave();

      sendJSON(ws, { ...baseFields(), messageType: 'name_claim_result', playerName: desired, nameAvailable: true });
      return;
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

    // ─── ⑥' leaderboard_reset：【開発者用】保存されているランキングデータを全消去する ───
    if (data.messageType === 'leaderboard_reset') {
      store.leaderboard = {};
      scheduleSave();
      // リセット後の（空の）ランキングを要求元にだけ返して画面をすぐ更新できるようにする
      sendJSON(ws, {
        ...baseFields(),
        messageType: 'leaderboard_update',
        leaderboardEntries: []
      });
      return;
    }

    // ─── ⑥'' char_stat_submit：【バランス調整用】オンライン対戦の勝敗をキャラ別に集計する ───
    if (data.messageType === 'char_stat_submit' && data.characterId) {
      const charId = data.characterId;
      if (!store.charStats[charId]) store.charStats[charId] = { wins: 0, losses: 0 };
      if (data.matchResult === 'win') {
        store.charStats[charId].wins += 1;
      } else {
        store.charStats[charId].losses += 1;
      }
      scheduleSave();
      return; // 集計は他プレイヤーへ転送しない
    }

    // ─── ⑥''' char_stats_request：【開発者用】キャラ別勝率の集計結果を返す ───
    if (data.messageType === 'char_stats_request') {
      const entries = Object.keys(store.charStats).map((id) => {
        const s = store.charStats[id];
        const total = s.wins + s.losses;
        return {
          characterId: id,
          wins: s.wins,
          losses: s.losses,
          winRate: total > 0 ? Math.round((s.wins / total) * 1000) / 10 : 0
        };
      }).sort((a, b) => (b.wins + b.losses) - (a.wins + a.losses));

      sendJSON(ws, {
        ...baseFields(),
        messageType: 'char_stats_update',
        charStatsJSON: JSON.stringify(entries)
      });
      return;
    }

    // ─── ⑥'''' char_stats_reset：【開発者用】キャラ別勝率の集計を全消去する ───
    if (data.messageType === 'char_stats_reset') {
      store.charStats = {};
      scheduleSave();
      sendJSON(ws, { ...baseFields(), messageType: 'char_stats_update', charStatsJSON: '[]' });
      return;
    }

    // ─── ⑥⑤ account_register：ログイン用アカウントを新規作成する ───
    if (data.messageType === 'account_register') {
      const username = (data.playerName || '').trim();
      const password = data.authPassword || '';
      if (!username || password.length < 4) {
        sendJSON(ws, { ...baseFields(), messageType: 'account_auth_result', playerName: username, authSuccess: false, authFailReason: 'invalid' });
        return;
      }
      if (store.accounts[username]) {
        sendJSON(ws, { ...baseFields(), messageType: 'account_auth_result', playerName: username, authSuccess: false, authFailReason: 'taken' });
        return;
      }
      const salt = generateSalt();
      store.accounts[username] = {
        salt,
        hash: hashPassword(password, salt),
        data: data.accountDataJSON || '{}'
      };
      scheduleSave();
      sendJSON(ws, { ...baseFields(), messageType: 'account_auth_result', playerName: username, authSuccess: true, accountDataJSON: store.accounts[username].data });
      return;
    }

    // ─── ⑥⑥ account_login：既存アカウントにログインし、保存済みデータを受け取る ───
    if (data.messageType === 'account_login') {
      const username = (data.playerName || '').trim();
      const password = data.authPassword || '';
      const account = store.accounts[username];
      if (!account || hashPassword(password, account.salt) !== account.hash) {
        sendJSON(ws, { ...baseFields(), messageType: 'account_auth_result', playerName: username, authSuccess: false, authFailReason: 'invalid_credentials' });
        return;
      }
      sendJSON(ws, { ...baseFields(), messageType: 'account_auth_result', playerName: username, authSuccess: true, accountDataJSON: account.data });
      return;
    }

    // ─── ⑥⑦ account_sync_push：ログイン中のアカウントに最新のセーブデータを反映する ───
    if (data.messageType === 'account_sync_push') {
      const username = (data.playerName || '').trim();
      const password = data.authPassword || '';
      const account = store.accounts[username];
      if (!account || hashPassword(password, account.salt) !== account.hash) return; // 認証が通らなければ黙って無視
      account.data = data.accountDataJSON || account.data;
      scheduleSave();
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

    // ─── ⑦' find_match：マッチメイキングのキューに参加する ───
    if (data.messageType === 'find_match' && data.senderFriendCode) {
      removeFromQueue(ws); // 二重登録防止
      leaveRoom(ws); // 前の対戦の部屋に入ったままなら抜けておく
      matchmakingQueue.push({
        ws,
        friendCode: data.senderFriendCode,
        name: data.playerName || '名無し',
        rate: typeof data.submittedRate === 'number' ? data.submittedRate : 1500,
        joinedAt: Date.now()
      });
      attemptMatchmaking();
      return;
    }

    // ─── ⑦'' cancel_match：マッチメイキングを取りやめる ───
    if (data.messageType === 'cancel_match') {
      removeFromQueue(ws);
      return;
    }

    // ─── ⑧ それ以外（対戦の位置同期・勝敗結果など）は、マッチング済みの「同じ部屋の相手」にだけ送る ───
    // 🔧【修正】以前は接続者全員へブロードキャストしていたため、3人以上が同時にオンライン対戦を
    // 開始すると、無関係な相手の操作情報が混ざってしまう問題があった。マッチメイキング導入に伴い、
    // ペアリングされた2人だけの部屋（ルーム）に閉じて送るように変更した
    if (ws.roomId && socketsByRoom.has(ws.roomId)) {
      const peers = socketsByRoom.get(ws.roomId);
      peers.forEach((peer) => {
        if (peer !== ws && peer.readyState === WebSocket.OPEN) peer.send(JSON.stringify(data));
      });
    }
  });

  ws.on('close', () => {
    unregisterSocket(ws);
    removeFromQueue(ws);
    leaveRoom(ws);
  });
});
