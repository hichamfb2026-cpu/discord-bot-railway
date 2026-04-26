import WebSocket from "ws";

const TOKEN = process.env.DISCORD_TOKEN;
if (!TOKEN) {
  console.error("[FATAL] DISCORD_TOKEN environment variable is required");
  process.exit(1);
}

const GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json";
const API_BASE = "https://discord.com/api/v10";

const INTENTS = (1 << 0) | (1 << 9) | (1 << 15);

const DEFAULT_EMOJIS = [
  "<:emoji_3:1498045542708805683>",
  "<:emoji_4:1498045619128766696>",
];

let emojis = [...DEFAULT_EMOJIS];

let ws = null;
let heartbeatInterval = null;
let heartbeatTimer = null;
let lastSequence = null;
let sessionId = null;
let resumeGatewayUrl = null;
let receivedAck = true;
let reconnectAttempts = 0;

const reactionQueue = [];
let processingQueue = false;
const REACTION_DELAY_MS = 450;

function parseEmoji(raw) {
  const customMatch = raw.match(/^<(a?):([a-zA-Z0-9_]+):(\d+)>$/);
  if (customMatch) {
    return { type: "custom", name: customMatch[2], id: customMatch[3], raw };
  }
  return { type: "unicode", name: raw, raw };
}

function emojiToReactionPath(emoji) {
  const parsed = parseEmoji(emoji);
  if (parsed.type === "custom") {
    return encodeURIComponent(`${parsed.name}:${parsed.id}`);
  }
  return encodeURIComponent(parsed.name);
}

async function discordREST(method, path, body = null, retries = 0) {
  const MAX_RETRIES = 3;
  const url = `${API_BASE}${path}`;
  const headers = {
    Authorization: `Bot ${TOKEN}`,
    "User-Agent": "DiscordBot (https://railway.app, 1.0.0)",
  };
  const init = { method, headers };
  if (body !== null) {
    headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(body);
  }

  try {
    const res = await fetch(url, init);

    if (res.status === 429) {
      if (retries >= MAX_RETRIES) {
        console.error(`[REST] 429 max retries exceeded for ${method} ${path}`);
        return null;
      }
      let retryAfter = 1;
      try {
        const data = await res.json();
        retryAfter = data.retry_after ?? 1;
      } catch {
        const header = res.headers.get("retry-after");
        if (header) retryAfter = parseFloat(header);
      }
      const backoff = Math.max(retryAfter * 1000, 2 ** retries * 1000);
      console.warn(
        `[REST] 429 rate limited on ${method} ${path}, retry ${retries + 1}/${MAX_RETRIES} after ${backoff}ms`,
      );
      await sleep(backoff);
      return discordREST(method, path, body, retries + 1);
    }

    if (!res.ok && res.status !== 204) {
      let errBody = "";
      try {
        errBody = await res.text();
      } catch {}
      console.error(`[REST] ${res.status} ${method} ${path}: ${errBody}`);
      return null;
    }

    if (res.status === 204) return {};
    try {
      return await res.json();
    } catch {
      return {};
    }
  } catch (err) {
    console.error(`[REST] network error on ${method} ${path}:`, err.message);
    if (retries < MAX_RETRIES) {
      const backoff = 2 ** retries * 1000;
      await sleep(backoff);
      return discordREST(method, path, body, retries + 1);
    }
    return null;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function enqueueReaction(channelId, messageId, emoji) {
  reactionQueue.push({ channelId, messageId, emoji });
  if (!processingQueue) processReactionQueue();
}

async function processReactionQueue() {
  processingQueue = true;
  while (reactionQueue.length > 0) {
    const { channelId, messageId, emoji } = reactionQueue.shift();
    const path = `/channels/${channelId}/messages/${messageId}/reactions/${emojiToReactionPath(emoji)}/@me`;
    await discordREST("PUT", path);
    await sleep(REACTION_DELAY_MS);
  }
  processingQueue = false;
}

async function sendMessage(channelId, content) {
  return discordREST("POST", `/channels/${channelId}/messages`, { content });
}

async function handleCommand(message) {
  const content = (message.content || "").trim();
  const channelId = message.channel_id;

  if (content === "!ايموجي") {
    const list =
      emojis.length > 0
        ? emojis.map((e, i) => `${i + 1}. ${e}`).join("\n")
        : "(لا توجد ايموجيات حالياً)";
    await sendMessage(channelId, `**الايموجيات الحالية:**\n${list}`);
    return true;
  }

  if (content.startsWith("!ايموجي-اضف")) {
    const args = content.slice("!ايموجي-اضف".length).trim();
    if (!args) {
      await sendMessage(channelId, "الاستخدام: `!ايموجي-اضف <emoji>`");
      return true;
    }
    const newEmojis = args.split(/\s+/).filter(Boolean);
    let added = 0;
    for (const e of newEmojis) {
      if (!emojis.includes(e)) {
        emojis.push(e);
        added++;
      }
    }
    await sendMessage(
      channelId,
      `تمت اضافة ${added} ايموجي. المجموع الآن: ${emojis.length}`,
    );
    return true;
  }

  if (content.startsWith("!ايموجي-احذف")) {
    const args = content.slice("!ايموجي-احذف".length).trim();
    if (!args) {
      await sendMessage(channelId, "الاستخدام: `!ايموجي-احذف <emoji>`");
      return true;
    }
    const toRemove = args.split(/\s+/).filter(Boolean);
    const before = emojis.length;
    emojis = emojis.filter((e) => !toRemove.includes(e));
    const removed = before - emojis.length;
    await sendMessage(
      channelId,
      `تم حذف ${removed} ايموجي. المجموع الآن: ${emojis.length}`,
    );
    return true;
  }

  if (content.startsWith("!ايموجي-غير")) {
    const args = content.slice("!ايموجي-غير".length).trim();
    if (!args) {
      await sendMessage(channelId, "الاستخدام: `!ايموجي-غير <emoji1> <emoji2> ...`");
      return true;
    }
    const next = args.split(/\s+/).filter(Boolean);
    emojis = next;
    await sendMessage(
      channelId,
      `تم تغيير الايموجيات. المجموع الآن: ${emojis.length}`,
    );
    return true;
  }

  if (content === "!ايموجي-مسح") {
    emojis = [];
    await sendMessage(channelId, "تم مسح جميع الايموجيات.");
    return true;
  }

  return false;
}

function identify() {
  ws.send(
    JSON.stringify({
      op: 2,
      d: {
        token: TOKEN,
        intents: INTENTS,
        properties: {
          os: "linux",
          browser: "raw-ws-bot",
          device: "raw-ws-bot",
        },
      },
    }),
  );
  console.log("[GATEWAY] sent IDENTIFY");
}

function resume() {
  ws.send(
    JSON.stringify({
      op: 6,
      d: {
        token: TOKEN,
        session_id: sessionId,
        seq: lastSequence,
      },
    }),
  );
  console.log("[GATEWAY] sent RESUME");
}

function startHeartbeat(interval) {
  heartbeatInterval = interval;
  receivedAck = true;
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  const jitter = Math.random();
  setTimeout(() => {
    sendHeartbeat();
    heartbeatTimer = setInterval(sendHeartbeat, interval);
  }, interval * jitter);
}

function sendHeartbeat() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  if (!receivedAck) {
    console.warn("[GATEWAY] no heartbeat ack — reconnecting");
    try {
      ws.close(4000, "no heartbeat ack");
    } catch {}
    return;
  }
  receivedAck = false;
  ws.send(JSON.stringify({ op: 1, d: lastSequence }));
}

function connect(url = GATEWAY_URL) {
  console.log(`[GATEWAY] connecting to ${url}`);
  ws = new WebSocket(url);

  ws.on("open", () => {
    console.log("[GATEWAY] connection open");
    reconnectAttempts = 0;
  });

  ws.on("message", (raw) => {
    let payload;
    try {
      payload = JSON.parse(raw.toString());
    } catch (err) {
      console.error("[GATEWAY] failed to parse message:", err.message);
      return;
    }

    const { op, d, s, t } = payload;
    if (s !== null && s !== undefined) lastSequence = s;

    switch (op) {
      case 10: {
        console.log("[GATEWAY] HELLO received");
        startHeartbeat(d.heartbeat_interval);
        if (sessionId && resumeGatewayUrl) {
          resume();
        } else {
          identify();
        }
        break;
      }
      case 11: {
        receivedAck = true;
        break;
      }
      case 1: {
        sendHeartbeat();
        break;
      }
      case 7: {
        console.log("[GATEWAY] RECONNECT requested");
        try {
          ws.close(4000, "reconnect requested");
        } catch {}
        break;
      }
      case 9: {
        console.warn("[GATEWAY] INVALID SESSION — resumable:", d);
        sessionId = null;
        resumeGatewayUrl = null;
        setTimeout(() => {
          if (ws && ws.readyState === WebSocket.OPEN) identify();
        }, 1000 + Math.random() * 4000);
        break;
      }
      case 0: {
        if (t === "READY") {
          sessionId = d.session_id;
          resumeGatewayUrl = d.resume_gateway_url;
          console.log(
            `[GATEWAY] READY as ${d.user.username}#${d.user.discriminator} (${d.user.id})`,
          );
        } else if (t === "RESUMED") {
          console.log("[GATEWAY] RESUMED");
        } else if (t === "MESSAGE_CREATE") {
          handleMessageCreate(d).catch((err) =>
            console.error("[handler] error:", err),
          );
        }
        break;
      }
      default:
        break;
    }
  });

  ws.on("close", (code, reasonBuf) => {
    const reason = reasonBuf?.toString() || "";
    console.warn(`[GATEWAY] closed code=${code} reason="${reason}"`);
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }

    if (code === 4004 || code === 4014) {
      console.error(
        `[GATEWAY] fatal close code ${code} — will not reconnect`,
      );
      process.exit(1);
    }

    const delay = Math.min(30000, 1000 * 2 ** Math.min(reconnectAttempts, 5));
    reconnectAttempts++;
    console.log(`[GATEWAY] reconnecting in ${delay}ms`);
    setTimeout(() => {
      const nextUrl = sessionId && resumeGatewayUrl ? resumeGatewayUrl : GATEWAY_URL;
      connect(nextUrl);
    }, delay);
  });

  ws.on("error", (err) => {
    console.error("[GATEWAY] error:", err.message);
  });
}

async function handleMessageCreate(message) {
  if (!message || !message.author) return;
  if (message.author.bot) return;

  const isCommand = await handleCommand(message);
  if (isCommand) return;

  for (const emoji of emojis) {
    enqueueReaction(message.channel_id, message.id, emoji);
  }
}

process.on("SIGINT", () => {
  console.log("[PROCESS] SIGINT received, shutting down");
  if (ws) ws.close(1000, "shutdown");
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log("[PROCESS] SIGTERM received, shutting down");
  if (ws) ws.close(1000, "shutdown");
  process.exit(0);
});

process.on("unhandledRejection", (err) => {
  console.error("[PROCESS] unhandled rejection:", err);
});

connect();
