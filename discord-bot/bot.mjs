import WebSocket from "ws";

const TOKEN = process.env.DISCORD_TOKEN;
if (!TOKEN) {
  console.error("[FATAL] DISCORD_TOKEN environment variable is required");
  process.exit(1);
}

const GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json";
const API_BASE = "https://discord.com/api/v10";

// GUILDS | GUILD_MESSAGES | MESSAGE_CONTENT
const INTENTS = (1 << 0) | (1 << 9) | (1 << 15);

const DEFAULT_EMOJIS = [
  "<:emoji_3:1498045542708805683>",
  "<:emoji_4:1498045619128766696>",
];

const DEFAULT_AUTO_COMMENT = "اكتبوا آراءكم عن المنشور فقط";

let emojis = [...DEFAULT_EMOJIS];

// Filter is ON by default in every channel.
// Channels listed here have the filter explicitly disabled.
const disabledFilterChannels = new Set();

// Auto-comment is ON by default in every channel using DEFAULT_AUTO_COMMENT.
// channelComments overrides the default text per channel.
// disabledCommentChannels disables the auto-comment for specific channels.
const channelComments = new Map();
const disabledCommentChannels = new Set();

// Guild metadata for permission checks
const guildOwners = new Map(); // guildId -> ownerId
const guildRoles = new Map(); // guildId -> Map<roleId, BigInt(permissions)>

// Track thread channel IDs so the media filter ignores them entirely.
// Threads are conversation spaces — users must be free to chat there.
const threadChannels = new Set();

// Per-guild "active channel" restriction. When set, the bot only operates
// inside that single channel for that guild. When unset, the bot is active
// in every channel (legacy behaviour).
const activeChannels = new Map(); // guildId -> channelId

// Per-guild rolling stats. Reset every time the weekly report is sent.
const stats = new Map(); // guildId -> stats object

function freshStats() {
  return {
    mediaPosts: 0,
    deletedMessages: 0,
    threadsCreated: 0,
    reactionsAdded: 0,
    commandsUsed: 0,
    weekStartTime: Date.now(),
  };
}

function getStats(guildId) {
  if (!stats.has(guildId)) stats.set(guildId, freshStats());
  return stats.get(guildId);
}

function bumpStat(guildId, key, n = 1) {
  if (!guildId) return;
  getStats(guildId)[key] += n;
}

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

const URL_REGEX =
  /(https?:\/\/\S+|www\.\S+|discord\.gg\/\S+|\b[a-z0-9-]+\.(?:com|net|org|io|gg|co|me|app|xyz|dev|tv|to|ru|info|biz|tech|store|online|site|club|live|news|tube|stream|link|click|page|cc|us|eu|fr|de|uk)(?:\/\S*)?)/i;

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

async function sendMessage(channelId, content, extra = {}) {
  return discordREST("POST", `/channels/${channelId}/messages`, {
    content,
    ...extra,
  });
}

async function replyToMessage(channelId, messageId, content, extra = {}) {
  return discordREST("POST", `/channels/${channelId}/messages`, {
    content,
    message_reference: {
      message_id: messageId,
      channel_id: channelId,
      fail_if_not_exists: false,
    },
    allowed_mentions: { parse: [], replied_user: false },
    ...extra,
  });
}

async function deleteMessage(channelId, messageId) {
  return discordREST(
    "DELETE",
    `/channels/${channelId}/messages/${messageId}`,
  );
}

function formatStatsReport(s, guildName = null) {
  const start = new Date(s.weekStartTime);
  const end = new Date();
  const fmt = (d) =>
    d.toLocaleString("ar-SA", {
      timeZone: "Asia/Riyadh",
      dateStyle: "medium",
      timeStyle: "short",
    });
  const headerLine = guildName
    ? `📊 **التقرير الأسبوعي — ${guildName}**`
    : "📊 **التقرير الأسبوعي للبوت**";
  return [
    headerLine,
    "━━━━━━━━━━━━━━━━━━━━━━━━━",
    `📅 **الفترة:**`,
    `   ${fmt(start)}`,
    `   ←→`,
    `   ${fmt(end)}`,
    "━━━━━━━━━━━━━━━━━━━━━━━━━",
    `📸 **منشورات صور/فيديو:** ${s.mediaPosts.toLocaleString("ar-SA")}`,
    `🗑️ **رسائل محذوفة (مخالفات):** ${s.deletedMessages.toLocaleString("ar-SA")}`,
    `💬 **مناقشات أُنشئت:** ${s.threadsCreated.toLocaleString("ar-SA")}`,
    `✨ **تفاعلات أُضيفت:** ${s.reactionsAdded.toLocaleString("ar-SA")}`,
    `⚙️ **أوامر استُخدمت:** ${s.commandsUsed.toLocaleString("ar-SA")}`,
    "━━━━━━━━━━━━━━━━━━━━━━━━━",
    "🤖 شكراً لاستخدامك البوت — نراك الأسبوع القادم!",
  ].join("\n");
}

async function sendDM(userId, content) {
  try {
    const dm = await discordREST("POST", "/users/@me/channels", {
      recipient_id: userId,
    });
    if (!dm?.id) return null;
    return await discordREST("POST", `/channels/${dm.id}/messages`, {
      content,
    });
  } catch {
    return null;
  }
}

function autoDeleteAfter(channelId, messageId, ms) {
  setTimeout(() => {
    deleteMessage(channelId, messageId).catch(() => {});
  }, ms);
}

function hasMedia(message) {
  if (!message.attachments || message.attachments.length === 0) return false;
  return message.attachments.some((a) => {
    const ct = (a.content_type || "").toLowerCase();
    if (ct.startsWith("image/") || ct.startsWith("video/")) return true;
    const filename = (a.filename || "").toLowerCase();
    return /\.(png|jpe?g|gif|webp|bmp|svg|mp4|mov|webm|mkv|avi|m4v)$/i.test(
      filename,
    );
  });
}

function containsURL(text) {
  if (!text) return false;
  return URL_REGEX.test(text);
}

function isFilterEnabled(channelId) {
  return !disabledFilterChannels.has(channelId);
}

function getEffectiveAutoComment(channelId) {
  if (disabledCommentChannels.has(channelId)) return null;
  return channelComments.get(channelId) || DEFAULT_AUTO_COMMENT;
}

function isGuildOwner(message) {
  const guildId = message.guild_id;
  if (!guildId) return false;
  const ownerId = guildOwners.get(guildId);
  return !!ownerId && ownerId === message.author.id;
}

async function denyCommand(message) {
  const m = await replyToMessage(
    message.channel_id,
    message.id,
    "❌ هذا الأمر مخصص لمالك السيرفر فقط.",
  );
  if (m?.id) autoDeleteAfter(message.channel_id, m.id, 6000);
}

async function handleCommand(message) {
  const content = (message.content || "").trim();
  const channelId = message.channel_id;

  if (content.startsWith("!")) {
    bumpStat(message.guild_id, "commandsUsed");
  }

  // ===== Stats command (owner only) =====
  if (content === "!احصائيات") {
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
    const s = getStats(message.guild_id);
    await sendMessage(channelId, formatStatsReport(s));
    return true;
  }

  // ===== Emoji commands (open to everyone) =====
  if (content === "!ايموجي") {
    const list =
      emojis.length > 0
        ? emojis.map((e, i) => `${i + 1}. ${e}`).join("\n")
        : "(لا توجد ايموجيات حالياً)";
    await sendMessage(channelId, `**الايموجيات الحالية:**\n${list}`);
    return true;
  }

  if (content.startsWith("!ايموجي-اضف")) {
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
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
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
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
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
    const args = content.slice("!ايموجي-غير".length).trim();
    if (!args) {
      await sendMessage(
        channelId,
        "الاستخدام: `!ايموجي-غير <emoji1> <emoji2> ...`",
      );
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
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
    emojis = [];
    await sendMessage(channelId, "تم مسح جميع الايموجيات.");
    return true;
  }

  // ===== Filter commands (mods only) =====
  if (content === "!فلتر-تشغيل") {
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
    disabledFilterChannels.delete(channelId);
    await sendMessage(
      channelId,
      "✅ **الفلتر مفعّل في هذه القناة**\nيُسمح فقط بنشر الصور أو الفيديوهات (مع كلمات اختيارياً). الروابط والرسائل النصية وحدها سيتم حذفها تلقائياً.",
    );
    return true;
  }

  if (content === "!فلتر-ايقاف") {
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
    disabledFilterChannels.add(channelId);
    await sendMessage(channelId, "🛑 تم إيقاف الفلتر في هذه القناة.");
    return true;
  }

  if (content === "!فلتر-حالة") {
    const filterOn = isFilterEnabled(channelId);
    const effectiveComment = getEffectiveAutoComment(channelId);
    const isCustom = channelComments.has(channelId);
    const lines = [
      `**حالة الفلتر:** ${filterOn ? "✅ مفعّل (افتراضي)" : "🛑 متوقف"}`,
      `**التعليق التلقائي:** ${
        effectiveComment === null
          ? "🛑 متوقف"
          : isCustom
            ? `✏️ مخصص:\n\`\`\`\n${effectiveComment}\n\`\`\``
            : `✅ افتراضي:\n\`\`\`\n${effectiveComment}\n\`\`\``
      }`,
    ];
    await sendMessage(channelId, lines.join("\n"));
    return true;
  }

  // ===== Auto-comment commands (mods only) =====
  if (content.startsWith("!تعليق-تعيين")) {
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
    const args = content.slice("!تعليق-تعيين".length).trim();
    if (!args) {
      await sendMessage(channelId, "الاستخدام: `!تعليق-تعيين <نص التعليق>`");
      return true;
    }
    if (args.length > 1500) {
      await sendMessage(channelId, "❌ النص طويل جداً (الحد الأقصى 1500 حرف).");
      return true;
    }
    channelComments.set(channelId, args);
    disabledCommentChannels.delete(channelId);
    await sendMessage(
      channelId,
      `✅ تم تعيين التعليق التلقائي المخصص لهذه القناة:\n\`\`\`\n${args}\n\`\`\``,
    );
    return true;
  }

  if (content === "!تعليق-افتراضي") {
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
    channelComments.delete(channelId);
    disabledCommentChannels.delete(channelId);
    await sendMessage(
      channelId,
      `✅ تم إعادة التعليق التلقائي للوضع الافتراضي:\n\`\`\`\n${DEFAULT_AUTO_COMMENT}\n\`\`\``,
    );
    return true;
  }

  if (content === "!تعليق-ايقاف") {
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
    disabledCommentChannels.add(channelId);
    await sendMessage(channelId, "🛑 تم إيقاف التعليق التلقائي في هذه القناة.");
    return true;
  }

  if (content === "!تعليق-تشغيل") {
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
    disabledCommentChannels.delete(channelId);
    const txt = channelComments.get(channelId) || DEFAULT_AUTO_COMMENT;
    await sendMessage(
      channelId,
      `✅ تم تفعيل التعليق التلقائي:\n\`\`\`\n${txt}\n\`\`\``,
    );
    return true;
  }

  if (content === "!تعليق-عرض") {
    const c = getEffectiveAutoComment(channelId);
    await sendMessage(
      channelId,
      c === null
        ? "🛑 التعليق التلقائي متوقف في هذه القناة."
        : `**التعليق التلقائي الحالي:**\n\`\`\`\n${c}\n\`\`\``,
    );
    return true;
  }

  // ===== Active channel commands (mods only) =====
  if (content === "!قناة-تعيين") {
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
    const guildId = message.guild_id;
    if (!guildId) return true;
    activeChannels.set(guildId, channelId);
    await sendMessage(
      channelId,
      `✅ **تم تفعيل البوت في هذه القناة فقط** <#${channelId}>\nسيتم تجاهل جميع القنوات الأخرى في هذا السيرفر.`,
    );
    return true;
  }

  if (content === "!قناة-الغاء") {
    if (!isGuildOwner(message)) {
      await denyCommand(message);
      return true;
    }
    const guildId = message.guild_id;
    if (!guildId) return true;
    activeChannels.delete(guildId);
    await sendMessage(
      channelId,
      "♻️ **تم إلغاء التحديد** — البوت الآن يعمل في جميع القنوات.",
    );
    return true;
  }

  if (content === "!قناة-عرض") {
    const guildId = message.guild_id;
    const active = guildId ? activeChannels.get(guildId) : null;
    await sendMessage(
      channelId,
      active
        ? `📍 **القناة النشطة:** <#${active}>`
        : "📍 لا توجد قناة محددة — البوت يعمل في جميع القنوات.",
    );
    return true;
  }

  // ===== Help =====
  if (content === "!مساعده" || content === "!help") {
    const helpText = [
        "**📖 قائمة الأوامر:**",
        "",
        "🔒 **جميع أوامر التحكم مخصصة لمالك السيرفر فقط** (لمنع التخريب).",
        "",
        "**الايموجيات:**",
        "`!ايموجي` — عرض الايموجيات الحالية (للجميع)",
        "`!ايموجي-اضف <emoji>` — اضافة ايموجي 🔒",
        "`!ايموجي-احذف <emoji>` — حذف ايموجي 🔒",
        "`!ايموجي-غير <emoji1> <emoji2> ...` — تغيير القائمة 🔒",
        "`!ايموجي-مسح` — مسح كل الايموجيات 🔒",
        "",
        "**الفلتر** (افتراضياً مفعّل في كل القنوات):",
        "`!فلتر-تشغيل` — اعادة تفعيل الفلتر 🔒",
        "`!فلتر-ايقاف` — ايقاف الفلتر في هذه القناة 🔒",
        "`!فلتر-حالة` — عرض حالة الفلتر والتعليق (للجميع)",
        "",
        "**التعليق التلقائي** — يُنشأ كـ مناقشة (Thread) داخل كل صورة/فيديو:",
        "`!تعليق-تعيين <نص>` — تعيين عنوان مخصص للمناقشة 🔒",
        "`!تعليق-افتراضي` — اعادة استخدام العنوان الافتراضي 🔒",
        "`!تعليق-ايقاف` — ايقاف انشاء المناقشات 🔒",
        "`!تعليق-تشغيل` — اعادة تشغيلها 🔒",
        "`!تعليق-عرض` — عرض العنوان الحالي (للجميع)",
        "",
        "**تحديد قناة العمل:**",
        "`!قناة-تعيين` — تشغيل البوت في هذه القناة فقط (تجاهل الباقي) 🔒",
        "`!قناة-الغاء` — إلغاء التحديد (يعمل في كل القنوات) 🔒",
        "`!قناة-عرض` — عرض القناة النشطة الحالية (للجميع)",
        "",
        "**الإحصائيات:**",
        "`!احصائيات` — عرض إحصائيات الأسبوع الجارية 🔒",
        "📅 يصلك تقرير أسبوعي تلقائي كل خميس الساعة 8:00 صباحاً (توقيت السعودية) في الخاص.",
      ].join("\n");

    // Send the menu via DM so it doesn't pollute the posts channel,
    // but DO NOT touch the user's command message.
    const dmResult = await sendDM(message.author.id, helpText);

    if (dmResult?.id) {
      const ack = await sendMessage(
        channelId,
        `<@${message.author.id}> 📬 تم إرسال قائمة الأوامر إلى رسائلك الخاصة.`,
      );
      if (ack?.id) autoDeleteAfter(channelId, ack.id, 5000);
    } else {
      // Couldn't DM (likely DMs closed) — post in channel and self-clean.
      const fallback = await sendMessage(
        channelId,
        `<@${message.author.id}> ⚠️ لم أستطع مراسلتك في الخاص. (افتح الخاص ثم أعد المحاولة)\n\n${helpText}`,
      );
      if (fallback?.id) autoDeleteAfter(channelId, fallback.id, 60000);
    }
    return true;
  }

  return false;
}

async function enforceMediaFilter(message) {
  const channelId = message.channel_id;
  // Threads are dedicated discussion spaces — never filter messages there.
  if (threadChannels.has(channelId)) return false;
  if (!isFilterEnabled(channelId)) return false;

  const content = message.content || "";
  const media = hasMedia(message);
  const linkInside = containsURL(content);

  let violation = null;
  if (linkInside) {
    violation = "links";
  } else if (!media) {
    violation = "text-only";
  }

  if (!violation) return false;

  const deleted = await deleteMessage(channelId, message.id);
  if (deleted === null) {
    console.warn(
      `[FILTER] failed to delete message ${message.id} in ${channelId} — check MANAGE_MESSAGES permission`,
    );
  } else {
    bumpStat(message.guild_id, "deletedMessages");
  }

  const userMention = `<@${message.author.id}>`;
  const reasonLine =
    violation === "links"
      ? "⛔ **لا يُسمح بإرسال الروابط في هذه القناة.**"
      : "⛔ **لا يُسمح بإرسال الرسائل النصية وحدها في هذه القناة.**";

  const warning = await sendMessage(
    channelId,
    [
      `${userMention} ${reasonLine}`,
      "🎬 يمكنك فقط نشر **صورة** أو **فيديو** (مع كلمات اختيارياً).",
    ].join("\n"),
    { allowed_mentions: { users: [message.author.id], parse: [] } },
  );
  if (warning?.id) autoDeleteAfter(channelId, warning.id, 8000);

  return true;
}

async function postAutoComment(message) {
  const channelId = message.channel_id;
  if (!hasMedia(message)) return;
  const text = getEffectiveAutoComment(channelId);
  if (!text) return;

  // Create a public thread on the message itself so the discussion lives
  // INSIDE the post, not as a separate reply that clutters the channel.
  // Discord thread name limit is 100 characters.
  const threadName =
    text.length > 100 ? text.slice(0, 97) + "..." : text;

  const thread = await discordREST(
    "POST",
    `/channels/${channelId}/messages/${message.id}/threads`,
    {
      name: threadName,
      auto_archive_duration: 1440, // 24 hours
    },
  );

  // Register the thread immediately so the filter never touches messages
  // inside it (THREAD_CREATE may arrive after the first user reply).
  if (thread?.id) {
    threadChannels.add(thread.id);
    bumpStat(message.guild_id, "threadsCreated");
    await sendMessage(thread.id, text);
  }
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

function ingestGuild(d) {
  if (!d?.id) return;
  if (d.owner_id) guildOwners.set(d.id, d.owner_id);
  const roleMap = new Map();
  for (const role of d.roles || []) {
    try {
      roleMap.set(role.id, BigInt(role.permissions ?? "0"));
    } catch {}
  }
  guildRoles.set(d.id, roleMap);

  // Discord includes active threads in GUILD_CREATE; cache their IDs so
  // we can recognize them in MESSAGE_CREATE and skip the media filter.
  for (const thread of d.threads || []) {
    if (thread?.id) threadChannels.add(thread.id);
  }
}

function isThreadType(type) {
  // 10 = ANNOUNCEMENT_THREAD, 11 = PUBLIC_THREAD, 12 = PRIVATE_THREAD
  return type === 10 || type === 11 || type === 12;
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
        } else if (t === "GUILD_CREATE" || t === "GUILD_UPDATE") {
          ingestGuild(d);
        } else if (t === "GUILD_ROLE_CREATE" || t === "GUILD_ROLE_UPDATE") {
          const roleMap = guildRoles.get(d.guild_id);
          if (roleMap && d.role) {
            try {
              roleMap.set(d.role.id, BigInt(d.role.permissions ?? "0"));
            } catch {}
          }
        } else if (t === "GUILD_ROLE_DELETE") {
          const roleMap = guildRoles.get(d.guild_id);
          if (roleMap) roleMap.delete(d.role_id);
        } else if (t === "THREAD_CREATE" || t === "THREAD_UPDATE") {
          if (d?.id) threadChannels.add(d.id);
        } else if (t === "THREAD_DELETE") {
          if (d?.id) threadChannels.delete(d.id);
        } else if (t === "THREAD_LIST_SYNC") {
          for (const thread of d.threads || []) {
            if (thread?.id) threadChannels.add(thread.id);
          }
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
      const nextUrl =
        sessionId && resumeGatewayUrl ? resumeGatewayUrl : GATEWAY_URL;
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

  // 1. Commands always have priority and bypass the filter (so admins
  //    can manage settings — including the active-channel — from anywhere).
  const isCommand = await handleCommand(message);
  if (isCommand) return;

  // 1b. If this guild has an active channel pinned, ignore everything
  //     happening outside it.
  const guildId = message.guild_id;
  if (guildId) {
    const activeId = activeChannels.get(guildId);
    if (activeId && activeId !== message.channel_id) {
      // Allow thread messages whose parent is the active channel? Threads
      // are skipped anyway by later checks; a non-active channel simply
      // means we do nothing here.
      return;
    }
  }

  // 2. Enforce media-only filter (deletes + warns if violation)
  const wasFiltered = await enforceMediaFilter(message);
  if (wasFiltered) return;

  // Threads are discussion spaces — never react or open new threads there.
  if (threadChannels.has(message.channel_id)) return;

  // Only count and decorate real media posts (filter already let it through).
  if (hasMedia(message)) {
    bumpStat(message.guild_id, "mediaPosts");
    bumpStat(message.guild_id, "reactionsAdded", emojis.length);
  }

  // 3. Add reactions to the surviving message
  for (const emoji of emojis) {
    enqueueReaction(message.channel_id, message.id, emoji);
  }

  // 4. Auto-comment under media posts
  postAutoComment(message).catch((err) =>
    console.error("[auto-comment] error:", err),
  );
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

// ===== Weekly stats report =====
// Every Thursday at 08:00 Saudi Arabia time (UTC+3 → 05:00 UTC),
// DM each guild owner an elegant report of the past week's activity,
// then reset that guild's counters.
function msUntilNextThursday8AMRiyadh() {
  const now = new Date();
  const target = new Date();
  target.setUTCHours(5, 0, 0, 0); // 05:00 UTC == 08:00 Riyadh
  // Day-of-week in UTC: 0=Sun … 4=Thu
  let daysAhead = (4 - target.getUTCDay() + 7) % 7;
  if (daysAhead === 0 && now.getTime() >= target.getTime()) {
    daysAhead = 7;
  }
  target.setUTCDate(target.getUTCDate() + daysAhead);
  return target.getTime() - now.getTime();
}

async function sendWeeklyReports() {
  console.log("[STATS] sending weekly reports");
  for (const [guildId, ownerId] of guildOwners.entries()) {
    try {
      const s = getStats(guildId);
      const report = formatStatsReport(s);
      await sendDM(ownerId, report);
      stats.set(guildId, freshStats());
    } catch (err) {
      console.error(
        `[STATS] failed to send report for guild ${guildId}:`,
        err?.message || err,
      );
    }
  }
}

function scheduleWeeklyReports() {
  const delay = msUntilNextThursday8AMRiyadh();
  const hours = (delay / 3600000).toFixed(1);
  console.log(`[STATS] next weekly report in ~${hours}h`);
  setTimeout(() => {
    sendWeeklyReports().catch((err) =>
      console.error("[STATS] weekly run error:", err),
    );
    setInterval(
      () =>
        sendWeeklyReports().catch((err) =>
          console.error("[STATS] weekly run error:", err),
        ),
      7 * 24 * 60 * 60 * 1000,
    );
  }, delay);
}

scheduleWeeklyReports();
connect();
