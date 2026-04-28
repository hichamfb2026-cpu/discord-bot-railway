// =====================================================================
// Discord Stream Notifications Bot
// ---------------------------------------------------------------------
// Watches YouTube channels and Kick streamers and posts a notification
// inside a Discord channel the moment a stream goes live.
// No discord.js, no third-party API keys required.
// =====================================================================

import WebSocket from "ws";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { dirname } from "node:path";

const TOKEN = process.env.DISCORD_TOKEN;
if (!TOKEN) {
  console.error("[FATAL] DISCORD_TOKEN environment variable is required");
  process.exit(1);
}

// On Railway, mount a Volume at /data and set STATE_FILE=/data/state.json
// so subscriptions survive every redeploy.
const STATE_FILE = process.env.STATE_FILE || "./state.json";

// How often (in seconds) we poll YouTube + Kick for live status.
// Default 60s is a good balance between latency and rate-limit safety.
const POLL_INTERVAL_SEC = Math.max(
  20,
  parseInt(process.env.POLL_INTERVAL_SEC || "60", 10),
);

const GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json";
const API_BASE = "https://discord.com/api/v10";

// GUILDS | GUILD_MESSAGES | MESSAGE_CONTENT
const INTENTS = (1 << 0) | (1 << 9) | (1 << 15);

// A real-browser User-Agent is required for YouTube and Kick to return
// the same HTML/JSON they serve to humans. Without it, we get truncated
// pages or 403s.
const BROWSER_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

// =====================================================================
// In-memory state
// =====================================================================

/**
 * guilds: Map<guildId, {
 *   notifyChannelId: string | null,
 *   youtube: Array<{
 *     channelId: string,         // UC...
 *     handle: string,            // "@MrBeast" or ""
 *     displayName: string,       // "MrBeast"
 *     lastVideoId: string | null // last live video id we already announced
 *   }>,
 *   kick: Array<{
 *     slug: string,              // "xqc"
 *     displayName: string,       // "xQc"
 *     lastStreamId: number | null
 *   }>
 * }>
 */
const guilds = new Map();

// guildId -> ownerId (populated from GUILD_CREATE / GUILD_UPDATE)
const guildOwners = new Map();

function getGuild(guildId) {
  if (!guilds.has(guildId)) {
    guilds.set(guildId, {
      notifyChannelId: null,
      youtube: [],
      kick: [],
    });
  }
  return guilds.get(guildId);
}

// =====================================================================
// Persistence
// =====================================================================

let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    saveState();
  }, 1500);
}

function saveState() {
  try {
    const data = { guilds: {} };
    for (const [gid, g] of guilds.entries()) {
      data.guilds[gid] = g;
    }
    const dir = dirname(STATE_FILE);
    if (dir && dir !== "." && !existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
    writeFileSync(STATE_FILE, JSON.stringify(data, null, 2));
  } catch (err) {
    console.error("[state] failed to save:", err);
  }
}

function loadState() {
  try {
    if (!existsSync(STATE_FILE)) {
      console.log(`[state] no state file at ${STATE_FILE}, starting fresh`);
      return;
    }
    const raw = readFileSync(STATE_FILE, "utf8");
    const data = JSON.parse(raw);
    if (data && data.guilds && typeof data.guilds === "object") {
      for (const [gid, g] of Object.entries(data.guilds)) {
        guilds.set(gid, {
          notifyChannelId: g.notifyChannelId || null,
          youtube: Array.isArray(g.youtube) ? g.youtube : [],
          kick: Array.isArray(g.kick) ? g.kick : [],
        });
      }
    }
    console.log(
      `[state] loaded ${guilds.size} guild(s) from ${STATE_FILE}`,
    );
  } catch (err) {
    console.error("[state] failed to load:", err);
  }
}

// =====================================================================
// Discord HTTP
// =====================================================================

async function discordRequest(method, path, body) {
  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers: {
      Authorization: `Bot ${TOKEN}`,
      "Content-Type": "application/json",
      "User-Agent": "DiscordBot (replit-stream-notifier, 1.0)",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (res.status === 429) {
    const data = await res.json().catch(() => ({}));
    const wait = (data.retry_after || 1) * 1000;
    console.warn(`[discord] rate limited, retrying in ${wait}ms`);
    await new Promise((r) => setTimeout(r, wait));
    return discordRequest(method, path, body);
  }
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Discord ${method} ${path} -> ${res.status}: ${text}`);
  }
  if (res.status === 204) return null;
  return res.json();
}

async function sendMessage(channelId, content, extra = {}) {
  return discordRequest("POST", `/channels/${channelId}/messages`, {
    content,
    ...extra,
  });
}

async function deleteMessage(channelId, messageId) {
  return discordRequest(
    "DELETE",
    `/channels/${channelId}/messages/${messageId}`,
  ).catch(() => null);
}

async function openDM(userId) {
  const ch = await discordRequest("POST", "/users/@me/channels", {
    recipient_id: userId,
  });
  return ch.id;
}

async function sendDM(userId, content, extra = {}) {
  try {
    const channelId = await openDM(userId);
    return await sendMessage(channelId, content, extra);
  } catch (err) {
    console.error(`[dm] failed to send to ${userId}:`, err.message);
    return null;
  }
}

// =====================================================================
// YouTube live detection (no API key)
// =====================================================================

/**
 * Resolve any user input (URL, @handle, or raw channel ID) to a stable
 * { channelId, handle, displayName } record by fetching the channel page
 * and parsing meta tags + embedded JSON.
 */
async function resolveYouTubeChannel(input) {
  let trimmed = String(input || "").trim();
  // Discord wraps URLs in <…> to suppress embeds — strip those.
  trimmed = trimmed.replace(/^<+/, "").replace(/>+$/, "");
  if (!trimmed) return null;

  // 1) Direct UCxxxxxxxxxxxxxxxxxxxxxx
  const idMatch = trimmed.match(/UC[a-zA-Z0-9_-]{22}/);
  let url;
  if (idMatch && trimmed.length <= 30) {
    url = `https://www.youtube.com/channel/${idMatch[0]}`;
  } else if (/^https?:\/\//i.test(trimmed)) {
    url = trimmed;
  } else if (trimmed.startsWith("@")) {
    url = `https://www.youtube.com/${trimmed}`;
  } else {
    url = `https://www.youtube.com/@${trimmed}`;
  }

  let html;
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": BROWSER_UA,
        "Accept-Language": "en-US,en;q=0.9",
      },
      redirect: "follow",
    });
    if (!res.ok) return null;
    html = await res.text();
  } catch (err) {
    console.error("[yt] resolve fetch failed:", err.message);
    return null;
  }

  // Pull channelId out of canonical / embedded JSON
  const cidMatch =
    html.match(/"channelId":"(UC[a-zA-Z0-9_-]{22})"/) ||
    html.match(/<meta itemprop="identifier" content="(UC[a-zA-Z0-9_-]{22})">/) ||
    html.match(/channel\/(UC[a-zA-Z0-9_-]{22})/);
  const channelId = cidMatch ? cidMatch[1] : null;
  if (!channelId) return null;

  // Display name
  const nameMatch =
    html.match(/<meta property="og:title" content="([^"]+)">/) ||
    html.match(/<meta name="title" content="([^"]+)">/);
  const displayName = nameMatch ? decodeHtml(nameMatch[1]) : channelId;

  // Handle
  const handleMatch = html.match(/"channelHandleText":\{"runs":\[\{"text":"(@[^"]+)"\}/) ||
    html.match(/<link rel="canonical" href="https:\/\/www\.youtube\.com\/(@[^"\/]+)"/);
  const handle = handleMatch ? handleMatch[1] : "";

  return { channelId, handle, displayName };
}

/**
 * Returns { live: true, videoId, title, thumbnail } if the channel is
 * live right now, or { live: false } otherwise.
 */
async function checkYouTubeLive(channelId) {
  const url = `https://www.youtube.com/channel/${channelId}/live`;
  let html;
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": BROWSER_UA,
        "Accept-Language": "en-US,en;q=0.9",
      },
      redirect: "follow",
    });
    if (!res.ok) return { live: false };
    html = await res.text();
  } catch (err) {
    console.error(`[yt] live check failed for ${channelId}:`, err.message);
    return { live: false };
  }

  // YouTube embeds these markers when (and only when) a livestream is
  // currently broadcasting on the /live URL.
  const isLive =
    html.includes('"isLiveBroadcast":true') ||
    html.includes('"isLive":true') ||
    /hlsManifestUrl/.test(html);

  if (!isLive) return { live: false };

  const vidMatch =
    html.match(/"videoId":"([a-zA-Z0-9_-]{11})"/) ||
    html.match(/watch\?v=([a-zA-Z0-9_-]{11})/);
  if (!vidMatch) return { live: false };
  const videoId = vidMatch[1];

  const titleMatch =
    html.match(/<meta name="title" content="([^"]+)">/) ||
    html.match(/<meta property="og:title" content="([^"]+)">/);
  const title = titleMatch ? decodeHtml(titleMatch[1]) : "بث مباشر";

  return {
    live: true,
    videoId,
    title,
    url: `https://www.youtube.com/watch?v=${videoId}`,
    thumbnail: `https://i.ytimg.com/vi/${videoId}/maxresdefault.jpg`,
  };
}

// =====================================================================
// Kick live detection
// ---------------------------------------------------------------------
// Kick's official JSON API (kick.com/api/v2/channels/<slug>) is fronted
// by Cloudflare and routinely blocks server/datacenter IPs (e.g. Railway)
// with a 403 "challenge" page. The public HTML page kick.com/<slug>
// contains the same channel + livestream data embedded as escaped JSON
// inside Next.js streaming chunks, and is far more permissive.
// We try the JSON API first (fast, clean), then fall back to scraping HTML.
// =====================================================================

const KICK_HEADERS = {
  "User-Agent": BROWSER_UA,
  Accept:
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  "Cache-Control": "no-cache",
  Pragma: "no-cache",
};

function cleanKickInput(raw) {
  let input = String(raw || "").trim();
  // Discord wraps URLs in <…> to suppress embeds — strip those.
  input = input.replace(/^<+/, "").replace(/>+$/, "");
  // <@123> Discord mention — not valid for us.
  if (/^@?!?\d+$/.test(input)) return "";
  // If user pasted full URL, take the last meaningful segment.
  const urlMatch = input.match(/kick\.com\/([a-zA-Z0-9_-]+)/i);
  if (urlMatch) input = urlMatch[1];
  // Drop leading @ and lowercase (Kick slugs are case-insensitive).
  input = input.replace(/^@/, "").toLowerCase();
  // Slug allowed characters
  if (!/^[a-z0-9_-]{2,30}$/.test(input)) return "";
  return input;
}

async function fetchKickPage(slug) {
  const url = `https://kick.com/${encodeURIComponent(slug)}`;
  const res = await fetch(url, { headers: KICK_HEADERS, redirect: "follow" });
  if (res.status === 404) return { ok: false, status: 404 };
  if (!res.ok) return { ok: false, status: res.status };
  const html = await res.text();
  return { ok: true, html };
}

async function fetchKickChannelApi(slug) {
  // Best-effort JSON API. Returns null on any non-200 (incl. CF blocks).
  try {
    const res = await fetch(
      `https://kick.com/api/v2/channels/${encodeURIComponent(slug)}`,
      {
        headers: { ...KICK_HEADERS, Accept: "application/json" },
      },
    );
    if (!res.ok) return null;
    const ct = res.headers.get("content-type") || "";
    if (!ct.includes("application/json")) return null;
    return await res.json();
  } catch {
    return null;
  }
}

/**
 * Parses the Next.js HTML page of a Kick channel and extracts:
 *   { slug, displayName, livestream, thumbnail }
 * livestream is null when offline.
 */
function parseKickHtml(html, slug) {
  // Confirm the page actually represents the requested channel.
  // Kick HTML escapes JSON as \"slug\":\"<value>\".
  const slugLit = slug.toLowerCase();
  const slugRe = new RegExp(
    `\\\\"slug\\\\":\\\\"${slugLit.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\\\"`,
    "i",
  );
  if (!slugRe.test(html)) return null;

  // Proper-cased username from the embedded user object.
  const userMatch = html.match(
    /\\"user\\":\{[^}]*?\\"username\\":\\"([^"\\]+)\\"/,
  );
  const displayName = userMatch ? decodeHtml(userMatch[1]) : slug;

  // Livestream block (only present when channel has any livestream object,
  // which is also the case when offline if there's a previous one — so we
  // must additionally check is_live:true).
  let livestream = null;
  const liveMatch = html.match(/\\"livestream\\":\{([^}]{0,2000})\}/);
  if (liveMatch) {
    const block = liveMatch[1];
    const isLiveM = block.match(/\\"is_live\\":(true|false)/);
    const idM = block.match(/\\"id\\":(\d+)/);
    if (isLiveM && isLiveM[1] === "true" && idM) {
      const titleM = block.match(/\\"session_title\\":\\"([^\\]{0,300})/);
      const viewerM = block.match(/\\"viewer_count\\":(\d+)/);
      const startM = block.match(/\\"start_time\\":\\"([^\\]+?)\\"/);
      livestream = {
        id: parseInt(idM[1], 10),
        session_title: titleM ? decodeUnicodeEscapes(titleM[1]) : "بث مباشر",
        viewer_count: viewerM ? parseInt(viewerM[1], 10) : 0,
        created_at: startM ? startM[1] : null,
      };
    }
  }

  // Thumbnail — pick the first webp under images.kick.com/video_thumbnails
  let thumbnail = null;
  const thumbMatch = html.match(
    /(https:\/\/images\.kick\.com\/video_thumbnails\/[^"\\\s]+\.webp(?:\?[^"\\\s]*)?)/,
  );
  if (thumbMatch) thumbnail = thumbMatch[1];

  return { slug: slugLit, displayName, livestream, thumbnail };
}

function decodeUnicodeEscapes(s) {
  return String(s).replace(/\\u([0-9a-fA-F]{4})/g, (_, h) =>
    String.fromCharCode(parseInt(h, 16)),
  );
}

async function resolveKickChannel(input) {
  const slug = cleanKickInput(input);
  if (!slug) return null;

  // 1) Try JSON API — fast and clean when available.
  const api = await fetchKickChannelApi(slug);
  if (api && api.slug) {
    return {
      slug: api.slug.toLowerCase(),
      displayName: api.user?.username || api.slug,
    };
  }

  // 2) Fallback: scrape public HTML page (works when Cloudflare blocks API).
  const page = await fetchKickPage(slug);
  if (!page.ok) {
    console.error(`[kick] resolve failed: HTTP ${page.status} for ${slug}`);
    return null;
  }
  const parsed = parseKickHtml(page.html, slug);
  if (!parsed) {
    console.error(`[kick] resolve: page loaded but no channel data for ${slug}`);
    return null;
  }
  return { slug: parsed.slug, displayName: parsed.displayName };
}

async function checkKickLive(slug) {
  // Try API first.
  const api = await fetchKickChannelApi(slug);
  if (api) {
    if (!api.livestream) return { live: false };
    const ls = api.livestream;
    return {
      live: true,
      streamId: ls.id,
      title: ls.session_title || "بث مباشر",
      url: `https://kick.com/${api.slug || slug}`,
      thumbnail: ls.thumbnail?.url || null,
      viewers: ls.viewer_count || 0,
      startedAt: ls.created_at || null,
    };
  }

  // Fallback: HTML page.
  const page = await fetchKickPage(slug);
  if (!page.ok) return { live: false };
  const parsed = parseKickHtml(page.html, slug);
  if (!parsed || !parsed.livestream) return { live: false };
  const ls = parsed.livestream;
  return {
    live: true,
    streamId: ls.id,
    title: ls.session_title,
    url: `https://kick.com/${slug}`,
    thumbnail: parsed.thumbnail,
    viewers: ls.viewer_count,
    startedAt: ls.created_at,
  };
}

// =====================================================================
// Notification embeds
// =====================================================================

function youtubeEmbed(sub, info) {
  return {
    embeds: [
      {
        title: `🔴 ${sub.displayName} بدأ البث الآن على YouTube!`,
        url: info.url,
        description: `**${info.title}**`,
        color: 0xff0000,
        image: info.thumbnail ? { url: info.thumbnail } : undefined,
        author: {
          name: sub.displayName,
          url: sub.handle
            ? `https://www.youtube.com/${sub.handle}`
            : `https://www.youtube.com/channel/${sub.channelId}`,
        },
        footer: { text: "YouTube Live • شاهد الآن" },
        timestamp: new Date().toISOString(),
      },
    ],
    content: `@everyone 🔴 **${sub.displayName}** يبث مباشر على يوتيوب الآن!\n${info.url}`,
    allowed_mentions: { parse: ["everyone"] },
  };
}

function kickEmbed(sub, info) {
  return {
    embeds: [
      {
        title: `🟢 ${sub.displayName} بدأ البث الآن على Kick!`,
        url: info.url,
        description: `**${info.title}**`,
        color: 0x53fc18,
        image: info.thumbnail ? { url: info.thumbnail } : undefined,
        author: {
          name: sub.displayName,
          url: `https://kick.com/${sub.slug}`,
        },
        fields: info.viewers
          ? [{ name: "👥 المشاهدون", value: String(info.viewers), inline: true }]
          : undefined,
        footer: { text: "Kick Live • شاهد الآن" },
        timestamp: info.startedAt || new Date().toISOString(),
      },
    ],
    content: `@everyone 🟢 **${sub.displayName}** يبث مباشر على كيك الآن!\n${info.url}`,
    allowed_mentions: { parse: ["everyone"] },
  };
}

// =====================================================================
// Polling loop
// =====================================================================

let pollTimer = null;

async function pollOnce() {
  for (const [guildId, g] of guilds.entries()) {
    if (!g.notifyChannelId) continue;
    if (g.youtube.length === 0 && g.kick.length === 0) continue;

    for (const sub of g.youtube) {
      try {
        const info = await checkYouTubeLive(sub.channelId);
        if (info.live && info.videoId !== sub.lastVideoId) {
          await sendMessage(
            g.notifyChannelId,
            youtubeEmbed(sub, info).content,
            { embeds: youtubeEmbed(sub, info).embeds, allowed_mentions: youtubeEmbed(sub, info).allowed_mentions },
          ).catch((err) =>
            console.error(`[yt-notify] failed for ${sub.displayName}:`, err.message),
          );
          sub.lastVideoId = info.videoId;
          scheduleSave();
          console.log(
            `[yt] announced ${sub.displayName} live (vid=${info.videoId}) in guild ${guildId}`,
          );
        }
      } catch (err) {
        console.error(`[yt] poll error ${sub.displayName}:`, err.message);
      }
      // Small jitter so we don't hammer YouTube too synchronously
      await sleep(400);
    }

    for (const sub of g.kick) {
      try {
        const info = await checkKickLive(sub.slug);
        if (info.live && info.streamId !== sub.lastStreamId) {
          await sendMessage(
            g.notifyChannelId,
            kickEmbed(sub, info).content,
            { embeds: kickEmbed(sub, info).embeds, allowed_mentions: kickEmbed(sub, info).allowed_mentions },
          ).catch((err) =>
            console.error(`[kick-notify] failed for ${sub.displayName}:`, err.message),
          );
          sub.lastStreamId = info.streamId;
          scheduleSave();
          console.log(
            `[kick] announced ${sub.displayName} live (id=${info.streamId}) in guild ${guildId}`,
          );
        }
      } catch (err) {
        console.error(`[kick] poll error ${sub.displayName}:`, err.message);
      }
      await sleep(400);
    }
  }
}

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  // Run immediately, then on a fixed interval.
  pollOnce().catch((err) => console.error("[poll] error:", err));
  pollTimer = setInterval(() => {
    pollOnce().catch((err) => console.error("[poll] error:", err));
  }, POLL_INTERVAL_SEC * 1000);
  console.log(`[poll] started, every ${POLL_INTERVAL_SEC}s`);
}

// =====================================================================
// Helpers
// =====================================================================

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function decodeHtml(s) {
  return String(s)
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function isGuildOwner(guildId, userId) {
  return guildOwners.get(guildId) === userId;
}

async function denyNotOwner(channelId, messageId) {
  const m = await sendMessage(
    channelId,
    "⛔ هذا الأمر متاح لمالك السيرفر فقط.",
  ).catch(() => null);
  if (m) setTimeout(() => deleteMessage(channelId, m.id), 6000);
  if (messageId) setTimeout(() => deleteMessage(channelId, messageId), 6000);
}

// =====================================================================
// Commands
// =====================================================================

function helpText() {
  return [
    "**🔔 بوت إشعارات البث المباشر**",
    "",
    "**اليوتيوب:**",
    "`!اضف-يوتيوب <رابط أو @handle>` — متابعة قناة يوتيوب 🔒",
    "`!احذف-يوتيوب <handle أو ID>` — إيقاف المتابعة 🔒",
    "`!قائمة-يوتيوب` — عرض كل قنوات اليوتيوب المتابَعة",
    "",
    "**كيك:**",
    "`!اضف-كيك <اسم المستخدم>` — متابعة قناة كيك 🔒",
    "`!احذف-كيك <اسم المستخدم>` — إيقاف المتابعة 🔒",
    "`!قائمة-كيك` — عرض كل قنوات الكيك المتابَعة",
    "",
    "**الإعدادات:**",
    "`!قناة-اشعارات` — تعيين هذه القناة لاستقبال الإشعارات 🔒",
    "`!حالة` — عرض القناة النشطة وعدد المتابعات",
    "`!مساعده` — عرض هذه القائمة في الخاص",
    "",
    "🔒 = متاح لمالك السيرفر فقط",
    `⏱️ يتم فحص حالة البث كل ${POLL_INTERVAL_SEC} ثانية تقريباً.`,
  ].join("\n");
}

async function handleCommand(msg) {
  const content = (msg.content || "").trim();
  if (!content.startsWith("!")) return;

  const guildId = msg.guild_id;
  const channelId = msg.channel_id;
  const userId = msg.author?.id;
  const messageId = msg.id;

  // Parse "!command rest of the args"
  const space = content.indexOf(" ");
  const cmd = (space === -1 ? content : content.slice(0, space)).toLowerCase();
  const args = space === -1 ? "" : content.slice(space + 1).trim();

  // ---- Help (everyone, DM'd) ----
  if (cmd === "!مساعده" || cmd === "!help") {
    await sendDM(userId, helpText()).catch(() => {});
    return;
  }

  // ---- Status (everyone) ----
  if (cmd === "!حالة" || cmd === "!status") {
    if (!guildId) return;
    const g = getGuild(guildId);
    const ch = g.notifyChannelId
      ? `<#${g.notifyChannelId}>`
      : "_غير محدّدة_";
    const ytList =
      g.youtube.length === 0
        ? "_لا توجد قنوات_"
        : g.youtube.map((s) => `• ${s.displayName}`).join("\n");
    const kickList =
      g.kick.length === 0
        ? "_لا توجد قنوات_"
        : g.kick.map((s) => `• ${s.displayName}`).join("\n");
    await sendMessage(channelId, "", {
      embeds: [
        {
          title: "🔔 حالة بوت الإشعارات",
          color: 0x5865f2,
          fields: [
            { name: "📢 قناة الإشعارات", value: ch, inline: false },
            { name: "▶️ يوتيوب", value: ytList, inline: true },
            { name: "🟢 كيك", value: kickList, inline: true },
          ],
          footer: { text: `الفحص كل ${POLL_INTERVAL_SEC} ثانية` },
        },
      ],
    });
    return;
  }

  // ---- List YouTube (everyone) ----
  if (cmd === "!قائمة-يوتيوب" || cmd === "!list-youtube") {
    if (!guildId) return;
    const g = getGuild(guildId);
    if (g.youtube.length === 0) {
      await sendMessage(channelId, "📭 لا توجد قنوات يوتيوب متابَعة.");
      return;
    }
    const lines = g.youtube
      .map(
        (s, i) =>
          `${i + 1}. **${s.displayName}** ${s.handle || `(\`${s.channelId}\`)`}`,
      )
      .join("\n");
    await sendMessage(channelId, `📺 **قنوات اليوتيوب المتابَعة:**\n${lines}`);
    return;
  }

  // ---- List Kick (everyone) ----
  if (cmd === "!قائمة-كيك" || cmd === "!list-kick") {
    if (!guildId) return;
    const g = getGuild(guildId);
    if (g.kick.length === 0) {
      await sendMessage(channelId, "📭 لا توجد قنوات كيك متابَعة.");
      return;
    }
    const lines = g.kick
      .map((s, i) => `${i + 1}. **${s.displayName}** (\`${s.slug}\`)`)
      .join("\n");
    await sendMessage(channelId, `🟢 **قنوات الكيك المتابَعة:**\n${lines}`);
    return;
  }

  // -------------------------------------------------------------------
  // From here on: owner-only commands
  // -------------------------------------------------------------------
  if (!guildId || !isGuildOwner(guildId, userId)) {
    if (cmd.startsWith("!اضف-") || cmd.startsWith("!احذف-") || cmd === "!قناة-اشعارات") {
      await denyNotOwner(channelId, messageId);
    }
    return;
  }

  // ---- Set notify channel ----
  if (cmd === "!قناة-اشعارات" || cmd === "!notify-here") {
    const g = getGuild(guildId);
    g.notifyChannelId = channelId;
    scheduleSave();
    await sendMessage(
      channelId,
      `✅ تم تعيين <#${channelId}> كقناة الإشعارات. سيتم نشر إشعارات البث هنا.`,
    );
    return;
  }

  // ---- Add YouTube ----
  if (cmd === "!اضف-يوتيوب" || cmd === "!add-youtube") {
    if (!args) {
      await sendMessage(
        channelId,
        "📝 الاستخدام: `!اضف-يوتيوب <رابط أو @handle أو channel ID>`",
      );
      return;
    }
    const wait = await sendMessage(channelId, "🔍 جاري التحقق من القناة...");
    const resolved = await resolveYouTubeChannel(args);
    if (wait) deleteMessage(channelId, wait.id).catch(() => {});
    if (!resolved) {
      await sendMessage(
        channelId,
        "❌ لم أستطع إيجاد هذه القناة. تأكد من الرابط أو الـ @handle.",
      );
      return;
    }
    const g = getGuild(guildId);
    if (g.youtube.some((s) => s.channelId === resolved.channelId)) {
      await sendMessage(
        channelId,
        `⚠️ قناة **${resolved.displayName}** متابَعة بالفعل.`,
      );
      return;
    }
    g.youtube.push({
      channelId: resolved.channelId,
      handle: resolved.handle,
      displayName: resolved.displayName,
      lastVideoId: null,
    });
    scheduleSave();
    await sendMessage(
      channelId,
      `✅ تمت إضافة **${resolved.displayName}** ${resolved.handle || ""}\nسأنبهك فوراً عند بدء أي بث مباشر.`,
    );
    return;
  }

  // ---- Remove YouTube ----
  if (cmd === "!احذف-يوتيوب" || cmd === "!remove-youtube") {
    if (!args) {
      await sendMessage(
        channelId,
        "📝 الاستخدام: `!احذف-يوتيوب <handle أو اسم القناة أو channel ID>`",
      );
      return;
    }
    const g = getGuild(guildId);
    const needle = args.toLowerCase().replace(/^@/, "");
    const idx = g.youtube.findIndex(
      (s) =>
        s.channelId.toLowerCase() === needle ||
        s.handle.toLowerCase().replace(/^@/, "") === needle ||
        s.displayName.toLowerCase() === needle.toLowerCase(),
    );
    if (idx === -1) {
      await sendMessage(channelId, "❌ لم أجد قناة بهذا الاسم في القائمة.");
      return;
    }
    const removed = g.youtube.splice(idx, 1)[0];
    scheduleSave();
    await sendMessage(
      channelId,
      `🗑️ تم إيقاف متابعة **${removed.displayName}**.`,
    );
    return;
  }

  // ---- Add Kick ----
  if (cmd === "!اضف-كيك" || cmd === "!add-kick") {
    if (!args) {
      await sendMessage(
        channelId,
        "📝 الاستخدام: `!اضف-كيك <اسم المستخدم أو رابط kick.com>`",
      );
      return;
    }
    const wait = await sendMessage(channelId, "🔍 جاري التحقق من القناة...");
    const resolved = await resolveKickChannel(args);
    if (wait) deleteMessage(channelId, wait.id).catch(() => {});
    if (!resolved) {
      await sendMessage(channelId, "❌ لم أستطع إيجاد هذه القناة على Kick.");
      return;
    }
    const g = getGuild(guildId);
    if (g.kick.some((s) => s.slug === resolved.slug)) {
      await sendMessage(
        channelId,
        `⚠️ قناة **${resolved.displayName}** متابَعة بالفعل.`,
      );
      return;
    }
    g.kick.push({
      slug: resolved.slug,
      displayName: resolved.displayName,
      lastStreamId: null,
    });
    scheduleSave();
    await sendMessage(
      channelId,
      `✅ تمت إضافة **${resolved.displayName}** (\`${resolved.slug}\`)\nسأنبهك فوراً عند بدء أي بث مباشر.`,
    );
    return;
  }

  // ---- Remove Kick ----
  if (cmd === "!احذف-كيك" || cmd === "!remove-kick") {
    if (!args) {
      await sendMessage(
        channelId,
        "📝 الاستخدام: `!احذف-كيك <اسم المستخدم>`",
      );
      return;
    }
    const g = getGuild(guildId);
    const needle = args.toLowerCase().replace(/^@/, "");
    const idx = g.kick.findIndex(
      (s) =>
        s.slug.toLowerCase() === needle ||
        s.displayName.toLowerCase() === needle,
    );
    if (idx === -1) {
      await sendMessage(channelId, "❌ لم أجد قناة بهذا الاسم في القائمة.");
      return;
    }
    const removed = g.kick.splice(idx, 1)[0];
    scheduleSave();
    await sendMessage(
      channelId,
      `🗑️ تم إيقاف متابعة **${removed.displayName}**.`,
    );
    return;
  }
}

// =====================================================================
// Discord Gateway (raw WebSocket)
// =====================================================================

let ws = null;
let heartbeatInterval = null;
let lastSeq = null;
let sessionId = null;
let resumeGatewayUrl = null;
let lastHeartbeatAck = true;

function clearHeartbeat() {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
  }
}

function sendIdentify() {
  ws.send(
    JSON.stringify({
      op: 2,
      d: {
        token: TOKEN,
        intents: INTENTS,
        properties: {
          os: "linux",
          browser: "stream-notifier",
          device: "stream-notifier",
        },
      },
    }),
  );
}

function sendResume() {
  ws.send(
    JSON.stringify({
      op: 6,
      d: { token: TOKEN, session_id: sessionId, seq: lastSeq },
    }),
  );
}

function startHeartbeat(intervalMs) {
  clearHeartbeat();
  // Initial random jitter as required by Discord
  const initialDelay = Math.floor(Math.random() * intervalMs);
  setTimeout(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    sendHeartbeat();
    heartbeatInterval = setInterval(() => {
      if (!lastHeartbeatAck) {
        console.warn("[gateway] missed heartbeat ACK, reconnecting");
        try {
          ws.close(4000, "no ack");
        } catch {}
        return;
      }
      sendHeartbeat();
    }, intervalMs);
  }, initialDelay);
}

function sendHeartbeat() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  lastHeartbeatAck = false;
  ws.send(JSON.stringify({ op: 1, d: lastSeq }));
}

function connect() {
  const url = resumeGatewayUrl || GATEWAY_URL;
  console.log(`[gateway] connecting to ${url}`);
  ws = new WebSocket(url);

  ws.on("open", () => {
    console.log("[gateway] socket open");
    lastHeartbeatAck = true;
  });

  ws.on("message", (raw) => {
    let payload;
    try {
      payload = JSON.parse(raw.toString());
    } catch {
      return;
    }
    const { op, d, s, t } = payload;
    if (s) lastSeq = s;

    switch (op) {
      case 10: // HELLO
        startHeartbeat(d.heartbeat_interval);
        if (sessionId && resumeGatewayUrl) {
          sendResume();
        } else {
          sendIdentify();
        }
        break;
      case 11: // Heartbeat ACK
        lastHeartbeatAck = true;
        break;
      case 1: // Server requested heartbeat
        sendHeartbeat();
        break;
      case 7: // Reconnect
        console.log("[gateway] op 7 reconnect");
        try {
          ws.close(4000, "server reconnect");
        } catch {}
        break;
      case 9: // Invalid session
        console.warn("[gateway] invalid session, re-identifying");
        sessionId = null;
        resumeGatewayUrl = null;
        setTimeout(() => sendIdentify(), 2000);
        break;
      case 0: // DISPATCH
        handleDispatch(t, d);
        break;
    }
  });

  ws.on("close", (code, reason) => {
    console.warn(`[gateway] closed (${code}) ${reason || ""}`);
    clearHeartbeat();
    // Backoff and reconnect
    setTimeout(() => connect(), 3000);
  });

  ws.on("error", (err) => {
    console.error("[gateway] error:", err.message);
  });
}

function handleDispatch(type, d) {
  switch (type) {
    case "READY":
      sessionId = d.session_id;
      resumeGatewayUrl = d.resume_gateway_url
        ? `${d.resume_gateway_url}?v=10&encoding=json`
        : null;
      console.log(
        `[gateway] READY as ${d.user.username}#${d.user.discriminator || "0"} — ${d.guilds.length} guilds`,
      );
      break;

    case "RESUMED":
      console.log("[gateway] RESUMED");
      break;

    case "GUILD_CREATE":
    case "GUILD_UPDATE":
      if (d.id && d.owner_id) guildOwners.set(d.id, d.owner_id);
      break;

    case "MESSAGE_CREATE":
      // Ignore bots / system messages
      if (!d.author || d.author.bot) return;
      if (!d.content) return;
      handleCommand(d).catch((err) =>
        console.error("[command] error:", err),
      );
      break;
  }
}

// =====================================================================
// Process lifecycle
// =====================================================================

process.on("SIGINT", () => {
  console.log("[process] SIGINT, shutting down");
  saveState();
  if (ws) ws.close(1000, "shutdown");
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log("[process] SIGTERM, shutting down");
  saveState();
  if (ws) ws.close(1000, "shutdown");
  process.exit(0);
});

process.on("unhandledRejection", (err) => {
  console.error("[process] unhandled rejection:", err);
});

// =====================================================================
// Boot
// =====================================================================

loadState();
connect();
startPolling();
