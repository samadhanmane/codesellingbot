const fs = require("fs");
const os = require("os");
const path = require("path");
const http = require("http");
const https = require("https");
const dotenv = require("dotenv");
const express = require("express");
const { Telegraf, Markup } = require("telegraf");

const { connectDb, getCollection, COLLECTIONS, closeDb } = require("./db");

dotenv.config({ path: path.join(__dirname, ".env") });

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_CHAT_ID = Number(process.env.ADMIN_CHAT_ID);
const REQUIRED_CHANNELS_ENV = process.env.REQUIRED_CHANNEL; // can be one or multiple (comma/newline separated)
const MONGODB_URI = process.env.MONGODB_URI;

const PAYMENT_QR_PHOTO_FILE_ID = process.env.PAYMENT_QR_PHOTO_FILE_ID;
const PAYMENT_QR_IMAGE_URL = process.env.PAYMENT_QR_IMAGE_URL;
const PAYMENT_INSTRUCTIONS_TEXT =
  process.env.PAYMENT_INSTRUCTIONS_TEXT ||
  'Send payment, then press "Payment Done" and submit your UTR.';

if (!BOT_TOKEN) throw new Error("Missing BOT_TOKEN in .env");
if (!ADMIN_CHAT_ID) throw new Error("Missing ADMIN_CHAT_ID in .env");
if (!MONGODB_URI) throw new Error("Missing MONGODB_URI in .env");

const bot = new Telegraf(BOT_TOKEN);

// Render Web Service needs an HTTP server listening on PORT.
// The bot itself runs with polling; this server is only for Render uptime checks.
let dbReady = false;
function startHealthServer() {
  const app = express();
  app.get("/", (_req, res) => res.status(200).send("OK"));
  app.get("/healthz", (_req, res) => res.status(200).json({ ok: true, dbReady }));
  const port = Number(process.env.PORT) || 3000;
  app.listen(port, () => {
    console.log(`Health server listening on port ${port}`);
  });

  // Best-effort keep-alive: ping local health endpoint every 4 minutes.
  // Helps some free-tier setups that suspend on inactivity.
  setInterval(() => {
    try {
      const req = http.get(
        {
          host: "127.0.0.1",
          port,
          path: "/healthz",
          timeout: 2000,
        },
        (res) => {
          res.resume();
        }
      );
      req.on("error", () => {});
      req.on("timeout", () => req.destroy());
    } catch (_) {}
  }, 4 * 60 * 1000);
}

// Central error handler so the bot doesn't silently fail.
bot.catch((err, ctx) => {
  const extra = ctx ? JSON.stringify({ updateType: ctx.updateType, chatId: ctx.chat?.id }) : "";
  console.error("Telegraf error:", err, extra);
  if (ctx && typeof ctx.reply === "function") {
    ctx.reply("Something went wrong. Please try again.").catch(() => {});
  }
});

// Categories + routing rules
const CATEGORY_IDS = [500, 1000, 2000, 4000];
const PREFIX_TO_CATEGORY = {
  svi: 500,
  svc: 1000,
  svd: 2000,
  svh: 4000,
};

const ORDER_DURATION_MS = 10 * 60 * 1000;

// In-memory UX state. Orders are persisted in MongoDB; this just tracks which step expects a message.
// Map<userId, { orderId: string, expecting: 'utr'|'screenshot', ui: { chatId, messageId } } >
const userStates = new Map();

// In-memory admin UX state (admin configuration flows).
// Map<adminId, { expecting: 'set_channels'|'set_qr' }>
const adminStates = new Map();

// Live timer intervals per active order (QR message updates).
// Map<orderId, NodeJS.Timeout>
const orderTimers = new Map();

function stopOrderTimer(orderId) {
  const h = orderTimers.get(orderId);
  if (h) clearInterval(h);
  orderTimers.delete(orderId);
}

function clearUserStateByOrderId(orderId) {
  for (const [uid, st] of userStates.entries()) {
    if (st?.orderId === orderId) userStates.delete(uid);
  }
}

async function sendUserChatIdInfo(ctx) {
  // Loader animation: send once, then edit in-place (no ID shown).
  await ctx.reply("⏳ Preparing your access…", {
    parse_mode: "Markdown",
    disable_web_page_preview: true,
  });

  await new Promise((r) => setTimeout(r, 550));

  const finalText =
    `🚫 <b>No Replacement</b>\n` +
    `❌ <b>No Refund</b>\n` +
    `⚠️ <b>No Changes</b> (at any cost)\n\n` +
    `📌 Once payment is submitted and admin approves, the delivery is final.\n` +
    `\n` +
    `📚 <b>Educational Purpose Only</b>\n` +
    `This bot is for learning/testing. You are responsible for your own actions.`;

  try {
    // Edit the most recent message if possible; otherwise send normally.
    const msg = ctx.update?.message;
    if (msg?.message_id) {
      await ctx.editMessageText(finalText, { parse_mode: "HTML" }).catch(() => {});
      return;
    }
    await ctx.reply(finalText, { parse_mode: "HTML", disable_web_page_preview: true });
  } catch (_) {
    await ctx.reply(finalText, { parse_mode: "HTML", disable_web_page_preview: true });
  }
}

// Cache membership checks to reduce getChatMember calls.
// Map<`${userId}|${channelId}`, { ok: boolean, checkedAt: number }>
const memberCache = new Map();

function now() {
  return new Date();
}

function categoryLabel(category) {
  return `${category} off`;
}

function formatExpires(expiresAt) {
  const ms = expiresAt - Date.now();
  const m = Math.max(0, Math.ceil(ms / 60000));
  return m <= 0 ? "soon" : `${m} min`;
}

function formatRemainingMMSS(expiresAt) {
  const ms = Math.max(0, expiresAt - Date.now());
  const totalSeconds = Math.floor(ms / 1000);
  const mm = String(Math.floor(totalSeconds / 60)).padStart(2, "0");
  const ss = String(totalSeconds % 60).padStart(2, "0");
  return `${mm}:${ss}`;
}

function dotsAt(n) {
  const count = n % 4; // 0..3
  return ".".repeat(count);
}

function escapeHtml(input) {
  const s = String(input ?? "");
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function statusPretty(status) {
  const map = {
    qr_sent: "⏳ Waiting payment",
    awaiting_utr: "📥 Waiting UTR",
    awaiting_screenshot: "📸 Waiting screenshot",
    awaiting_admin: "🧑‍💼 Waiting admin",
    accepted_processing: "✅ Approved (processing)",
    fulfilled: "🎉 Delivered",
    declined: "❌ Declined",
    out_of_stock: "🚫 Out of stock",
    cancelled: "🚫 Cancelled",
    expired: "⌛ Expired",
  };
  return map[status] || status;
}

function generateOrderId() {
  // Human-readable + unique enough for this use case.
  // Example: ORD-20260330-153045-7f3a2b
  const dt = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  const stamp = `${dt.getFullYear()}${pad(dt.getMonth() + 1)}${pad(dt.getDate())}-${pad(
    dt.getHours()
  )}${pad(dt.getMinutes())}${pad(dt.getSeconds())}`;
  const rand = Math.random().toString(16).slice(2, 8);
  return `ORD-${stamp}-${rand}`.toUpperCase();
}

function isTerminalOrderStatus(status) {
  return [
    "fulfilled",
    "declined",
    "accepted_processing",
    "cancelled",
    "expired",
    "out_of_stock",
  ].includes(status);
}

function normalizeChannelChatId(input) {
  let s = String(input ?? "").trim();
  if (!s) return null;

  // Accept full links like https://t.me/username
  if (s.includes("t.me/")) {
    const m = s.match(/t\.me\/([^\/\?\s]+)/i);
    if (m?.[1]) s = m[1];
  }

  // Remove @ if present
  if (s.startsWith("@")) s = s.slice(1);

  // Invite links (t.me/+...) can't be validated via getChatMember reliably
  if (s.startsWith("+")) return null;

  // If it's a numeric channel id
  if (/^-?\d+$/.test(s)) return s;

  if (!s) return null;
  return `@${s}`;
}

function channelChatIdToJoinUrl(chatId) {
  if (!chatId) return null;
  if (/^-?\d+$/.test(chatId)) return null;
  if (chatId.startsWith("@")) return `https://t.me/${chatId.slice(1)}`;
  return null;
}

function parseChannelsText(text) {
  if (!text) return [];
  // Split by whitespace/comma/semicolon/newline.
  const parts = String(text)
    .split(/[\s,;]+/g)
    .map((x) => x.trim())
    .filter(Boolean);

  const out = [];
  for (const p of parts) {
    const norm = normalizeChannelChatId(p);
    if (norm) out.push(norm);
  }
  return [...new Set(out)];
}

async function getRequiredChannels() {
  const configCol = getCollection(COLLECTIONS.CONFIG);
  const cfg = await configCol.findOne({ _id: "global" });

  if (cfg?.requiredChannels?.length) return cfg.requiredChannels;

  // Fallback to env (comma/newline separated)
  const envText = REQUIRED_CHANNELS_ENV || "";
  const channels = parseChannelsText(envText);
  return channels;
}

async function isUserInRequiredChannels(userId) {
  const requiredChannels = await getRequiredChannels();
  if (!requiredChannels.length) return false;

  // User must be in ALL required channels.
  for (const channelId of requiredChannels) {
    const cacheKey = `${userId}|${channelId}`;
    const cached = memberCache.get(cacheKey);
    if (cached && Date.now() - cached.checkedAt < 60_000) {
      if (!cached.ok) return false;
      continue;
    }

    let ok = false;
    try {
      const res = await bot.telegram.getChatMember(channelId, userId);
      ok = ["creator", "administrator", "member"].includes(res.status);
    } catch (e) {
      ok = false;
    }

    memberCache.set(cacheKey, { ok, checkedAt: Date.now() });
    if (!ok) return false;
  }
  return true;
}

async function getMissingRequiredChannels(userId) {
  const requiredChannels = await getRequiredChannels();
  const missing = [];

  for (const channelId of requiredChannels) {
    const cacheKey = `${userId}|${channelId}`;
    const cached = memberCache.get(cacheKey);
    if (cached && Date.now() - cached.checkedAt < 60_000) {
      if (!cached.ok) missing.push(channelId);
      continue;
    }

    let ok = false;
    try {
      const res = await bot.telegram.getChatMember(channelId, userId);
      ok = ["creator", "administrator", "member"].includes(res.status);
    } catch (e) {
      ok = false;
    }

    memberCache.set(cacheKey, { ok, checkedAt: Date.now() });
    if (!ok) missing.push(channelId);
  }

  return missing;
}

function buildJoinKeyboard(channels) {
  const urlButtons = channels
    .map((ch) => {
      const url = channelChatIdToJoinUrl(ch);
      if (!url) return null;
      const name = ch.startsWith("@") ? ch.slice(1) : ch;
      const label = `🔗 Join ${name}`;
      return Markup.button.url(label, url);
    })
    .filter(Boolean);

  const rows = [];
  // 2 buttons per row if possible
  for (let i = 0; i < urlButtons.length; i += 2) {
    rows.push(urlButtons.slice(i, i + 2));
  }

  // "I've joined" checks again whether the user is in ALL channels.
  rows.push([Markup.button.callback("✅ I Joined", "join:check")]);
  return Markup.inlineKeyboard(rows);
}

async function getGlobalConfig() {
  const configCol = getCollection(COLLECTIONS.CONFIG);
  const cfg = await configCol.findOne({ _id: "global" });
  return (
    cfg || {
      _id: "global",
      requiredChannels: [],
      paymentQr: null,
    }
  );
}

async function setRequiredChannels(channels) {
  const configCol = getCollection(COLLECTIONS.CONFIG);
  await configCol.updateOne(
    { _id: "global" },
    { $set: { requiredChannels: channels, updatedAt: now() } },
    { upsert: true }
  );
}

async function setPaymentQrFileId(photoFileId) {
  const configCol = getCollection(COLLECTIONS.CONFIG);
  await configCol.updateOne(
    { _id: "global" },
    { $set: { paymentQr: { fileId: photoFileId, source: "file_id" }, updatedAt: now() } },
    { upsert: true }
  );
}

async function getPaymentQr() {
  const cfg = await getGlobalConfig();
  const fileId = cfg?.paymentQr?.fileId;
  if (fileId) return { type: "file_id", fileId };

  // No fallback: admin must upload the QR in the bot.
  return null;
}

async function getCategoryPrices() {
  const cfg = await getGlobalConfig();
  const raw = cfg?.categoryPrices || {};

  const prices = {};
  for (const [k, v] of Object.entries(raw)) {
    const cat = Number(k);
    if (!CATEGORY_IDS.includes(cat)) continue;
    const num = Number(v);
    if (!Number.isFinite(num) || num <= 0) continue;
    prices[cat] = num;
  }
  return prices;
}

async function setCategoryPrices(pricesByCategory) {
  const configCol = getCollection(COLLECTIONS.CONFIG);

  const normalized = {};
  for (const cat of CATEGORY_IDS) {
    const v = pricesByCategory[cat];
    if (!Number.isFinite(Number(v)) || Number(v) <= 0) {
      throw new Error(`Invalid price for ${cat}`);
    }
    normalized[String(cat)] = Number(v);
  }

  await configCol.updateOne(
    { _id: "global" },
    { $set: { categoryPrices: normalized, updatedAt: now() } },
    { upsert: true }
  );
}

function parsePricesText(text) {
  // Accept formats like:
  // 500=10
  // 500: 10
  // 500 10
  const lines = String(text || "")
    .split(/\r?\n/g)
    .map((x) => x.trim())
    .filter(Boolean);

  const out = {};
  for (const line of lines) {
    const m = line.match(/^(\d{3,4})\s*[:= ]\s*(\d+(?:\.\d+)?)\s*$/);
    if (!m) continue;
    const cat = Number(m[1]);
    const price = Number(m[2]);
    if (CATEGORY_IDS.includes(cat) && Number.isFinite(price)) out[cat] = price;
  }

  // Also support a single-line "500 1000 2000 4000" style without prices? Not supported.
  return out;
}

async function editOrSend(ctx, ui, text, extra = {}) {
  const { chatId, messageId, isPhoto } = ui || {};
  if (messageId) {
    try {
      if (isPhoto) {
        await bot.telegram.editMessageCaption(chatId, messageId, undefined, text, extra);
      } else {
        await bot.telegram.editMessageText(chatId, messageId, undefined, text, extra);
      }
      return messageId;
    } catch (_) {
      // Message might be too old or already edited. Fall back to sending a new one.
    }
  }
  const sent = await bot.telegram.sendMessage(chatId, text, extra);
  return sent.message_id;
}

async function getStocks() {
  const codes = getCollection(COLLECTIONS.CODES);
  const counts = await Promise.all(
    CATEGORY_IDS.map(async (cat) => {
      const count = await codes.countDocuments({ category: cat, available: true });
      return { category: cat, stock: count };
    })
  );
  const map = new Map(counts.map((x) => [x.category, x.stock]));
  return map;
}

function buildCategoryKeyboard(stocks) {
  const mkBtn = (cat) => {
    const stock = stocks.get(cat) ?? 0;
    return Markup.button.callback(`${cat} (stock ${stock})`, `buy:${cat}`);
  };

  return Markup.inlineKeyboard([
    [mkBtn(500), mkBtn(1000)],
    [mkBtn(2000), mkBtn(4000)],
    [Markup.button.callback("📊 Stocks", "menu:stocks"), Markup.button.callback("📄 History", "history:open")],
  ]);
}

async function showMainMenu(ctx, editMessage = false, ui = {}) {
  const userId = ctx.from.id;
  const stocks = await getStocks();
  const text =
    `🛍️ <b>Code Store</b>\n\n` +
    `📦 Choose a category below. Stock is shown on each button.\n\n` +
    CATEGORY_IDS.map((c) => `• ${categoryLabel(c)}`).join("\n");

  const keyboard = buildCategoryKeyboard(stocks).reply_markup;
  const extra = { reply_markup: keyboard, disable_web_page_preview: true, parse_mode: "HTML" };

  if (editMessage && ui.chatId && ui.messageId) {
    await editOrSend(ctx, ui, text, extra);
  } else {
    const sent = await ctx.reply(text, extra);
    return { chatId: sent.chat.id, messageId: sent.message_id };
  }
}

async function sendQrFlow(ctx, order) {
  const chatId = ctx.chat.id;
  const expiresText = formatRemainingMMSS(order.expiresAt);

  const orderId = escapeHtml(order.orderId);
  const category = escapeHtml(order.category);
  const qty = escapeHtml(order.quantity || 1);
  const total = escapeHtml(order.totalAmount || 0);

  const caption =
    `🧾 <b>ORDER</b>\n` +
    `━━━━━━━━━━━━━━\n` +
    `🆔 <b>Order ID:</b> <code>${orderId}</code>\n` +
    `💠 <b>Category:</b> <code>${category}</code> off\n` +
    `🧮 <b>Qty:</b> <code>${qty}</code>\n` +
    `💳 <b>Total:</b> <code>${total}</code>\n` +
    `⏱️ <b>Time left:</b> <code>${escapeHtml(expiresText)}</code>\n` +
    `━━━━━━━━━━━━━━\n\n` +
    `🟨 <b>NEXT STEP</b>\n` +
    `➡️ <b>Send payment, then tap:</b> <b>Payment Done ✅</b>\n\n` +
    `ℹ️ ${escapeHtml(PAYMENT_INSTRUCTIONS_TEXT)}`;

  const keyboard = Markup.inlineKeyboard([
    [
      Markup.button.callback("Payment Done ✅", `paid:${order.orderId}`),
      Markup.button.callback("Cancel ❌", `cancel:${order.orderId}`),
    ],
  ]);

  const qr = await getPaymentQr();
  if (qr?.type === "file_id") {
    const sent = await ctx.replyWithPhoto(qr.fileId, {
      caption,
      parse_mode: "HTML",
      ...keyboard,
    });
    return { chatId, messageId: sent.message_id, isPhoto: true };
  }

  if (qr?.type === "image_url") {
    const sent = await ctx.replyWithPhoto(qr.url, {
      caption,
      parse_mode: "HTML",
      ...keyboard,
    });
    return { chatId, messageId: sent.message_id, isPhoto: true };
  }

  // Fallback: text only
  const sent = await ctx.reply(caption, { parse_mode: "HTML", ...keyboard });
  return { chatId, messageId: sent.message_id };
}

async function updateQrMessageForOrder(orderId, forced = false) {
  const order = await getCollection(COLLECTIONS.ORDERS).findOne({ orderId });
  if (!order) return;

  if (order.status === "fulfilled" || order.status === "declined" || order.status === "cancelled" || order.status === "expired" || order.status === "out_of_stock") {
    stopOrderTimer(orderId);
  }

  const remaining = formatRemainingMMSS(order.expiresAt);
  const dots = forced ? "" : dotsAt((Date.now() / 2000) | 0);

  const qty = order.quantity || 1;
  const total = order.totalAmount ?? 0;

  const orderIdHtml = escapeHtml(order.orderId);
  const categoryHtml = escapeHtml(order.category);
  const qtyHtml = escapeHtml(qty);
  const totalHtml = escapeHtml(total);
  const remainingHtml = escapeHtml(remaining);

  let statusTitle = "";
  let statusBody = "";

  if (order.status === "qr_sent") {
    statusTitle = `🟨 <b>NEXT STEP</b>`;
    statusBody = `➡️ <b>Make payment</b>, then tap <b>Payment Done ✅</b>${escapeHtml(dots)}`;
  } else if (order.status === "awaiting_utr") {
    statusTitle = `🟦 <b>ACTION REQUIRED</b>`;
    statusBody = `📥 <b>SEND UTR</b> (payment reference) as a text message${escapeHtml(dots)}`;
  } else if (order.status === "awaiting_screenshot") {
    statusTitle = `🟦 <b>ACTION REQUIRED</b>`;
    statusBody = `📸 <b>SEND PAYMENT SCREENSHOT</b> as a photo${escapeHtml(dots)}`;
  } else if (order.status === "awaiting_admin") {
    statusTitle = `🟩 <b>SUBMITTED</b>`;
    statusBody = `✅ <b>Transaction completed from your side.</b>\n⏳ Waiting for admin approval${escapeHtml(dots)}`;
  } else if (order.status === "accepted_processing") {
    statusTitle = `🟩 <b>APPROVED</b>`;
    statusBody = `✅ Approved. Preparing your code(s)${escapeHtml(dots)}`;
  } else if (order.status === "fulfilled") {
    statusTitle = `🟩 <b>DONE</b>`;
    statusBody = `🎉 Payment accepted. Code(s) delivered.`;
  } else if (order.status === "declined") {
    statusTitle = `🟥 <b>DECLINED</b>`;
    statusBody = `❌ Payment declined by admin.`;
  } else if (order.status === "cancelled") {
    statusTitle = `🟥 <b>CANCELLED</b>`;
    statusBody = `🚫 Order cancelled.`;
  } else if (order.status === "expired") {
    statusTitle = `🟥 <b>EXPIRED</b>`;
    statusBody = `⌛ Order expired.`;
  } else if (order.status === "out_of_stock") {
    statusTitle = `🟥 <b>FAILED</b>`;
    statusBody = `🚫 Out of stock for requested quantity.`;
  } else {
    statusTitle = `📌 <b>Status</b>`;
    statusBody = escapeHtml(order.status);
  }

  const caption =
    `🧾 <b>ORDER</b>\n` +
    `━━━━━━━━━━━━━━\n` +
    `🆔 <b>Order ID:</b> <code>${orderIdHtml}</code>\n` +
    `💠 <b>Category:</b> <code>${categoryHtml}</code> off\n` +
    `🧮 <b>Qty:</b> <code>${qtyHtml}</code>\n` +
    `💳 <b>Total:</b> <code>${totalHtml}</code>\n` +
    `⏱️ <b>Time left:</b> <code>${remainingHtml}</code>\n` +
    `━━━━━━━━━━━━━━\n\n` +
    `${statusTitle}\n` +
    `${statusBody}`;

  // Keyboard depends on the stage
  let keyboard = undefined;
  if (order.status === "qr_sent" || order.status === "awaiting_utr" || order.status === "awaiting_screenshot") {
    keyboard = Markup.inlineKeyboard([
      [
        Markup.button.callback("Cancel ❌", `cancel:${order.orderId}`),
      ],
    ]).reply_markup;
  }
  if (order.status === "qr_sent") {
    keyboard = Markup.inlineKeyboard([
      [
        Markup.button.callback("Payment Done ✅", `paid:${order.orderId}`),
        Markup.button.callback("Cancel ❌", `cancel:${order.orderId}`),
      ],
    ]).reply_markup;
  }

  const ui = {
    chatId: order.qrMessageChatId || order.userChatId,
    messageId: order.qrMessageId,
    isPhoto: order.qrMessageIsPhoto,
  };

  if (!ui.messageId || !ui.chatId) return;

  const extra = keyboard
    ? { reply_markup: keyboard, disable_web_page_preview: true, parse_mode: "HTML" }
    : { disable_web_page_preview: true, parse_mode: "HTML" };

  if (ui.isPhoto) {
    try {
      await bot.telegram.editMessageCaption(ui.chatId, ui.messageId, undefined, caption, extra);
    } catch (_) {}
  } else {
    try {
      await bot.telegram.editMessageText(ui.chatId, ui.messageId, undefined, caption, extra);
    } catch (_) {}
  }
}

async function startOrderLiveTimer(orderId) {
  // Avoid double timers.
  if (orderTimers.has(orderId)) return;

  const tick = async () => {
    try {
      await updateQrMessageForOrder(orderId);
      const order = await getCollection(COLLECTIONS.ORDERS).findOne({ orderId });
      if (!order || isTerminalOrderStatus(order.status)) stopOrderTimer(orderId);
    } catch (_) {}
  };

  // Update quickly for the first render, then keep steady.
  await tick();
  const h = setInterval(tick, 2000);
  orderTimers.set(orderId, h);
}

async function ensureFreshOrder(orderId) {
  const orders = getCollection(COLLECTIONS.ORDERS);
  const order = await orders.findOne({ orderId });
  if (!order) throw new Error("Order not found");
  if (isTerminalOrderStatus(order.status)) throw new Error(`Order is ${order.status}`);
  if (order.expiresAt && order.expiresAt <= new Date())
    throw new Error("Order expired");
  return order;
}

async function updateOrder(orderId, patch) {
  const orders = getCollection(COLLECTIONS.ORDERS);
  await orders.updateOne({ orderId }, { $set: patch });
}

async function atomicSetOrderStatus(orderId, currentStatus, nextStatus, extraSet = {}) {
  const orders = getCollection(COLLECTIONS.ORDERS);
  const res = await orders.updateOne(
    { orderId, status: currentStatus },
    { $set: { ...extraSet, status: nextStatus, decisionAt: now() } }
  );
  return res.matchedCount > 0;
}

async function claimNCodes(category, orderId, qty) {
  const codes = getCollection(COLLECTIONS.CODES);
  const claimed = [];

  for (let i = 0; i < qty; i++) {
    const res = await codes.findOneAndUpdate(
      { category, available: true },
      {
        $set: {
          available: false,
          soldOrderId: orderId,
          soldAt: now(),
        },
      },
      { sort: { _id: 1 }, returnDocument: "after" }
    );

    if (!res.value) break;
    claimed.push(res.value);
  }

  return claimed;
}

async function releaseCodesByIds(codeIds) {
  if (!codeIds?.length) return;
  const codes = getCollection(COLLECTIONS.CODES);
  await codes.updateMany(
    { _id: { $in: codeIds } },
    { $set: { available: true, soldOrderId: null, soldAt: null } }
  );
}

function parseCsvCodes(csvText) {
  // Admin upload format (required): one code per line.
  // We keep it strict and uppercase codes.
  const lines = String(csvText || "")
    .split(/\r?\n/g)
    .map((l) => l.trim())
    .filter(Boolean);

  const codes = [];
  for (const line of lines) {
    // If someone accidentally sends "CODE,..." keep the first token.
    const token = line.split(/[,\s]+/g)[0];
    if (!token) continue;
    codes.push(String(token).toUpperCase());
  }

  return [...new Set(codes)];
}

async function importCodesToDb(rawCodes, ctx) {
  const codesCol = getCollection(COLLECTIONS.CODES);

  const normalized = (rawCodes || []).map((c) => String(c || "").trim().toUpperCase()).filter(Boolean);
  const totalLines = normalized.length;
  const uniqueCodes = [...new Set(normalized)];
  const duplicateInUpload = totalLines - uniqueCodes.length;

  // Find codes already present in DB
  const existing = await codesCol
    .find({ code: { $in: uniqueCodes } }, { projection: { code: 1 } })
    .toArray();
  const existingSet = new Set(existing.map((d) => d.code));

  const newOnes = uniqueCodes.filter((c) => !existingSet.has(c));
  const alreadyPresent = uniqueCodes.filter((c) => existingSet.has(c));

  // Route + validate by prefix
  const toInsert = [];
  const unknownPrefix = [];
  for (const code of newOnes) {
    const lower = code.toLowerCase();
    const prefix = Object.keys(PREFIX_TO_CATEGORY).find((p) => lower.startsWith(p));
    if (!prefix) {
      unknownPrefix.push(code);
      continue;
    }
    const category = PREFIX_TO_CATEGORY[prefix];
    toInsert.push({
      code,
      category,
      available: true,
      soldOrderId: null,
      soldAt: null,
    });
  }

  let inserted = 0;
  if (toInsert.length) {
    const operations = toInsert.map((doc) => ({
      insertOne: { document: doc },
    }));

    // ordered:false so one duplicate doesn't kill batch (though we pre-filtered).
    const res = await codesCol.bulkWrite(operations, { ordered: false });
    inserted = res.insertedCount || 0;
  }

  const sample = (arr) => arr.slice(0, 10).join(", ");

  await ctx.reply(
    `✅ Code import summary\n\n` +
      `📄 Lines received: ${totalLines}\n` +
      `🔁 Duplicate lines in upload: ${duplicateInUpload}\n` +
      `🚫 Already present (skipped): ${alreadyPresent.length}\n` +
      `❓ Unknown prefix (skipped): ${unknownPrefix.length}\n` +
      `📥 Inserted: ${inserted}\n\n` +
      (alreadyPresent.length ? `Already present examples: ${sample(alreadyPresent)}\n` : "") +
      (unknownPrefix.length ? `Unknown prefix examples: ${sample(unknownPrefix)}\n` : ""),
    { disable_web_page_preview: true }
  );

  return { inserted, alreadyPresent, unknownPrefix, duplicateInUpload };
}

async function downloadTelegramFile(fileId) {
  const fileLink = await bot.telegram.getFileLink(fileId);
  return await new Promise((resolve, reject) => {
    https
      .get(fileLink.href, (res) => {
        const { statusCode } = res;
        if (statusCode && statusCode >= 400) {
          reject(new Error(`Failed to download file: ${statusCode}`));
          res.resume();
          return;
        }
        const chunks = [];
        res.on("data", (d) => chunks.push(d));
        res.on("end", () => resolve(Buffer.concat(chunks)));
      })
      .on("error", reject);
  });
}

async function exportPaymentsCsv(ctx) {
  const orders = getCollection(COLLECTIONS.ORDERS);
  const rows = await orders
    .find({})
    .sort({ createdAt: -1 })
    .toArray();

  const header = [
    "orderId",
    "userId",
    "userChatId",
    "username",
    "category",
    "quantity",
    "unitPrice",
    "totalAmount",
    "status",
    "utr",
    "screenshotFileId",
    "createdAt",
    "submittedAt",
    "decisionAt",
    "adminDecisionBy",
    "deliveredCodes",
  ];

  const escapeCsv = (v) => {
    const s = Array.isArray(v) ? v.join("|") : v === undefined || v === null ? "" : String(v);
    if (/[,"\n]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
    return s;
  };

  const lines = [
    header.join(","),
    ...rows.map((r) =>
      header.map((h) => escapeCsv(r[h])).join(",")
    ),
  ];

  const csv = lines.join("\n");

  const tmpPath = path.join(os.tmpdir(), `payments-${Date.now()}.csv`);
  await fs.promises.writeFile(tmpPath, csv, "utf8");

  try {
    await ctx.telegram.sendDocument(ctx.chat.id, {
      source: tmpPath,
      filename: "payment_history.csv",
    });
  } finally {
    fs.promises.unlink(tmpPath).catch(() => {});
  }
}

async function runOrderExpiryLoop() {
  const orders = getCollection(COLLECTIONS.ORDERS);

  const scanStatuses = ["qr_sent", "awaiting_utr", "awaiting_screenshot", "awaiting_admin"];
  const expired = await orders
    .find({
      status: { $in: scanStatuses },
      expiresAt: { $lte: new Date() },
    })
    .toArray();

  if (expired.length === 0) return;

  const orderIds = expired.map((o) => o.orderId);
  await orders.updateMany(
    { orderId: { $in: orderIds } },
    { $set: { status: "expired", decisionAt: now() } }
  );

  // Notify users (best-effort)
  for (const order of expired) {
    try {
      stopOrderTimer(order.orderId);
      clearUserStateByOrderId(order.orderId);
      await updateQrMessageForOrder(order.orderId, true);
      await bot.telegram.sendMessage(
        order.userChatId,
        `Order expired: ${order.orderId}\nPlease start again from /start.`
      );
    } catch (_) {}
  }
}

async function handleUserBackToMain(ctx) {
  const ui = userStates.get(ctx.from.id)?.ui;
  userStates.delete(ctx.from.id);
  await showMainMenu(ctx, true, ui || {});
}

bot.start(async (ctx) => {
  const userId = ctx.from.id;
  const isAdmin = userId === ADMIN_CHAT_ID;

  if (!isAdmin) {
    const requiredChannels = await getRequiredChannels();
    if (!requiredChannels.length) {
      await ctx.reply("Admin hasn't configured required channels yet. Please try again later.");
      return;
    }

    const ok = await isUserInRequiredChannels(userId);
    if (!ok) {
      const missing = await getMissingRequiredChannels(userId);
      await ctx.reply(
        `Join the required channel(s) first.\nMissing: ${missing
          .map((c) => (c.startsWith("@") ? c.slice(1) : c))
          .join(", ")}`,
        buildJoinKeyboard(requiredChannels)
      );
      return;
    }

    // Allowed: show welcome/policy UI.
    await sendUserChatIdInfo(ctx);
  }

  // For admin too, show IDs (helps debugging/admin config).
  if (isAdmin) await sendUserChatIdInfo(ctx);

  await showMainMenu(ctx, false);
});

bot.action("join:check", async (ctx) => {
  const userId = ctx.from.id;
  if (userId === ADMIN_CHAT_ID) return ctx.answerCbQuery();

  const requiredChannels = await getRequiredChannels();
  if (!requiredChannels.length) {
    await ctx.answerCbQuery("Admin not configured yet.");
    return;
  }

  const ok = await isUserInRequiredChannels(userId);
  if (!ok) {
    const missing = await getMissingRequiredChannels(userId);
    await ctx.answerCbQuery("Not all channels joined yet.");
    try {
      await ctx.editMessageText(
        `🔒 <b>Access locked</b>\n\nTelegram still didn't detect your join.\n\nMissing: ${missing
          .map((c) => `<code>${escapeHtml(c.replace(/^@/, ""))}</code>`)
          .join(", ")}`,
        {
          parse_mode: "HTML",
          reply_markup: buildJoinKeyboard(requiredChannels).reply_markup,
          disable_web_page_preview: true,
        }
      );
    } catch (_) {}
    return;
  }
  await ctx.answerCbQuery();
  await showMainMenu(ctx, false);
});

bot.action(/^(buy|history|admin:|paid:|cancel:)/, async (ctx, next) => {
  // Global access control for admin callbacks.
  const data = ctx.callbackQuery?.data || "";
  if (data.startsWith("admin:") && ctx.from.id !== ADMIN_CHAT_ID) {
    return ctx.answerCbQuery("Not allowed.");
  }

  // Central place to handle callback flow; avoid breaking membership gating.
  try {
    return await next();
  } catch (e) {
    return ctx.answerCbQuery("Something went wrong.");
  }
});

bot.action(/^(buy:\d+)/, async (ctx) => {
  const userId = ctx.from.id;
  const category = Number(ctx.match[1].split(":")[1]);

  if (userId !== ADMIN_CHAT_ID) {
    const requiredChannels = await getRequiredChannels();
    if (!requiredChannels.length) {
      await ctx.answerCbQuery("Admin not configured yet.");
      return;
    }
    const ok = await isUserInRequiredChannels(userId);
    if (!ok) {
      const missing = await getMissingRequiredChannels(userId);
      await ctx.answerCbQuery("Join required channels first.");
      try {
        await ctx.editMessageText(
          `Join the required channel(s) first.\nMissing: ${missing
            .map((c) => (c.startsWith("@") ? c.slice(1) : c))
            .join(", ")}`,
          buildJoinKeyboard(requiredChannels)
        );
      } catch (_) {}
      return;
    }
  }

  const stocks = await getStocks();
  const stock = stocks.get(category) ?? 0;
  if (stock <= 0) {
    await ctx.answerCbQuery("No codes available right now.");
    const sent = await ctx.editMessageText(`No codes available for ${category} at the moment. Try another category.`);
    return;
  }

  const qr = await getPaymentQr();
  if (!qr) {
    await ctx.answerCbQuery("Payment QR not configured");
    try {
      await ctx.editMessageText(
        `Admin hasn't uploaded the payment QR yet.\nPlease try again later.`
      );
    } catch (_) {}
    return;
  }

  const prices = await getCategoryPrices();
  const unitPrice = prices[category];
  if (!unitPrice) {
    await ctx.answerCbQuery("Price not configured");
    try {
      await ctx.editMessageText(`Admin hasn't set the price for ${category} yet. Try later.`);
    } catch (_) {}
    return;
  }

  const ui = {
    chatId: ctx.chat.id,
    messageId: ctx.callbackQuery.message.message_id,
    isPhoto: false,
  };

  const kb = Markup.inlineKeyboard([
    [Markup.button.callback("1 code", `qty:${category}:1`)],
    [Markup.button.callback("2 codes", `qty:${category}:2`)],
    [Markup.button.callback("5 codes", `qty:${category}:5`)],
    [Markup.button.callback("Custom", `qty:${category}:custom`)],
    [Markup.button.callback("Cancel", "cancel:flow")],
  ]);

  await ctx.answerCbQuery();
  try {
    await ctx.editMessageText(
      `🛒 <b>Category:</b> <code>${category}</code> off\n\n` +
        `📦 <b>Stock available:</b> <code>${stock}</code> units\n` +
        `💳 <b>Price per code:</b> <code>${unitPrice}</code>\n\n` +
        `✨ <b>How many codes do you want to buy?</b>`,
      { reply_markup: kb.reply_markup, disable_web_page_preview: true, parse_mode: "HTML" }
    );
  } catch (_) {}

  userStates.set(userId, {
    category,
    qty: null,
    unitPrice,
    expecting: null,
    ui,
  });
});

bot.action("cancel:flow", async (ctx) => {
  const userId = ctx.from.id;
  userStates.delete(userId);
  await ctx.answerCbQuery();
  try {
    await ctx.editMessageText("🚫 <b>Cancelled</b>\n\nReturning to menu...", {
      parse_mode: "HTML",
      disable_web_page_preview: true,
    });
  } catch (_) {}
  await showMainMenu(ctx, true, {
    chatId: ctx.chat.id,
    messageId: ctx.callbackQuery.message.message_id,
  });
});

bot.action(/^qty:(\d+):custom$/, async (ctx) => {
  const userId = ctx.from.id;
  const category = Number(ctx.match[1]);

  const stocks = await getStocks();
  const stock = stocks.get(category) ?? 0;
  if (stock <= 0) {
    await ctx.answerCbQuery("No stock.");
    await ctx.editMessageText(`No codes available for ${category} right now.`);
    return;
  }

  const prices = await getCategoryPrices();
  const unitPrice = prices[category];
  if (!unitPrice) {
    await ctx.answerCbQuery("Price not configured.");
    await ctx.editMessageText(`Admin hasn't set the price for ${category} yet.`);
    return;
  }

  await ctx.answerCbQuery();
  userStates.set(userId, {
    category,
    qty: null,
    unitPrice,
    expectedTotal: null,
    expecting: "custom_qty",
    ui: { chatId: ctx.chat.id, messageId: ctx.callbackQuery.message.message_id, isPhoto: false },
  });

  const kb = Markup.inlineKeyboard([
    [Markup.button.callback("🏠 Main Menu", "cancel:flow")],
    [Markup.button.callback("📄 History", "history:open")],
  ]);
  await ctx.editMessageText(
    `✍️ <b>Custom quantity</b>\n\n` +
      `Category: <code>${category}</code> off\n` +
      `Available stock: <code>${stock}</code> units\n\n` +
      `Enter the quantity you want (whole number):\n` +
      `<code>1</code> to <code>${stock}</code>`,
    { reply_markup: kb.reply_markup, parse_mode: "HTML", disable_web_page_preview: true }
  );
});

bot.action(/^qty:(\d+):(1|2|5)$/, async (ctx) => {
  const userId = ctx.from.id;
  const category = Number(ctx.match[1]);
  const qty = Number(ctx.match[2]);

  const stocks = await getStocks();
  const stock = stocks.get(category) ?? 0;
  if (stock < qty) {
    await ctx.answerCbQuery("Not enough stock.");
    await ctx.editMessageText(
      `Stock not available for ${category}.\nRequested: ${qty}\nTotal stock: ${stock} units.\n\nChoose another category.`
    );
    return;
  }

  const prices = await getCategoryPrices();
  const unitPrice = prices[category];
  if (!unitPrice) {
    await ctx.answerCbQuery("Price not configured.");
    await ctx.editMessageText(`Admin hasn't set the price for ${category} yet.`);
    return;
  }

  const totalAmount = Math.round(unitPrice * qty * 100) / 100;

  await ctx.answerCbQuery();

  // Create order immediately and show QR flow.
  const orderId = generateOrderId();
  const orderDoc = {
    orderId,
    userId,
    userChatId: ctx.chat.id,
    username: ctx.from.username || "",
    category,
    quantity: qty,
    unitPrice,
    totalAmount,
    status: "qr_sent",
    createdAt: now(),
    expiresAt: new Date(Date.now() + ORDER_DURATION_MS),
    utr: null,
    screenshotFileId: null,
    deliveredCodes: [],
    decisionAt: null,
    adminDecisionBy: null,
    qrMessageChatId: null,
    qrMessageId: null,
    qrMessageIsPhoto: true,
  };

  const orders = getCollection(COLLECTIONS.ORDERS);
  await orders.insertOne(orderDoc);

  // Replace current message with a short loader.
  try {
    await ctx.editMessageText(
      `🧾 <b>Order Created</b>\n\n` +
        `🆔 <b>Order ID:</b> <code>${escapeHtml(orderId)}</code>\n` +
        `💠 <b>Category:</b> <code>${category}</code> off\n` +
        `🧮 <b>Qty:</b> <code>${qty}</code>\n` +
        `💳 <b>Total:</b> <code>${totalAmount}</code>\n\n` +
        `⏳ Loading payment QR...`,
      { parse_mode: "HTML", disable_web_page_preview: true }
    );
  } catch (_) {}

  const ui = await sendQrFlow(ctx, orderDoc);
  await orders.updateOne(
    { orderId },
    { $set: { qrMessageChatId: ui.chatId, qrMessageId: ui.messageId, qrMessageIsPhoto: Boolean(ui.isPhoto) } }
  );

  await startOrderLiveTimer(orderId);
  userStates.set(userId, { orderId, expecting: null, ui });
});

bot.action(/^cancel:(ORD-)/, async (ctx) => {
  const userId = ctx.from.id;
  const orderId2 = ctx.callbackQuery.data.split(":")[1];

  const orders = getCollection(COLLECTIONS.ORDERS);
  const order = await orders.findOne({ orderId: orderId2 });
  if (!order) {
    await ctx.answerCbQuery("Order not found.");
    return;
  }
  if (order.userId !== userId && userId !== ADMIN_CHAT_ID) {
    await ctx.answerCbQuery("Not your order.");
    return;
  }

  if (isTerminalOrderStatus(order.status)) {
    await ctx.answerCbQuery("Order already finished.");
    return;
  }

  await orders.updateOne({ orderId: orderId2 }, { $set: { status: "cancelled", decisionAt: now() } });
  clearUserStateByOrderId(orderId2);
  await ctx.answerCbQuery("Cancelled.");
  stopOrderTimer(orderId2);
  await updateQrMessageForOrder(orderId2, true);
});

bot.action(/^paid:(ORD-)/, async (ctx) => {
  const userId = ctx.from.id;
  const orderId = ctx.callbackQuery.data.split(":")[1];

  if (userId !== ADMIN_CHAT_ID) {
    const requiredChannels = await getRequiredChannels();
    if (!requiredChannels.length) {
      await ctx.answerCbQuery("Admin not configured yet.");
      return;
    }
    const ok = await isUserInRequiredChannels(userId);
    if (!ok) {
      const missing = await getMissingRequiredChannels(userId);
      await ctx.answerCbQuery("Join required channels first.");
      try {
        await ctx.editMessageText(
          `Join the required channel(s) first.\nMissing: ${missing
            .map((c) => (c.startsWith("@") ? c.slice(1) : c))
            .join(", ")}`,
          buildJoinKeyboard(requiredChannels)
        );
      } catch (_) {}
      return;
    }
  }

  let order;
  try {
    order = await ensureFreshOrder(orderId);
  } catch (e) {
    await ctx.answerCbQuery(e?.message || "Can't do this now.");
    return;
  }

  const canMoveFrom = ["qr_sent"];
  if (!canMoveFrom.includes(order.status)) {
    await ctx.answerCbQuery("Can't do this now.");
    return;
  }

  await updateOrder(orderId, { status: "awaiting_utr", decisionAt: null });
  await ctx.answerCbQuery("OK.");

  const ui = userStates.get(userId)?.ui || {
    chatId: ctx.chat.id,
    messageId: ctx.callbackQuery.message.message_id,
    isPhoto: Boolean(ctx.callbackQuery.message?.photo),
  };

  userStates.set(userId, { orderId, expecting: "utr", ui });
  await updateQrMessageForOrder(orderId, true);
});

// UTR text handler
bot.on("text", async (ctx, next) => {
  const userId = ctx.from.id;
  const adminState = adminStates.get(userId);
  if (adminState?.expecting === "set_channels") {
    const parsed = parseChannelsText(ctx.message.text);
    if (!parsed.length) {
      await ctx.reply(
        "No valid channels detected.\nSend one or more channel links/usernames like:\n`@channelusername`\n`https://t.me/channelusername`",
        { parse_mode: "Markdown" }
      );
      return;
    }

    await setRequiredChannels(parsed);
    adminStates.delete(userId);
    await ctx.reply(`Required channels updated. Total: ${parsed.length}`);
    return;
  }
  if (adminState?.expecting === "set_qr") {
    await ctx.reply("You're setting the Payment QR. Please send the QR as a *photo*.", {
      parse_mode: "Markdown",
    });
    return;
  }
  if (adminState?.expecting === "set_prices") {
    const parsed = parsePricesText(ctx.message.text);
    const missing = CATEGORY_IDS.filter((c) => !parsed[c]);
    if (!Object.keys(parsed).length || missing.length) {
      await ctx.reply(
        `Please send prices for all categories.\nMissing: ${missing.join(", ")}\n\nExample:\n500: 50\n1000: 100\n2000: 200\n4000: 400`,
        { parse_mode: "Markdown" }
      );
      return;
    }
    await setCategoryPrices(parsed);
    adminStates.delete(userId);
    await ctx.reply("Category prices updated ✅");
    return;
  }
  if (adminState?.expecting === "upload_codes") {
    const codes = parseCsvCodes(ctx.message.text || "");

    if (codes.length === 0) {
      await ctx.reply(
        "No codes detected.\nSend one code per line.\nExample:\n`SVIxxxx`\n`SVCyyyy`\n\nCodes should be uppercase (or the bot will uppercase them).",
        { parse_mode: "Markdown" }
      );
      return;
    }

    await importCodesToDb(codes, ctx);
    adminStates.delete(userId);
    return;
  }

  const state = userStates.get(userId);
  if (!state) return next();

  // Custom qty step (admin decided category, user chooses quantity)
  if (state.expecting === "custom_qty") {
    const raw = (ctx.message.text || "").trim();
    const qty = Number(raw);
    if (!Number.isFinite(qty) || qty <= 0 || !Number.isInteger(qty)) {
      const kb = Markup.inlineKeyboard([
        [Markup.button.callback("🏠 Main Menu", "cancel:flow")],
        [Markup.button.callback("📄 History", "history:open")],
      ]);
      try {
        await editOrSend(
          ctx,
          state.ui,
          `❌ <b>Invalid quantity</b>\n\nEnter a whole number (e.g., 1, 2, 5...).`,
          { reply_markup: kb.reply_markup, parse_mode: "HTML", disable_web_page_preview: true }
        );
      } catch (_) {}
      return;
    }

    const stocks = await getStocks();
    const stock = stocks.get(state.category) ?? 0;
    if (qty > stock) {
      const kb = Markup.inlineKeyboard([[Markup.button.callback("Back to Menu", "menu:back")]]);
      try {
        await editOrSend(
          ctx,
          state.ui,
          `🚫 <b>Not enough stock</b>\n\nCategory: <code>${escapeHtml(state.category)}</code>\nRequested: <code>${qty}</code>\nTotal stock: <code>${stock}</code> units.\n\nChoose another quantity/category.`,
          { reply_markup: kb.reply_markup, parse_mode: "HTML", disable_web_page_preview: true }
        );
      } catch (_) {
        await ctx.reply(
          `Stock not available for ${state.category}.\nRequested: ${qty}\nTotal stock: ${stock} units.`
        );
      }
      userStates.delete(userId);
      return;
    }

    const totalAmount = Math.round(state.unitPrice * qty * 100) / 100;

    const orderId = generateOrderId();
    const orderDoc = {
      orderId,
      userId,
      userChatId: ctx.chat.id,
      username: ctx.from.username || "",
      category: state.category,
      quantity: qty,
      unitPrice: state.unitPrice,
      totalAmount,
      status: "qr_sent",
      createdAt: now(),
      expiresAt: new Date(Date.now() + ORDER_DURATION_MS),
      utr: null,
      screenshotFileId: null,
      deliveredCodes: [],
      decisionAt: null,
      adminDecisionBy: null,
      qrMessageChatId: null,
      qrMessageId: null,
      qrMessageIsPhoto: true,
    };

    const orders = getCollection(COLLECTIONS.ORDERS);
    await orders.insertOne(orderDoc);

    userStates.delete(userId);

    // Update the same message with loader text before sending the QR.
    await editOrSend(
      ctx,
      state.ui,
      `🧾 <b>Order Created</b>\n\n` +
        `🆔 <b>Order ID:</b> <code>${escapeHtml(orderId)}</code>\n` +
        `💠 <b>Category:</b> <code>${escapeHtml(state.category)}</code> off\n` +
        `🧮 <b>Qty:</b> <code>${qty}</code>\n` +
        `💳 <b>Total:</b> <code>${totalAmount}</code>\n\n` +
        `⏳ Loading payment QR...`,
      { parse_mode: "HTML", disable_web_page_preview: true }
    );

    const ui = await sendQrFlow(ctx, orderDoc);
    await orders.updateOne(
      { orderId },
      { $set: { qrMessageChatId: ui.chatId, qrMessageId: ui.messageId, qrMessageIsPhoto: Boolean(ui.isPhoto) } }
    );

    await startOrderLiveTimer(orderId);
    userStates.set(userId, { orderId, expecting: null, ui });
    return;
  }

  // UTR step
  if (state.expecting !== "utr") return next();

  const orderId = state.orderId;
  const utr = (ctx.message.text || "").trim();
  if (!utr || utr.length < 4) {
    await ctx.reply("UTR looks too short. Please send a valid UTR/reference number.");
    return;
  }

  let order;
  try {
    order = await ensureFreshOrder(orderId);
  } catch (e) {
    userStates.delete(userId);
    await ctx.reply(e.message);
    return;
  }

  if (order.userId !== userId) {
    userStates.delete(userId);
    await ctx.reply("Not your order.");
    return;
  }

  if (order.status !== "awaiting_utr") {
    await ctx.reply("This order is not expecting UTR anymore. Please start again with /start.");
    userStates.delete(userId);
    return;
  }

  await updateOrder(orderId, { status: "awaiting_screenshot", utr, decisionAt: null });
  userStates.set(userId, { orderId, expecting: "screenshot", ui: state.ui });
  await updateQrMessageForOrder(orderId, true);
});

// Screenshot photo handler
bot.on("photo", async (ctx, next) => {
  const userId = ctx.from.id;
  const adminState = adminStates.get(userId);
  if (adminState?.expecting === "set_qr") {
    const photos = ctx.message.photo || [];
    const best = photos[photos.length - 1];
    const photoFileId = best?.file_id;
    if (!photoFileId) {
      await ctx.reply("Couldn't read the QR photo. Please send it again.");
      return;
    }
    await setPaymentQrFileId(photoFileId);
    adminStates.delete(userId);
    await ctx.reply("Payment QR updated ✅");
    return;
  }

  const state = userStates.get(userId);
  if (!state || state.expecting !== "screenshot") return next();

  const orderId = state.orderId;
  let order;
  try {
    order = await ensureFreshOrder(orderId);
  } catch (e) {
    userStates.delete(userId);
    await ctx.reply(e.message);
    return;
  }

  if (order.userId !== userId) {
    userStates.delete(userId);
    await ctx.reply("Not your order.");
    return;
  }

  if (order.status !== "awaiting_screenshot") {
    userStates.delete(userId);
    await ctx.reply("This order is not expecting a screenshot anymore. Please start again with /start.");
    return;
  }

  const photos = ctx.message.photo || [];
  const best = photos[photos.length - 1];
  const screenshotFileId = best?.file_id;
  if (!screenshotFileId) {
    await ctx.reply("Please send a photo/screenshot.");
    return;
  }

  await updateOrder(orderId, {
    status: "awaiting_admin",
    screenshotFileId,
    submittedAt: now(),
    decisionAt: null,
    adminDecisionBy: null,
  });
  userStates.delete(userId);
  // User flow is complete after screenshot submission.
  stopOrderTimer(orderId);
  await updateQrMessageForOrder(orderId, true);

  // Notify admin with accept/decline buttons
  const acceptCb = `admin:accept:${orderId}`;
  const declineCb = `admin:decline:${orderId}`;
  const keyboard = Markup.inlineKeyboard([
    [Markup.button.callback("✅ Accept", acceptCb), Markup.button.callback("❌ Decline", declineCb)],
  ]);

  const caption =
    `Payment request\n\n` +
    `Order: ${orderId}\n` +
    `User: ${order.username ? "@" + order.username : "(no username)"} (id: ${order.userId})\n` +
    `Category: ${order.category} off x${order.quantity || 1}\n` +
    `Total: ${order.totalAmount || 0}\n` +
    `UTR: ${order.utr}\n\n` +
    `Status: pending approval`;

  try {
    await bot.telegram.sendPhoto(ADMIN_CHAT_ID, screenshotFileId, {
      caption,
      ...keyboard,
    });
  } catch (e) {
    // If photo send fails, fall back to text only.
    await bot.telegram.sendMessage(
      ADMIN_CHAT_ID,
      `${caption}\nScreenshot file_id: ${screenshotFileId}`,
      keyboard
    );
  }
});

bot.action(/^(admin:accept:)/, async (ctx) => {
  const adminId = ctx.from.id;
  if (adminId !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");

  const orderId = ctx.callbackQuery.data.split(":").slice(2).join(":");
  const orders = getCollection(COLLECTIONS.ORDERS);
  const order = await orders.findOne({ orderId });
  if (!order) return ctx.answerCbQuery("Order not found.");

  const ok = await atomicSetOrderStatus(
    orderId,
    "awaiting_admin",
    "accepted_processing",
    { adminDecisionBy: adminId }
  );
  if (!ok) return ctx.answerCbQuery("Already processed or not pending.");

  const reqQty = Number(order.quantity) || 1;
  const claimed = await claimNCodes(order.category, orderId, reqQty);

  // Only treat as out-of-stock if we could not claim even a single code.
  if (claimed.length === 0) {
    // Release any claimed codes back to stock.
    const ids = claimed.map((c) => c._id);
    await releaseCodesByIds(ids);

    const codes = getCollection(COLLECTIONS.CODES);
    const remaining = await codes.countDocuments({ category: order.category, available: true });

    await orders.updateOne(
      { orderId },
      { $set: { status: "out_of_stock", deliveredCodes: [], decisionAt: now() } }
    );

    stopOrderTimer(orderId);
    clearUserStateByOrderId(orderId);
    await updateQrMessageForOrder(orderId, true);

    await ctx.answerCbQuery("Not enough stock.");
    try {
      const msg = ctx.callbackQuery.message;
      if (msg?.photo?.length) {
        await ctx.editMessageCaption(
          `❌ Not enough stock\nOrder: ${orderId}\nRequested: ${reqQty}\nRemaining: ${remaining}`
        );
      } else {
        await ctx.editMessageText(
          `❌ Not enough stock\nOrder: ${orderId}\nRequested: ${reqQty}\nRemaining: ${remaining}`
        );
      }
    } catch (_) {}

    try {
      await bot.telegram.sendMessage(
        order.userChatId,
        `Sorry ❌\nStock not available for ${order.category}.\nRequested: ${reqQty}\nTotal stock: ${remaining} units.\n\nOrder: ${orderId}\nPlease try again.`
      );
    } catch (_) {}
    return;
  }

  const codesStr = claimed.map((c) => c.code);
  const codeIds = claimed.map((c) => c._id);

  await orders.updateOne(
    { orderId },
    { $set: { status: "fulfilled", deliveredCodes: codesStr, decisionAt: now() } }
  );

  // Remove delivered codes from DB (sold codes must never be reused)
  try {
    await getCollection(COLLECTIONS.CODES).deleteMany({ _id: { $in: codeIds } });
  } catch (_) {}

  stopOrderTimer(orderId);
  clearUserStateByOrderId(orderId);
  await updateQrMessageForOrder(orderId, true);

  await ctx.answerCbQuery("Accepted.");
  try {
    const msg = ctx.callbackQuery.message;
    const codesPreview = codesStr.slice(0, 25).map((c) => `- ${c}`).join("\n");
    const adminText =
      `✅ Payment successful\n` +
      `Order: ${orderId}\n` +
      `User: ${order.username ? "@" + order.username : "(no username)"} (${order.userId})\n` +
      `Category: ${order.category} off x${reqQty}\n` +
      `Total: ${order.totalAmount || 0}\n` +
      `UTR: ${order.utr || ""}\n\n` +
      `Delivered code(s):\n${codesPreview}`;
    if (msg?.photo?.length) {
      await ctx.editMessageCaption(adminText);
    } else {
      await ctx.editMessageText(adminText);
    }
  } catch (_) {}

  // Send codes to user
  try {
    const list = codesStr.map((c, i) => `${i + 1}. ${c}`).join("\n");
    await bot.telegram.sendMessage(
      order.userChatId,
      `Payment accepted ✅\n\nYour codes (${order.category}):\n${list}\n\nOrder: ${orderId}`
    );
  } catch (_) {}
});

bot.action(/^(admin:decline:)/, async (ctx) => {
  const adminId = ctx.from.id;
  if (adminId !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");

  const orderId = ctx.callbackQuery.data.split(":").slice(2).join(":");
  const orders = getCollection(COLLECTIONS.ORDERS);
  const order = await orders.findOne({ orderId });
  if (!order) return ctx.answerCbQuery("Order not found.");

  const ok = await atomicSetOrderStatus(orderId, "awaiting_admin", "declined", {
    adminDecisionBy: adminId,
  });
  if (!ok) return ctx.answerCbQuery("Already processed or not pending.");

  await ctx.answerCbQuery("Declined.");
  stopOrderTimer(orderId);
  await updateQrMessageForOrder(orderId, true);
  try {
    const msg = ctx.callbackQuery.message;
    if (msg?.photo?.length) {
      await ctx.editMessageCaption(`Admin decision: Declined\nOrder: ${orderId}`);
    } else {
      await ctx.editMessageText(`Admin decision: Declined\nOrder: ${orderId}`);
    }
  } catch (_) {}

  // Notify user
  try {
    await bot.telegram.sendMessage(
      order.userChatId,
      `Payment declined ❌\n\nOrder: ${orderId}\nPlease try again later.`
    );
  } catch (_) {}
});

bot.action("history:open", async (ctx) => {
  const userId = ctx.from.id;
  if (userId !== ADMIN_CHAT_ID) {
    const requiredChannels = await getRequiredChannels();
    if (!requiredChannels.length) return ctx.answerCbQuery("Admin not configured yet.");
    const ok = await isUserInRequiredChannels(userId);
    if (!ok) {
      const missing = await getMissingRequiredChannels(userId);
      return ctx.answerCbQuery(`Join required channels first (missing: ${missing.length}).`);
    }
  }

  await ctx.answerCbQuery();
  const userChatId = ctx.chat.id;
  const orders = getCollection(COLLECTIONS.ORDERS);
  const list = await orders.find({ userChatId }).sort({ createdAt: -1 }).limit(10).toArray();

  if (!list.length) {
    const keyboard = Markup.inlineKeyboard([[Markup.button.callback("Back", "menu:back")]]);
    await ctx.editMessageText("📭 <b>No purchase history yet</b>\n\nWhen admin accepts a payment, your codes will appear here.", {
      parse_mode: "HTML",
      reply_markup: keyboard.reply_markup,
      disable_web_page_preview: true,
    });
    return;
  }

  const keyboard = Markup.inlineKeyboard([[Markup.button.callback("⬅️ Back", "menu:back")]]);
  const lines = list.map((o, idx) => {
    const orderId = escapeHtml(o.orderId);
    const category = escapeHtml(String(o.category));
    const qty = o.quantity ? Number(o.quantity) : 1;
    const total = o.totalAmount !== undefined && o.totalAmount !== null ? escapeHtml(o.totalAmount) : "";
    const utr = o.utr ? escapeHtml(o.utr) : "";
    const delivered =
      o.deliveredCodes?.length
        ? `\n🎁 Codes:\n${o.deliveredCodes
            .slice(0, 20)
            .map((c) => `• <code>${escapeHtml(c)}</code>`)
            .join("\n")}`
        : "";

    const statusLine = `📌 <b>${escapeHtml(statusPretty(o.status))}</b>`;

    return (
      `${idx + 1}. <b>Order</b>: <code>${orderId}</code>\n` +
      `💠 <b>Category</b>: ${category}\n` +
      `🧮 <b>Qty</b>: ${qty}\n` +
      `💳 <b>Total</b>: ${total}\n` +
      `🧾 ${statusLine}\n` +
      (utr ? `🔎 <b>UTR</b>: <code>${utr}</code>\n` : "") +
      delivered
    );
  });

  await ctx.editMessageText(
    `🧾 <b>Your Purchase History</b>\n\n${lines.join("\n\n")}\n\n✅ Showing last ${list.length} orders.`,
    {
      parse_mode: "HTML",
      reply_markup: keyboard.reply_markup,
      disable_web_page_preview: true,
    }
  );
});

bot.action("menu:back", async (ctx) => {
  await ctx.answerCbQuery();
  await showMainMenu(ctx, true, {
    chatId: ctx.chat.id,
    messageId: ctx.callbackQuery.message.message_id,
  });
});

bot.action("menu:stocks", async (ctx) => {
  const userId = ctx.from.id;
  await ctx.answerCbQuery();

  const stocks = await getStocks();
  const lines = CATEGORY_IDS.map((cat) => {
    const st = stocks.get(cat) ?? 0;
    return `• <b>${cat}</b> off — Stock: <code>${st}</code>`;
  });

  const keyboard = Markup.inlineKeyboard([
    [Markup.button.callback("⬅️ Back to menu", "menu:back")],
  ]);

  const text =
    `📊 <b>Live Stocks</b>\n\n` +
    lines.join("\n") +
    `\n\n` +
    `✅ Pick a category to buy. Stock updates automatically.`;

  try {
    await ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: keyboard.reply_markup, disable_web_page_preview: true });
  } catch (_) {
    await ctx.reply(text, { parse_mode: "HTML", reply_markup: keyboard.reply_markup, disable_web_page_preview: true });
  }
});

bot.command("history", async (ctx) => {
  const userId = ctx.from.id;
  if (userId !== ADMIN_CHAT_ID) {
    const requiredChannels = await getRequiredChannels();
    if (!requiredChannels.length) {
      await ctx.reply("⚙️ <b>Not configured yet</b>\nAdmin hasn't set required channels. Try again later.", {
        parse_mode: "HTML",
      });
      return;
    }
    const ok = await isUserInRequiredChannels(userId);
    if (!ok) {
      const missing = await getMissingRequiredChannels(userId);
      await ctx.reply(
        `🔒 <b>Channel required</b>\n\nMissing: ${missing
          .map((c) => `<code>${escapeHtml(c.replace(/^@/, ""))}</code>`)
          .join(", ")}`,
        {
          parse_mode: "HTML",
          reply_markup: buildJoinKeyboard(requiredChannels).reply_markup,
          disable_web_page_preview: true,
        }
      );
      return;
    }
  }

  const userChatId = ctx.chat.id;
  const orders = getCollection(COLLECTIONS.ORDERS);
  const list = await orders
    .find({ userChatId })
    .sort({ createdAt: -1 })
    .limit(10)
    .toArray();

  if (!list.length) {
    await ctx.reply(
      "📭 <b>No purchase history yet</b>\n\nWhen you buy and admin approves, your codes will appear here.",
      {
        parse_mode: "HTML",
        reply_markup: Markup.inlineKeyboard([[Markup.button.callback("⬅️ Back to menu", "menu:back")]])
          .reply_markup,
        disable_web_page_preview: true,
      }
    );
    return;
  }

  const keyboard = Markup.inlineKeyboard([[Markup.button.callback("⬅️ Back to menu", "menu:back")]]);
  const lines = list.map((o, idx) => {
    const orderId = escapeHtml(o.orderId);
    const category = escapeHtml(String(o.category));
    const qty = o.quantity ? Number(o.quantity) : 1;
    const total = o.totalAmount !== undefined && o.totalAmount !== null ? escapeHtml(o.totalAmount) : "";
    const utr = o.utr ? escapeHtml(o.utr) : "";
    const delivered =
      o.deliveredCodes?.length
        ? `\n🎁 Codes:\n${o.deliveredCodes.slice(0, 20).map((c) => `• <code>${escapeHtml(c)}</code>`).join("\n")}`
        : "";

    return (
      `${idx + 1}. <b>Order</b>: <code>${orderId}</code>\n` +
      `💠 <b>Category</b>: ${category}\n` +
      `🧮 <b>Qty</b>: ${qty}\n` +
      `💳 <b>Total</b>: ${total}\n` +
      `🧾 <b>Status</b>: ${escapeHtml(statusPretty(o.status))}\n` +
      (utr ? `🔎 <b>UTR</b>: <code>${utr}</code>\n` : "") +
      delivered
    );
  });

  await ctx.reply(
    `🧾 <b>Your Purchase History</b>\n\n${lines.join("\n\n")}\n\n✅ Showing last ${list.length} orders.`,
    {
      parse_mode: "HTML",
      reply_markup: keyboard.reply_markup,
      disable_web_page_preview: true,
    }
  );
});

bot.command("start_admin", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return;
  const keyboard = Markup.inlineKeyboard([
    [Markup.button.callback("Configure Required Channel(s)", "admin:config:channels")],
    [Markup.button.callback("Set Category Prices", "admin:config:prices")],
    [Markup.button.callback("Upload Payment QR (photo)", "admin:config:qr")],
    [Markup.button.callback("Upload Codes (1 per line)", "admin:config:codes")],
    [Markup.button.callback("🧹 Empty Codes", "admin:empty")],
    [Markup.button.callback("🕒 Pending Requests", "admin:pending")],
    [Markup.button.callback("View last payments", "admin:view:history")],
    [Markup.button.callback("Export payments (CSV)", "admin:export")],
  ]);
  await ctx.reply("Admin panel:", keyboard);
});

bot.action("admin:pending", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");
  await ctx.answerCbQuery("Loading pending...");

  const orders = getCollection(COLLECTIONS.ORDERS);
  const list = await orders.find({ status: "awaiting_admin" }).sort({ submittedAt: 1, createdAt: 1 }).limit(25).toArray();

  if (!list.length) {
    await ctx.editMessageText("✅ No pending requests right now.");
    return;
  }

  await ctx.editMessageText(`🕒 Pending requests: ${list.length}\n\nI will send them below...`);

  for (const o of list) {
    const acceptCb = `admin:accept:${o.orderId}`;
    const declineCb = `admin:decline:${o.orderId}`;
    const keyboard = Markup.inlineKeyboard([
      [Markup.button.callback("✅ Accept", acceptCb), Markup.button.callback("❌ Decline", declineCb)],
    ]);

    const caption =
      `🧾 <b>Pending Payment</b>\n\n` +
      `🆔 <b>Order:</b> <code>${escapeHtml(o.orderId)}</code>\n` +
      `👤 <b>User:</b> ${o.username ? "@" + escapeHtml(o.username) : "<i>(no username)</i>"}\n` +
      `🧑‍💻 <b>User ID:</b> <code>${escapeHtml(o.userId)}</code>\n` +
      `💠 <b>Category:</b> <code>${escapeHtml(o.category)}</code> off\n` +
      `🧮 <b>Qty:</b> <code>${escapeHtml(o.quantity || 1)}</code>\n` +
      `💳 <b>Total:</b> <code>${escapeHtml(o.totalAmount || 0)}</code>\n` +
      `🔎 <b>UTR:</b> <code>${escapeHtml(o.utr || "")}</code>\n`;

    try {
      if (o.screenshotFileId) {
        await bot.telegram.sendPhoto(ADMIN_CHAT_ID, o.screenshotFileId, {
          caption,
          parse_mode: "HTML",
          ...keyboard,
        });
      } else {
        await bot.telegram.sendMessage(ADMIN_CHAT_ID, caption, {
          parse_mode: "HTML",
          ...keyboard,
        });
      }
    } catch (_) {}
  }
});

bot.action("admin:empty", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");
  await ctx.answerCbQuery();

  const keyboard = Markup.inlineKeyboard([
    [Markup.button.callback("✅ Yes, delete ALL codes", "admin:empty:do")],
    [Markup.button.callback("↩️ Cancel", "admin:config:cancel")],
  ]);

  await ctx.editMessageText(
    "🧹 <b>Empty codes</b>\n\nThis will delete <b>all</b> stored codes from the database.\n\nAre you sure?",
    { parse_mode: "HTML", reply_markup: keyboard.reply_markup, disable_web_page_preview: true }
  );
});

bot.action("admin:empty:do", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");
  await ctx.answerCbQuery("Deleting...");

  const codesCol = getCollection(COLLECTIONS.CODES);
  const res = await codesCol.deleteMany({});

  const remaining = await codesCol.countDocuments({});
  const keyboard = Markup.inlineKeyboard([
    [Markup.button.callback("Configure Required Channel(s)", "admin:config:channels")],
    [Markup.button.callback("Set Category Prices", "admin:config:prices")],
    [Markup.button.callback("Upload Payment QR (photo)", "admin:config:qr")],
    [Markup.button.callback("Upload Codes (1 per line)", "admin:config:codes")],
    [Markup.button.callback("🧹 Empty Codes", "admin:empty")],
    [Markup.button.callback("🕒 Pending Requests", "admin:pending")],
    [Markup.button.callback("View last payments", "admin:view:history")],
    [Markup.button.callback("Export payments (CSV)", "admin:export")],
  ]);

  try {
    await ctx.editMessageText(
      `✅ Codes emptied successfully.\nDeleted: ${res.deletedCount}\nRemaining: ${remaining}`,
      { reply_markup: keyboard.reply_markup, disable_web_page_preview: true }
    );
  } catch (_) {
    await ctx.reply(
      `✅ Codes emptied successfully.\nDeleted: ${res.deletedCount}\nRemaining: ${remaining}`,
      { reply_markup: keyboard.reply_markup, disable_web_page_preview: true }
    );
  }
});

bot.action("admin:config:channels", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");
  await ctx.answerCbQuery();
  adminStates.set(ctx.from.id, { expecting: "set_channels" });
  await ctx.editMessageText(
    "Send the required channel link(s)/username(s).\nOne per line.\nExamples:\n`@channelusername`\n`https://t.me/channelusername`",
    {
      parse_mode: "Markdown",
      reply_markup: Markup.inlineKeyboard([[Markup.button.callback("Cancel", "admin:config:cancel")]]).reply_markup,
    }
  );
});

bot.action("admin:config:qr", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");
  await ctx.answerCbQuery();
  adminStates.set(ctx.from.id, { expecting: "set_qr" });
  await ctx.editMessageText("Send the Payment QR as a *photo* (PNG/JPG).", {
    parse_mode: "Markdown",
    reply_markup: Markup.inlineKeyboard([[Markup.button.callback("Cancel", "admin:config:cancel")]]).reply_markup,
  });
});

bot.action("admin:config:prices", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");
  await ctx.answerCbQuery();
  adminStates.set(ctx.from.id, { expecting: "set_prices" });
  await ctx.editMessageText(
    "Send category prices (one per line):\n" +
      "`500: <price>`\n`1000: <price>`\n`2000: <price>`\n`4000: <price>`\n\nExample:\n`500: 50`\n`1000: 100`",
    {
      parse_mode: "Markdown",
      reply_markup: Markup.inlineKeyboard([[Markup.button.callback("Cancel", "admin:config:cancel")]]).reply_markup,
    }
  );
});

bot.action("admin:config:codes", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");
  await ctx.answerCbQuery();
  adminStates.set(ctx.from.id, { expecting: "upload_codes" });
  await ctx.editMessageText(
    "Upload codes as a document (TXT/CSV/Paste).\nRequired format: one code per line.\nExample:\n`SVIxxxx`\n`SVCyyyy`\n\nCodes must be uppercase (the bot will uppercase them).",
    {
      reply_markup: Markup.inlineKeyboard([[Markup.button.callback("Cancel", "admin:config:cancel")]]).reply_markup,
      disable_web_page_preview: true,
    }
  );
});

bot.action("admin:view:history", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");
  await ctx.answerCbQuery();

  const orders = getCollection(COLLECTIONS.ORDERS);
  const list = await orders.find({}).sort({ createdAt: -1 }).limit(10).toArray();
  if (!list.length) {
    await ctx.editMessageText("No payment history yet.");
    return;
  }

  const lines = list.map((o) => {
    const who = o.username ? `@${o.username}` : `id:${o.userId}`;
    const codes = o.deliveredCodes?.length ? `Codes: ${o.deliveredCodes.join(", ")}` : "";
    const when = o.createdAt ? new Date(o.createdAt).toISOString() : "";
    return `• ${o.orderId}\n  ${when}\n  ${o.category} x${o.quantity || 1} | Total: ${o.totalAmount || 0}\n  ${o.status}\n  UTR: ${o.utr || ""}\n  User: ${who}\n  ${codes}`;
  });

  await ctx.editMessageText(`Last payments:\n\n${lines.join("\n\n")}`, {
    parse_mode: "Markdown",
    disable_web_page_preview: true,
  });
});

bot.action("admin:config:cancel", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");
  await ctx.answerCbQuery();
  adminStates.delete(ctx.from.id);
  const keyboard = Markup.inlineKeyboard([
    [Markup.button.callback("Configure Required Channel(s)", "admin:config:channels")],
    [Markup.button.callback("Set Category Prices", "admin:config:prices")],
    [Markup.button.callback("Upload Payment QR (photo)", "admin:config:qr")],
    [Markup.button.callback("Upload Codes (1 per line)", "admin:config:codes")],
    [Markup.button.callback("🕒 Pending Requests", "admin:pending")],
    [Markup.button.callback("View last payments", "admin:view:history")],
    [Markup.button.callback("Export payments (CSV)", "admin:export")],
  ]);
  try {
    await ctx.editMessageText("Admin panel:", keyboard);
  } catch (_) {
    await ctx.reply("Admin panel:", keyboard);
  }
});

bot.action("admin:export", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return ctx.answerCbQuery("Not allowed.");
  await ctx.answerCbQuery("Preparing CSV...");
  await exportPaymentsCsv(ctx);
});

// Admin code import: send a CSV document
bot.on("document", async (ctx) => {
  if (ctx.from.id !== ADMIN_CHAT_ID) return;
  const adminState = adminStates.get(ctx.from.id);
  if (
    adminState?.expecting === "set_channels" ||
    adminState?.expecting === "set_qr" ||
    adminState?.expecting === "set_prices"
  ) {
    await ctx.reply("You're in admin configuration mode. Finish by sending the requested input (text/photo).");
    return;
  }

  const doc = ctx.message.document;
  if (!doc) return;

  const fileId = doc.file_id;
  const fileBuf = await downloadTelegramFile(fileId);
  const csvText = fileBuf.toString("utf8");

  const codes = parseCsvCodes(csvText);

  if (codes.length === 0) {
    await ctx.reply("CSV parsed, but no codes found.");
    return;
  }

  await importCodesToDb(codes, ctx);
  adminStates.delete(ctx.from.id);
});

async function main() {
  startHealthServer();
  await connectDb(MONGODB_URI);
  dbReady = true;
  bot.launch().then(() => {
    console.log("Bot started.");
  });
  // Expiry loop
  setInterval(() => runOrderExpiryLoop().catch(() => {}), 15_000);
}

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled rejection:", reason);
});

process.on("uncaughtException", (err) => {
  console.error("Uncaught exception:", err);
});

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

process.once("SIGINT", async () => {
  await closeDb();
  process.exit(0);
});

process.once("SIGTERM", async () => {
  await closeDb();
  process.exit(0);
});

