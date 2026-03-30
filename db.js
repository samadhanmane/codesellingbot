const { MongoClient } = require("mongodb");

let client;
let db;

const COLLECTIONS = {
  CODES: "codes",
  ORDERS: "orders",
  CONFIG: "config",
};

async function connectDb(mongoUri, options = {}) {
  if (client) return db;
  client = new MongoClient(mongoUri, {
    maxPoolSize: 20,
    ...options,
  });
  await client.connect();
  db = client.db(process.env.MONGODB_DB_NAME || "sellingbot");
  await ensureIndexes();
  return db;
}

function getCollection(name) {
  if (!db) throw new Error("MongoDB not connected yet");
  return db.collection(name);
}

async function ensureIndexes() {
  const codes = getCollection(COLLECTIONS.CODES);
  const orders = getCollection(COLLECTIONS.ORDERS);
  const config = getCollection(COLLECTIONS.CONFIG);

  // Prevent duplicates on import
  await codes.createIndex({ code: 1 }, { unique: true });

  // Fast stock lookup + atomic claiming
  await codes.createIndex({ category: 1, available: 1 });

  // Unique order identifiers
  await orders.createIndex({ orderId: 1 }, { unique: true });

  // User history browsing
  await orders.createIndex({ userChatId: 1, createdAt: -1 });
  await orders.createIndex({ userId: 1, createdAt: -1 });

  // Expiry scans
  await orders.createIndex({ expiresAt: 1, status: 1 });

  // Single global config doc: _id is unique by default, so no explicit unique index needed.
}

async function closeDb() {
  if (!client) return;
  await client.close();
  client = undefined;
  db = undefined;
}

module.exports = {
  connectDb,
  getCollection,
  closeDb,
  COLLECTIONS,
};

