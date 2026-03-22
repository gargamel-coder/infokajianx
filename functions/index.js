/**
 * Info Kajian – Firebase Cloud Functions
 * sendKajianNotifications: Kirim FCM push ke subscriber saat kajian baru/diapprove
 *
 * Arsitektur:
 *  - Trigger  : onCreate + onUpdate di koleksi `kajian`
 *  - Fan-out  : Query userData berdasarkan field subscription (bukan scan semua dok)
 *  - Pengiriman: Admin SDK sendEachForMulticast (batch 500 token)
 *  - Cleanup  : Token yang expired/invalid dihapus otomatis
 */

const { onDocumentCreated, onDocumentUpdated } = require("firebase-functions/v2/firestore");
const { initializeApp }   = require("firebase-admin/app");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");
const { getMessaging }    = require("firebase-admin/messaging");
const { logger }          = require("firebase-functions");

initializeApp();

const db  = getFirestore();
const fcm = getMessaging();

// ─── Konstanta ────────────────────────────────────────────────────────────────
const PWA_URL    = "https://YOUR-PROJECT.web.app";   // ← ganti dengan domain PWA kamu
const ICON_URL   = `${PWA_URL}/icons/icon-192.png`;
const BADGE_URL  = `${PWA_URL}/icons/badge-72.png`;
const FCM_BATCH  = 500;   // batas multicast Firebase
const MAX_TOKENS = 2000;  // batas token per eksekusi (safety limit)

// ─── Helper: ambil semua FCM token dari daftar UID ───────────────────────────
/**
 * Mengambil FCM tokens dari Firestore secara paralel.
 * Mengembalikan Map<token, uid> agar bisa cleanup token invalid nanti.
 *
 * @param {string[]} uids
 * @returns {Promise<Map<string, string>>}  token → uid
 */
async function fetchTokenMap(uids) {
  const tokenMap = new Map();
  if (!uids.length) return tokenMap;

  // Deduplicate uids
  const uniqueUids = [...new Set(uids)];

  // Firestore 'in' query maks 30 item, chunking diperlukan
  const CHUNK = 30;
  const chunks = [];
  for (let i = 0; i < uniqueUids.length; i += CHUNK) {
    chunks.push(uniqueUids.slice(i, i + CHUNK));
  }

  const snapshots = await Promise.all(
    chunks.map(chunk =>
      db.collection("userData")
        .where("__name__", "in", chunk)
        .select("fcmTokens")   // hanya ambil field yang dibutuhkan
        .get()
    )
  );

  for (const snap of snapshots) {
    for (const doc of snap.docs) {
      const tokens = doc.data()?.fcmTokens;
      if (Array.isArray(tokens)) {
        for (const token of tokens) {
          if (token && typeof token === "string") {
            tokenMap.set(token, doc.id);
          }
        }
      }
    }
  }

  return tokenMap;
}

// ─── Helper: cari subscriber UID dari kajian data ────────────────────────────
/**
 * Query userData collection untuk mencari semua user yang subscribe ke:
 *  - speaker tertentu (subscriptions.speakers array-contains)
 *  - mosque tertentu  (subscriptions.mosques  array-contains)
 *  - contributor UID  (subscriptions.contributors array-contains)
 *
 * Kita jalankan 3 query paralel lalu union hasilnya.
 *
 * @param {object} kajianData
 * @returns {Promise<string[]>} array uid subscriber unik
 */
async function findSubscriberUids(kajianData) {
  const queries = [];

  // Speaker subscription
  if (kajianData.speakerId) {
    queries.push(
      db.collection("userData")
        .where("subscriptions.speakers", "array-contains", kajianData.speakerId)
        .select()   // hanya butuh doc ID
        .get()
    );
  }

  // Mosque subscription
  if (kajianData.mosqueId) {
    queries.push(
      db.collection("userData")
        .where("subscriptions.mosques", "array-contains", kajianData.mosqueId)
        .select()
        .get()
    );
  }

  // Contributor subscription
  if (kajianData.contributorId) {
    queries.push(
      db.collection("userData")
        .where("subscriptions.contributors", "array-contains", kajianData.contributorId)
        .select()
        .get()
    );
  }

  if (!queries.length) return [];

  const results = await Promise.all(queries);
  const uidSet  = new Set();
  for (const snap of results) {
    for (const doc of snap.docs) uidSet.add(doc.id);
  }

  // Jangan kirim notif ke contributor pembuat kajian itu sendiri
  if (kajianData.contributorId) uidSet.delete(kajianData.contributorId);

  return [...uidSet];
}

// ─── Helper: cleanup token invalid ───────────────────────────────────────────
/**
 * Hapus token yang expired/unregistered dari Firestore.
 * Dipanggil setelah multicast, berdasarkan error responses.
 *
 * @param {import("firebase-admin/messaging").SendEachForMulticastResponse} response
 * @param {string[]} tokens  token yang dikirim (urut sama dengan response)
 * @param {Map<string, string>} tokenToUid
 */
async function cleanupInvalidTokens(response, tokens, tokenToUid) {
  const invalidCodes = new Set([
    "messaging/registration-token-not-registered",
    "messaging/invalid-registration-token",
    "messaging/invalid-argument",
  ]);

  const toRemove = [];   // { uid, token }

  response.responses.forEach((res, idx) => {
    if (!res.success && res.error) {
      const code = res.error.code;
      if (invalidCodes.has(code)) {
        const token = tokens[idx];
        const uid   = tokenToUid.get(token);
        if (uid) toRemove.push({ uid, token });
      }
    }
  });

  if (!toRemove.length) return;

  // Batched Firestore writes
  const WRITE_BATCH = 400;
  for (let i = 0; i < toRemove.length; i += WRITE_BATCH) {
    const batch = db.batch();
    for (const { uid, token } of toRemove.slice(i, i + WRITE_BATCH)) {
      const ref = db.collection("userData").doc(uid);
      batch.update(ref, { fcmTokens: FieldValue.arrayRemove(token) });
    }
    await batch.commit();
  }

  logger.info(`[FCM] Cleaned up ${toRemove.length} invalid token(s).`);
}

// ─── Helper: simpan notifikasi ke Firestore (in-app fallback) ────────────────
/**
 * Tulis notif ke userData.{uid}.notifications – dipakai sebagai in-app notif
 * yang dibaca real-time listener di client.
 *
 * @param {string[]} uids
 * @param {object}   payload  { title, body, kajianId, kajianTitle }
 */
async function saveNotificationsToFirestore(uids, payload) {
  if (!uids.length) return;

  const notif = {
    id       : db.collection("_").doc().id,   // generate random ID
    title    : payload.title,
    body     : payload.body,
    kajianId : payload.kajianId,
    read     : false,
    createdAt: FieldValue.serverTimestamp(),
  };

  const WRITE_BATCH = 400;
  for (let i = 0; i < uids.length; i += WRITE_BATCH) {
    const batch = db.batch();
    for (const uid of uids.slice(i, i + WRITE_BATCH)) {
      const ref = db.collection("userData").doc(uid);
      batch.update(ref, {
        notifications: FieldValue.arrayUnion(notif),
      });
    }
    await batch.commit();
  }

  logger.info(`[FCM] Saved in-app notification for ${uids.length} user(s).`);
}

// ─── Helper: kirim FCM multicast (chunked 500) ───────────────────────────────
/**
 * @param {string[]} tokens
 * @param {import("firebase-admin/messaging").MulticastMessage} message
 * @param {Map<string, string>} tokenToUid  untuk cleanup
 */
async function sendMulticast(tokens, message, tokenToUid) {
  let totalSuccess = 0;
  let totalFail    = 0;

  for (let i = 0; i < tokens.length; i += FCM_BATCH) {
    const chunk = tokens.slice(i, i + FCM_BATCH);
    const msg   = { ...message, tokens: chunk };

    try {
      const response = await fcm.sendEachForMulticast(msg);
      totalSuccess += response.successCount;
      totalFail    += response.failureCount;

      if (response.failureCount > 0) {
        await cleanupInvalidTokens(response, chunk, tokenToUid);
      }
    } catch (err) {
      logger.error("[FCM] sendEachForMulticast error:", err);
    }
  }

  logger.info(`[FCM] Sent: ${totalSuccess} success, ${totalFail} fail.`);
}

// ─── Core: proses kajian dan kirim notif ─────────────────────────────────────
/**
 * @param {string} kajianId
 * @param {object} kajianData
 */
async function processKajian(kajianId, kajianData) {
  if (kajianData.status !== "approved") {
    logger.info(`[FCM] Skipping kajian ${kajianId}: status = ${kajianData.status}`);
    return;
  }

  // Tandai agar tidak diproses 2x (idempoten)
  if (kajianData.fcmSent === true) {
    logger.info(`[FCM] Skipping kajian ${kajianId}: already sent.`);
    return;
  }

  const speakerName = kajianData.speakerName || "Ustadz";
  const mosqueName  = kajianData.mosqueName  || "Masjid";
  const kajianTitle = kajianData.title        || "Kajian Baru";
  const dateStr     = kajianData.date         || "";

  const notifTitle = `📚 ${kajianTitle}`;
  const notifBody  = `${speakerName} · ${mosqueName}${dateStr ? " · " + dateStr : ""}`;

  // 1. Cari subscriber
  const subscriberUids = await findSubscriberUids(kajianData);
  logger.info(`[FCM] Found ${subscriberUids.length} subscriber(s) for kajian ${kajianId}.`);

  if (!subscriberUids.length) {
    // Tandai fcmSent walau tidak ada subscriber
    await db.collection("kajian").doc(kajianId).update({ fcmSent: true });
    return;
  }

  // Safety limit
  const targetUids = subscriberUids.slice(0, MAX_TOKENS);

  // 2. Ambil FCM tokens
  const tokenToUid = await fetchTokenMap(targetUids);
  const tokens     = [...tokenToUid.keys()];
  logger.info(`[FCM] Collected ${tokens.length} FCM token(s).`);

  // 3. Simpan in-app notif ke Firestore (real-time listener di client akan update badge)
  await saveNotificationsToFirestore(targetUids, {
    title     : notifTitle,
    body      : notifBody,
    kajianId,
    kajianTitle,
  });

  // 4. Kirim FCM push (hanya ke yang punya token)
  if (tokens.length > 0) {
    /** @type {import("firebase-admin/messaging").MulticastMessage} */
    const message = {
      tokens,   // akan di-assign per chunk di sendMulticast
      notification: {
        title: notifTitle,
        body : notifBody,
      },
      webpush: {
        notification: {
          title  : notifTitle,
          body   : notifBody,
          icon   : ICON_URL,
          badge  : BADGE_URL,
          vibrate: [200, 100, 200],
          requireInteraction: false,
          actions: [
            { action: "open_kajian", title: "Lihat Kajian" },
            { action: "dismiss",     title: "Tutup" },
          ],
        },
        fcmOptions: {
          link: `${PWA_URL}/#/kajian/${kajianId}`,
        },
        data: {
          kajianId,
          kajianTitle,
          url: `${PWA_URL}/#/kajian/${kajianId}`,
          click_action: "OPEN_KAJIAN",
        },
      },
      android: {
        notification: {
          channelId: "kajian_channel",
          priority : "high",
          icon     : "ic_notification",
          color    : "#2D6A4F",
        },
        data: {
          kajianId,
          click_action: "OPEN_KAJIAN",
        },
      },
      apns: {
        payload: {
          aps: {
            badge: 1,
            sound: "default",
          },
        },
        fcmOptions: {
          imageUrl: ICON_URL,
        },
      },
    };

    await sendMulticast(tokens, message, tokenToUid);
  }

  // 5. Tandai kajian sudah diproses (idempoten)
  await db.collection("kajian").doc(kajianId).update({ fcmSent: true });
  logger.info(`[FCM] Done processing kajian ${kajianId}.`);
}

// ─── Export Cloud Functions ───────────────────────────────────────────────────

/**
 * Trigger: Kajian baru dibuat langsung dengan status approved
 * (misal: admin create kajian)
 */
exports.onKajianCreated = onDocumentCreated(
  {
    document: "kajian/{kajianId}",
    region  : "asia-southeast1",   // Jakarta – pilih region terdekat
    timeoutSeconds: 120,
    memory : "256MiB",
  },
  async (event) => {
    const kajianId   = event.params.kajianId;
    const kajianData = event.data?.data();
    if (!kajianData) return;

    logger.info(`[FCM] onKajianCreated triggered: ${kajianId}`);
    await processKajian(kajianId, kajianData);
  }
);

/**
 * Trigger: Status kajian diupdate menjadi "approved"
 * (contributor submit → admin approve)
 */
exports.onKajianUpdated = onDocumentUpdated(
  {
    document: "kajian/{kajianId}",
    region  : "asia-southeast1",
    timeoutSeconds: 120,
    memory : "256MiB",
  },
  async (event) => {
    const kajianId   = event.params.kajianId;
    const before     = event.data?.before?.data();
    const after      = event.data?.after?.data();

    if (!before || !after) return;

    // Hanya proses jika status BARU berubah menjadi "approved"
    const statusChanged    = before.status !== after.status;
    const nowApproved      = after.status  === "approved";
    const alreadySent      = after.fcmSent  === true;

    if (!statusChanged || !nowApproved || alreadySent) {
      logger.info(`[FCM] onKajianUpdated skip: ${kajianId} – statusChanged=${statusChanged}, nowApproved=${nowApproved}, alreadySent=${alreadySent}`);
      return;
    }

    logger.info(`[FCM] onKajianUpdated triggered: ${kajianId} → approved`);
    await processKajian(kajianId, after);
  }
);
