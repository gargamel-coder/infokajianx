/**
 * Info Kajian – Firebase Cloud Functions
 * Trigger onCreate + onUpdate di koleksi `kajian`
 * Fix: field names disesuaikan dengan Firestore (speaker_id, mosque_id, submitted_by)
 *      notif structure disesuaikan dengan yang dibaca client (message, title, timestamp, type)
 *      batch.update → set merge:true agar tidak error kalau dokumen belum ada
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
const PWA_URL    = "https://infokajian.vercel.app";
const ICON_URL   = `${PWA_URL}/icons/icon-192.png`;
const BADGE_URL  = `${PWA_URL}/icons/badge-72.png`;
const FCM_BATCH  = 500;
const MAX_TOKENS = 2000;

// ─── Helper: format tanggal singkat ──────────────────────────────────────────
function fmtDateShort(dateStr) {
  if (!dateStr) return "";
  try {
    const d = new Date(dateStr);
    return d.toLocaleDateString("id-ID", { day: "numeric", month: "short" });
  } catch (e) {
    return dateStr;
  }
}

// ─── Helper: ambil semua FCM token dari daftar UID ───────────────────────────
async function fetchTokenMap(uids) {
  const tokenMap = new Map();
  if (!uids.length) return tokenMap;

  const uniqueUids = [...new Set(uids)];
  const CHUNK = 30;
  const chunks = [];
  for (let i = 0; i < uniqueUids.length; i += CHUNK) {
    chunks.push(uniqueUids.slice(i, i + CHUNK));
  }

  const snapshots = await Promise.all(
    chunks.map(chunk =>
      db.collection("userData")
        .where("__name__", "in", chunk)
        .select("fcmTokens")
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

// ─── Helper: cari subscriber UID ─────────────────────────────────────────────
// FIXED: pakai field name yang benar sesuai Firestore (speaker_id, mosque_id, submitted_by)
async function findSubscriberUids(kajianData) {
  const queries = [];

  // Speaker subscription — field di kajian: speaker_id
  if (kajianData.speaker_id) {
    queries.push(
      db.collection("userData")
        .where("subscriptions.speakers", "array-contains", kajianData.speaker_id)
        .select()
        .get()
    );
  }

  // Mosque subscription — field di kajian: mosque_id
  if (kajianData.mosque_id) {
    queries.push(
      db.collection("userData")
        .where("subscriptions.mosques", "array-contains", kajianData.mosque_id)
        .select()
        .get()
    );
  }

  // Contributor subscription — field di kajian: submitted_by
  if (kajianData.submitted_by) {
    queries.push(
      db.collection("userData")
        .where("subscriptions.contributors", "array-contains", kajianData.submitted_by)
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

  // Jangan kirim notif ke pembuat kajian itu sendiri
  if (kajianData.submitted_by) uidSet.delete(kajianData.submitted_by);

  return [...uidSet];
}

// ─── Helper: cleanup token invalid ───────────────────────────────────────────
async function cleanupInvalidTokens(response, tokens, tokenToUid) {
  const invalidCodes = new Set([
    "messaging/registration-token-not-registered",
    "messaging/invalid-registration-token",
    "messaging/invalid-argument",
  ]);

  const toRemove = [];
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

// ─── Helper: simpan notifikasi ke Firestore ───────────────────────────────────
// FIXED: struktur notif disesuaikan dengan yang dibaca client
//        (message, title, body, timestamp, type, kajianId, read)
//        pakai set merge:true agar tidak error kalau dokumen belum ada
async function saveNotificationsToFirestore(uids, payload) {
  if (!uids.length) return;

  const timestamp = new Date().toISOString();

  const notif = {
    title    : payload.title,
    body     : payload.body,
    message  : payload.message,
    kajianId : payload.kajianId,
    read     : false,
    timestamp: timestamp,
    type     : "new_kajian",
  };

  const WRITE_BATCH = 400;
  for (let i = 0; i < uids.length; i += WRITE_BATCH) {
    const batch = db.batch();
    for (const uid of uids.slice(i, i + WRITE_BATCH)) {
      const ref = db.collection("userData").doc(uid);
      // FIXED: set + merge:true agar tidak gagal kalau dokumen belum ada
      batch.set(ref, {
        notifications: FieldValue.arrayUnion(notif),
      }, { merge: true });
    }
    await batch.commit();
  }

  logger.info(`[FCM] Saved in-app notification for ${uids.length} user(s).`);
}

// ─── Helper: kirim FCM multicast ─────────────────────────────────────────────
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
async function processKajian(kajianId, kajianData) {
  if (kajianData.status !== "approved") {
    logger.info(`[FCM] Skipping kajian ${kajianId}: status = ${kajianData.status}`);
    return;
  }

  if (kajianData.fcmSent === true) {
    logger.info(`[FCM] Skipping kajian ${kajianId}: already sent.`);
    return;
  }

  const kajianTitle = kajianData.title || "Kajian Baru";
  const dateStr     = fmtDateShort(kajianData.date);

  // Ambil nama speaker & masjid dari collection speakers/mosques
  let speakerName = "Ustadz";
  let mosqueName  = "Masjid";

  try {
    if (kajianData.speaker_id) {
      const spkDoc = await db.collection("speakers").doc(kajianData.speaker_id).get();
      if (spkDoc.exists) speakerName = spkDoc.data().full_name || spkDoc.data().name || speakerName;
    }
  } catch (e) { logger.warn("[FCM] Failed to fetch speaker:", e); }

  try {
    if (kajianData.mosque_id) {
      const msqDoc = await db.collection("mosques").doc(kajianData.mosque_id).get();
      if (msqDoc.exists) mosqueName = msqDoc.data().name || mosqueName;
    }
  } catch (e) { logger.warn("[FCM] Failed to fetch mosque:", e); }

  const notifTitle   = `Kajian Baru - ${speakerName}`;
  const notifBody    = `"${kajianTitle}" pada ${dateStr}`;
  const notifMessage = `Kajian baru dari ${speakerName}: "${kajianTitle}" pada ${dateStr}`;

  // 1. Cari subscriber
  const subscriberUids = await findSubscriberUids(kajianData);
  logger.info(`[FCM] Found ${subscriberUids.length} subscriber(s) for kajian ${kajianId}.`);

  if (!subscriberUids.length) {
    await db.collection("kajian").doc(kajianId).update({ fcmSent: true });
    return;
  }

  const targetUids = subscriberUids.slice(0, MAX_TOKENS);

  // 2. Simpan in-app notif ke Firestore
  await saveNotificationsToFirestore(targetUids, {
    title  : notifTitle,
    body   : notifBody,
    message: notifMessage,
    kajianId,
  });

  // 3. Ambil FCM tokens dan kirim push notification
  const tokenToUid = await fetchTokenMap(targetUids);
  const tokens     = [...tokenToUid.keys()];
  logger.info(`[FCM] Collected ${tokens.length} FCM token(s).`);

  if (tokens.length > 0) {
    const message = {
      tokens,
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
          url: `${PWA_URL}/#/kajian/${kajianId}`,
        },
      },
    };

    await sendMulticast(tokens, message, tokenToUid);
  }

  // 4. Tandai kajian sudah diproses
  await db.collection("kajian").doc(kajianId).update({ fcmSent: true });
  logger.info(`[FCM] Done processing kajian ${kajianId}.`);
}

// ─── Export Cloud Functions ───────────────────────────────────────────────────

exports.onKajianCreated = onDocumentCreated(
  {
    document: "kajian/{kajianId}",
    region  : "asia-southeast1",
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

    const statusChanged = before.status !== after.status;
    const nowApproved   = after.status  === "approved";
    const alreadySent   = after.fcmSent  === true;

    if (!statusChanged || !nowApproved || alreadySent) {
      logger.info(`[FCM] onKajianUpdated skip: ${kajianId} – statusChanged=${statusChanged}, nowApproved=${nowApproved}, alreadySent=${alreadySent}`);
      return;
    }

    logger.info(`[FCM] onKajianUpdated triggered: ${kajianId} → approved`);
    await processKajian(kajianId, after);
  }
);
