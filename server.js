// ============================================================================
// سيرفر الديوان العسكري — Express + Socket.IO
// ----------------------------------------------------------------------------
// يوفّر بالضبط الواجهة (API) التي يتوقّعها index.html الحالي:
//   POST /api/sync/operations   — استقبال تعديلات العميل وتخزينها (Optimistic
//                                  Concurrency عبر baseRevision/revision)
//   GET  /api/sync/bootstrap    — تحميل الحالة الحالية (أو ما تغيّر منذ cursor)
//   GET  /api/whoami            — هوية المستخدم المصادَق حسب Basic Auth
//   Socket.IO event 'sync:operations' — بثّ فوري لأي تعديل مقبول لكل الجلسات
//                                  المتصلة حالياً (هذا هو "التحديث الحي")
//
// التخزين: ملف JSON واحد على القرص (DATA_DIR/sync-store.json) — أي تعديل
// يُكتب فوراً إلى القرص (لا شيء يبقى في الذاكرة فقط)، مع طابور كتابة بسيط
// يمنع تعارض الكتابات المتزامنة على نفس الملف.
// ============================================================================

const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs/promises');
const fssync = require('fs');
const { Server: SocketIOServer } = require('socket.io');
const compression = require('compression');

// ----------------------------------------------------------------------------
// الإعدادات (كلها قابلة للتغيير عبر متغيرات البيئة على Railway)
// ----------------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
// مجلد البيانات — على Railway اربطه بـ Volume دائم حتى لا تُفقد البيانات
// عند كل نشر جديد (راجع README.md قسم "التخزين الدائم على Railway").
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const STORE_FILE = path.join(DATA_DIR, 'sync-store.json');
const PUBLIC_DIR = path.join(__dirname, 'public');

// حسابات الدخول (Basic Auth) بثلاثة أدوار — نفس نظام مشروع Five66:
//   ADMIN: صلاحية كاملة (قراءة/كتابة)
//   APP:   صلاحية كاملة (قراءة/كتابة) — للاستخدام العادي من التطبيق
//   VIEWER: قراءة فقط (لا يمكنه إرسال تعديلات)
const ACCOUNTS = {
  [process.env.ADMIN_USER || 'admin']: { pass: process.env.ADMIN_PASS || 'change-me-admin', role: 'admin' },
  [process.env.APP_USER || 'app']: { pass: process.env.APP_PASS || 'change-me-app', role: 'app' },
  [process.env.VIEWER_USER || 'viewer']: { pass: process.env.VIEWER_PASS || 'change-me-viewer', role: 'viewer' },
};

// ----------------------------------------------------------------------------
// طبقة التخزين (Store) — تحميل/حفظ + طابور كتابة بسيط لمنع التعارض
// ----------------------------------------------------------------------------
let store = { seq: 0, records: {}, log: [] };
let writeChain = Promise.resolve(); // طابور يضمن كتابة واحدة في كل مرة على القرص

async function safeMkdir(dir) {
  // على بعض أنظمة تثبيت الـ Volumes (مثل Railway)، قد يرمي mkdir بخطأ EEXIST
  // حتى مع recursive:true إن كان المسار هو نقطة تثبيت الـ Volume نفسها —
  // هذا ليس خطأً فعلياً (المجلد موجود فعلاً وهذا هو المطلوب)، فنتجاهله فقط.
  try {
    await fs.mkdir(dir, { recursive: true });
  } catch (e) {
    if (e.code !== 'EEXIST') throw e;
  }
}

async function ensureDataDir() {
  await safeMkdir(DATA_DIR);
}

async function loadStore() {
  await ensureDataDir();
  try {
    const raw = await fs.readFile(STORE_FILE, 'utf8');
    store = JSON.parse(raw);
    if (!store.records) store.records = {};
    if (!store.log) store.log = [];
    if (typeof store.seq !== 'number') store.seq = 0;
  } catch (e) {
    if (e.code === 'ENOENT') {
      store = { seq: 0, records: {}, log: [] };
      await persistStore(); // أنشئ الملف من أول مرة
    } else {
      throw e;
    }
  }
}

function persistStore() {
  // نُسلسل الكتابات: كل كتابة تنتظر انتهاء سابقتها، وتُكتب عبر ملف مؤقت ثم
  // rename ذرّي حتى لا يُترك الملف في حالة نصف-مكتوبة عند انقطاع مفاجئ.
  writeChain = writeChain.then(async () => {
    const tmpFile = STORE_FILE + '.tmp';
    await fs.writeFile(tmpFile, JSON.stringify(store), 'utf8');
    await fs.rename(tmpFile, STORE_FILE);
  });
  return writeChain;
}

// ----------------------------------------------------------------------------
// نسخ احتياطية محلية دورية — طبقة أمان إضافية فوق الملف الحي نفسه، بحيث لو
// تلف sync-store.json أو حصل تعديل خاطئ جماعي، يمكن الرجوع لنسخة سابقة.
// تُحفَظ داخل DATA_DIR/backups (نفس القرص الدائم) وتُبقي آخر BACKUP_KEEP نسخة فقط.
// ----------------------------------------------------------------------------
const BACKUP_DIR = path.join(DATA_DIR, 'backups');
const BACKUP_INTERVAL_MS = 6 * 60 * 60 * 1000; // كل 6 ساعات
const BACKUP_KEEP = 12; // آخر 12 نسخة (= يومان تقريباً بمعدل كل 6 ساعات)

async function runBackup() {
  try {
    await safeMkdir(BACKUP_DIR);
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const dest = path.join(BACKUP_DIR, `sync-store-${stamp}.json`);
    await fs.copyFile(STORE_FILE, dest);
    const files = (await fs.readdir(BACKUP_DIR)).filter((f) => f.startsWith('sync-store-')).sort();
    const excess = files.length - BACKUP_KEEP;
    if (excess > 0) {
      for (const f of files.slice(0, excess)) await fs.unlink(path.join(BACKUP_DIR, f));
    }
  } catch (e) {
    console.warn('تعذّر إنشاء نسخة احتياطية:', e.message);
  }
}

// ----------------------------------------------------------------------------
// Express app
// ----------------------------------------------------------------------------
const app = express();
app.use(compression()); // ضغط gzip — مهم جداً لأن index.html بحجم ~8 ميجابايت
app.use(express.json({ limit: '25mb' })); // الصور Base64 قد تجعل الحمولة كبيرة
// جسم طلب تالف (JSON غير صالح) كان سيصل لمعالج الأخطاء الافتراضي في Express
// ويُظهر صفحة HTML فيها تفاصيل داخلية — هنا نرجع خطأ نظيف ومختصر بدلاً من ذلك.
app.use((err, req, res, next) => {
  if (err && err.type === 'entity.parse.failed') {
    return res.status(400).json({ error: 'invalid JSON body' });
  }
  next(err);
});

// فحص صحة بسيط بدون مصادقة — يفيد Railway healthcheck
app.get('/healthz', (req, res) => res.status(200).send('ok'));

// ----------------------------------------------------------------------------
// Basic Auth — يطبَّق على كل شيء ما عدا /healthz
// ----------------------------------------------------------------------------
function parseBasicAuth(req) {
  const header = req.headers.authorization || '';
  if (!header.startsWith('Basic ')) return null;
  try {
    const decoded = Buffer.from(header.slice(6), 'base64').toString('utf8');
    const idx = decoded.indexOf(':');
    if (idx === -1) return null;
    const user = decoded.slice(0, idx);
    const pass = decoded.slice(idx + 1);
    const account = ACCOUNTS[user];
    if (account && account.pass === pass) return { user, role: account.role };
    return null;
  } catch (e) {
    return null;
  }
}

function requireAuth(req, res, next) {
  const auth = parseBasicAuth(req);
  if (!auth) {
    // تسجيل تشخيصي: يساعد على معرفة سبب فشل أي طلب مزامنة لا يصل بنجاح —
    // خصوصاً بعد تغيير كلمات المرور، حيث يبقى المتصفح يرسل بيانات قديمة محفوظة.
    console.warn(`[auth] رُفض طلب بدون/بمصادقة خاطئة: ${req.method} ${req.path}`);
    // ملاحظة: قيمة ترويسة WWW-Authenticate يجب أن تكون بأحرف ASCII فقط —
    // Node.js يرمي خطأ ERR_INVALID_CHAR إن احتوت على أحرف عربية (يونيكود)،
    // لذلك النص هنا بالإنجليزية فقط (لا يظهر للمستخدم عادة، فقط اسم داخلي للمتصفح).
    res.set('WWW-Authenticate', 'Basic realm="Diwan Al-Askari"');
    return res.status(401).send('Authentication required');
  }
  req.authed = auth;
  next();
}

app.use(requireAuth);

// من هو المستخدم الحالي (يستخدمه العميل لعرض الاسم في التنبيهات)
app.get('/api/whoami', (req, res) => {
  res.json({ user: req.authed.user, role: req.authed.role });
});

// ----------------------------------------------------------------------------
// /api/sync/operations — استقبال دفعة تعديلات (Optimistic Concurrency)
// ----------------------------------------------------------------------------
app.post('/api/sync/operations', async (req, res) => {
  if (req.authed.role === 'viewer') {
    console.warn(`[sync] رفض كتابة من حساب قراءة-فقط: ${req.authed.user}`);
    return res.status(403).json({ error: 'viewer role is read-only' });
  }
  const operations = (req.body && req.body.operations) || [];
  console.log(`[sync] طلب من ${req.authed.user} — عدد العمليات: ${operations.length}`);
  if (!Array.isArray(operations) || !operations.length) {
    return res.json({ results: [], serverSequence: store.seq });
  }

  const results = [];
  const acceptedForBroadcast = [];
  const now = Date.now();

  for (const op of operations) {
    if (!op || !op.opId || !op.recordId || op.type !== 'upsert') {
      results.push({ opId: op && op.opId, status: 'invalid', message: 'malformed operation' });
      continue;
    }
    const current = store.records[op.recordId];
    const currentRevision = current ? current.revision : 0;
    const baseRevision = op.baseRevision || 0;

    if (baseRevision !== currentRevision) {
      // تعارض: جهاز آخر عدّل هذا المفتاح قبلنا — أعد للعميل المراجعة الحقيقية
      results.push({
        opId: op.opId,
        status: 'conflict',
        record: current
          ? { id: op.recordId, revision: current.revision, payload: current.payload }
          : { id: op.recordId, revision: 0, payload: null },
      });
      continue;
    }

    const newRevision = currentRevision + 1;
    const record = {
      id: op.recordId,
      collection: op.collection || 'app_state',
      revision: newRevision,
      payload: op.payload,
      actor: req.authed.user, // الهوية تُستمد من مصادقة السيرفر لا من العميل
      updatedAt: now,
    };
    store.records[op.recordId] = record;
    store.seq += 1;
    const logEntry = { seq: store.seq, opId: op.opId, record, actor: req.authed.user, ts: now };
    store.log.push(logEntry);

    results.push({ opId: op.opId, status: 'accepted', record: { id: record.id, revision: record.revision, payload: record.payload } });
    acceptedForBroadcast.push(logEntry);
  }

  try {
    await persistStore();
  } catch (e) {
    console.error(`[sync] فشل حفظ الملف على القرص:`, e.message);
    return res.status(500).json({ error: 'failed to persist to disk', message: e.message });
  }

  const accepted = results.filter((r) => r.status === 'accepted').length;
  const conflicts = results.filter((r) => r.status === 'conflict').length;
  console.log(`[sync] النتيجة — مقبول: ${accepted}، تعارض: ${conflicts}، بثّ لعدد اتصالات: ${io.engine.clientsCount}`);

  if (acceptedForBroadcast.length) {
    io.emit('sync:operations', {
      operations: acceptedForBroadcast.map((e) => ({
        opId: e.opId,
        // seq يُرفَق بالبث حتى يحدّث العميل مؤشر التسلسل لديه مباشرة — أي جلب
        // لاحق عبر /api/sync/bootstrap سيبدأ من بعده فلا تُعاد نفس العمليات.
        seq: e.seq,
        record: { id: e.record.id, revision: e.record.revision, payload: e.record.payload },
        actor: e.actor,
      })),
    });
  }

  res.json({ results, serverSequence: store.seq });
});

// ----------------------------------------------------------------------------
// /api/sync/bootstrap — تحميل الحالة الحالية، أو ما تغيّر منذ since=cursor
// ----------------------------------------------------------------------------
const PAGE_SIZE = 500;

app.get('/api/sync/bootstrap', (req, res) => {
  const since = parseInt(req.query.since, 10) || 0;

  if (since === 0) {
    // عميل جديد بالكامل: أرسل أحدث نسخة من كل سجل مباشرة (أسرع بكثير من
    // إعادة تشغيل كامل السجل التاريخي، والنتيجة النهائية متطابقة تماماً
    // لأن العميل أصلاً يحتفظ فقط بآخر عملية لكل مفتاح).
    const operations = Object.values(store.records).map((r) => ({
      opId: 'snapshot_' + r.id,
      record: { id: r.id, revision: r.revision, payload: r.payload },
      actor: r.actor,
    }));
    return res.json({ operations, serverSequence: store.seq, hasMore: false });
  }

  const page = store.log.filter((e) => e.seq > since).slice(0, PAGE_SIZE);
  const hasMore = store.log.filter((e) => e.seq > since).length > PAGE_SIZE;
  const operations = page.map((e) => ({
    opId: e.opId,
    record: { id: e.record.id, revision: e.record.revision, payload: e.record.payload },
    actor: e.actor,
  }));
  const serverSequence = page.length ? page[page.length - 1].seq : store.seq;
  res.json({ operations, serverSequence, hasMore });
});

// ----------------------------------------------------------------------------
// ملفات الصور المنفصلة (persons-photos.json / shamcash-photos.json) — تُقرأ
// من public/ مباشرة عبر express.static أدناه (بعد الآيدنتِتي/المصادقة).
// ----------------------------------------------------------------------------
app.use(express.static(PUBLIC_DIR));

// أي مسار آخر غير معروف يعيد index.html (Single Page fallback)
app.get('*', (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
});

// ----------------------------------------------------------------------------
// تشغيل السيرفر + Socket.IO
// ----------------------------------------------------------------------------
const httpServer = http.createServer(app);
const io = new SocketIOServer(httpServer, {
  // يستخدم المتصفح نفس ترويسة Basic Auth تلقائياً في طلب الـ handshake لأنه
  // نفس origin — نتحقق منها هنا بنفس طريقة الـ REST API فتماماً.
});

io.use((socket, next) => {
  const auth = parseBasicAuth(socket.request);
  if (!auth) {
    console.warn('[socket] رُفض اتصال بدون مصادقة صحيحة');
    return next(new Error('unauthorized'));
  }
  socket.authed = auth;
  next();
});

// قائمة الحضور الحية: أسماء الحسابات المتصلة حالياً عبر Socket.IO.
// تُبثّ لكل المتصلين عند كل دخول/خروج حتى يرى الجميع من هو موجود الآن.
function broadcastPresence() {
  const users = new Set();
  for (const [, s] of io.sockets.sockets) {
    if (s.authed && s.authed.user) users.add(s.authed.user);
  }
  io.emit('presence:update', { online: [...users] });
}

io.on('connection', (socket) => {
  console.log(`[socket] اتصال جديد — المستخدم: ${socket.authed.user} — إجمالي المتصلين الآن: ${io.engine.clientsCount}`);
  broadcastPresence();
  socket.on('disconnect', (reason) => {
    console.log(`[socket] قطع اتصال — المستخدم: ${socket.authed.user} — السبب: ${reason}`);
    // نؤجّل البث لحظة واحدة حتى يُزال الـ socket المنقطع فعلاً من قائمة الاتصالات.
    setTimeout(broadcastPresence, 50);
  });
});

async function main() {
  await loadStore();
  await runBackup(); // نسخة عند الإقلاع أيضاً، فوق الجدولة الدورية
  setInterval(runBackup, BACKUP_INTERVAL_MS);
  httpServer.listen(PORT, () => {
    console.log(`✓ الديوان العسكري — السيرفر يعمل على المنفذ ${PORT}`);
    console.log(`✓ مجلد البيانات: ${DATA_DIR}`);
  });
}

// إغلاق آمن: عند إعادة نشر Railway أو إيقاف يدوي، السيرفر يتلقى SIGTERM —
// ننتظر انتهاء أي كتابة قيد التنفيذ على القرص قبل الخروج فعلياً، حتى لا
// يُفقَد آخر تعديل وصل للتو ولم يُكتب بعد.
async function gracefulShutdown(signal) {
  console.log(`\n${signal} — إغلاق آمن، انتظار اكتمال آخر كتابة على القرص...`);
  try {
    await writeChain;
  } catch (e) {}
  httpServer.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000); // شبكة أمان لو تعلّق شيء
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

main().catch((err) => {
  console.error('فشل بدء تشغيل السيرفر:', err);
  process.exit(1);
});
