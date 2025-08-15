// main.js
import { Actor } from 'apify';
import got from 'got';
import pLimit from 'p-limit';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { CookieJar } from 'tough-cookie';

const IG_APP_ID = '936619743392459';

// ============== Utils gerais ==============
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function clamp01(x) { return Math.max(0, Math.min(1, x)); }
function median(arr) {
  if (!arr.length) return 0;
  const a = [...arr].sort((x, y) => x - y);
  const m = Math.floor(a.length / 2);
  return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
}
function mean(arr) { return arr.length ? arr.reduce((s, v) => s + v, 0) / arr.length : 0; }
function stddev(arr) {
  if (arr.length < 2) return 0;
  const m = mean(arr);
  const v = mean(arr.map(x => (x - m) ** 2));
  return Math.sqrt(v);
}
function ratio(a, b) { return b ? a / b : 0; }
function normalize(text = '') {
  return text.normalize('NFD').replace(/\p{Diacritic}/gu, '').toLowerCase();
}

// ============== Headers / Cookies / Proxy ==============
let proxyConfiguration = null;
let cookieJar = new CookieJar();
let sessionPrepared = false;

function buildHeaders(username, appId, csrf = '') {
  const h = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8',
    'Origin': 'https://www.instagram.com',
    'Referer': `https://www.instagram.com/${username}/`,
    'X-IG-App-ID': appId,
    'X-Requested-With': 'XMLHttpRequest',
    'X-ASBD-ID': '129477',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
  };
  if (csrf) h['X-CSRFToken'] = csrf;
  return h;
}

function buildAgent() {
  try {
    if (!proxyConfiguration) return undefined;
    const proxyUrl = proxyConfiguration.newUrl?.();
    if (!proxyUrl || typeof proxyUrl !== 'string') return undefined;
    const agent = new HttpsProxyAgent(proxyUrl);
    return { http: agent, https: agent };
  } catch {
    return undefined;
  }
}

async function getCsrfFromJar() {
  const cookies = await cookieJar.getCookies('https://www.instagram.com/');
  const c = cookies.find(k => k.key.toLowerCase() === 'csrftoken');
  return c ? c.value : '';
}

async function ensureSession() {
  if (sessionPrepared) return;
  try {
    const agent = buildAgent();
    await got('https://www.instagram.com/', {
      agent,
      cookieJar,
      timeout: { request: 20000 },
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8',
      },
    });
  } catch (_) {
    // segue mesmo assim
  } finally {
    sessionPrepared = true;
  }
}

// ============== HTTP com retry/backoff ==============
async function gotJsonWithRetry(url, { headers = {}, maxRetries = 3 } = {}) {
  let attempt = 0;
  let wait = 1200;

  while (true) {
    try {
      const agent = buildAgent();
      return await got(url, {
        throwHttpErrors: true,
        agent,
        cookieJar,
        timeout: { request: 20000 },
        headers,
      }).json();
    } catch (err) {
      const status = err?.response?.statusCode || err?.code;
      attempt++;
      const retriable = [401, 403, 429].includes(status) || err?.name === 'RequestError' || err?.name === 'TimeoutError';
      if (attempt > maxRetries || !retriable) throw err;
      await sleep(wait + Math.floor(Math.random() * 800));
      wait = Math.min(wait * 2, 8000);
    }
  }
}

// ============== Extração de dados do perfil ==============
function extractEmailFromProfile(profile) {
  const direct = profile?.business_email || profile?.public_email || null;
  if (direct) return direct;
  const bio = profile?.biography || '';
  const url = profile?.external_url || '';
  const re = /([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})/gi;
  const bioMatch = bio.match(re);
  if (bioMatch && bioMatch.length) return bioMatch[0].toLowerCase();
  const urlMatch = url.match(re);
  if (urlMatch && urlMatch.length) return urlMatch[0].toLowerCase();
  return null;
}

// ===== GÊNERO =====
function guessGender(profile, username = '') {
  const bio = (profile?.biography || '').toLowerCase();
  const cat = (profile?.category_name || '').toLowerCase();
  const uname = (username || '').toLowerCase();
  const full = (profile?.full_name || '').toLowerCase();

  const femPron = /(ela\/dela|she\/her|mulher|blogueira|modelo feminina|moda feminina|womenswear|feminina)\b/i.test(bio) || /(womenswear|feminina)\b/i.test(cat);
  const mascPron = /(ele\/dele|he\/him|homem|blogueiro|modelo masculino|moda masculina|menswear|masculina)\b/i.test(bio) || /(menswear|masculina)\b/i.test(cat);
  if (femPron && !mascPron) return 'feminino';
  if (mascPron && !femPron) return 'masculino';

  const token = (full.split(/\s+/)[0] || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const FEM = new Set(['ana', 'maria', 'mariana', 'juliana', 'camila', 'larissa', 'beatriz', 'giovanna', 'amanda', 'carla', 'luiza', 'luisa', 'patricia', 'isabela', 'isabel', 'isabella', 'bruna', 'bia', 'gabriela', 'barbara', 'fernanda', 'aline', 'leticia', 'sofia', 'sofia', 'thais', 'tatiana', 'flavia', 'raissa', 'raisa', 'virginia', 'virginia']);
  const MASC = new Set(['joao', 'jose', 'carlos', 'pedro', 'paulo', 'mateus', 'matheus', 'rafael', 'lucas', 'bruno', 'thiago', 'tiago', 'fernando', 'gustavo', 'leo', 'leonardo', 'marcos', 'marcio', 'andre', 'rodrigo', 'roberto', 'rodrigo', 'henrique', 'vitor', 'victor', 'daniel', 'diego', 'felipe', 'igor', 'neymar', 'russell', 'russo', 'russolimah', 'russian']);
  if (FEM.has(token)) return 'feminino';
  if (MASC.has(token)) return 'masculino';

  if (/(^|[^a-z])(girl|garota|feminina|dela)([^a-z]|$)/i.test(uname)) return 'feminino';
  if (/(^|[^a-z])(boy|garoto|masculina|dele|mens)([^a-z]|$)/i.test(uname)) return 'masculino';

  if (token.endsWith('a')) return 'feminino';
  if (token.endsWith('o') || token.endsWith('r') || token.endsWith('s')) return 'masculino';
  return 'desconhecido';
}

// ============== Classificações: comercial x moda ==============
const STORE_KEYWORDS = [
  'loja', 'store', 'boutique', 'outlet', 'atacado', 'varejo', 'multimarcas', 'brecho', 'brechó',
  'delivery', 'catalogo', 'catálogo', 'pedido', 'encomenda', 'frete', 'pix'
];

const FASHION_KEYWORDS = [
  'moda', 'fashion', 'estilo', 'look', 'looks', 'outfit', 'ootd', 'streetwear',
  'menswear', 'womenswear', 'modelo', 'stylist', 'consultor de imagem', 'consultora de imagem'
];

function isCommercialProfile(profile) {
  const uname = normalize(profile?.username || '');
  const fname = normalize(profile?.full_name || '');
  return STORE_KEYWORDS.some(k => uname.includes(k) || fname.includes(k));
}

function isFashionProfile(profile) {
  const cat = normalize(profile?.category_name || '');
  const bio = normalize(profile?.biography || '');
  const fname = normalize(profile?.full_name || '');

  const catFashion =
    /\b(mod|fashion|modelo|model|blogueir[ao]|criador[ae] de conteudo( digital)?)\b/.test(cat);

  const textHit = FASHION_KEYWORDS.some(k =>
    bio.includes(k) || fname.includes(k)
  );

  // precisa ter pelo menos 1 sinal forte de moda
  return catFashion || textHit;
}

// Substitua a função antiga por esta (retorna %)
function minViewRateByTierPct(followers) {
  if (followers >= 500000) return 6.0;   // macro/mega
  if (followers >= 100000) return 5.0;   // mid
  if (followers >= 10000) return 4.0;   // micro
  if (followers >= 3000) return 3.0;   // 3k–10k
  return 0.0;
}


function meetsEngagementThreshold(followers, erPctCombined) {
  if (followers >= 1_000_000) return erPctCombined >= 0.8;
  if (followers >= 100_000) return erPctCombined >= 1.2;
  if (followers >= 10_000) return erPctCombined >= 1.8;
  if (followers >= 3_000) return erPctCombined >= 2.5;
  return false;
}

function isApprovedInfluencer(profile, metrics) {
  const motivos = [];
  const followers = metrics.followers || 0;
  const following = metrics.following || 0;
  const erPctCombined = metrics.engagement_rate_pct || 0;
  const mvr = metrics.median_views_recent || 0;

  // Reprova se for perfil comercial
  if (isCommercialProfile(profile)) {
    motivos.push("Perfil comercial/loja detectado");
  }

  // Reprova se não for perfil de moda
  if (!isFashionProfile(profile)) {
    motivos.push("Categoria/tema não relacionado à moda");
  }

  // Reprova se seguidores < 3000
  if (followers < 3000) {
    motivos.push("Menos de 3000 seguidores");
  }

  // Reprova se relação followers/following < 1.2
  if (ratio(followers, following) < 1.2) {
    motivos.push("Proporção seguidores/seguindo menor que 1.2");
  }

  // Reprova se ER combinado abaixo do mínimo
  if (!meetsEngagementThreshold(followers, erPctCombined)) {
    motivos.push(`Engajamento combinado baixo (${erPctCombined}%)`);
  }

  // Reprova se view rate abaixo do mínimo esperado
  // ---- Gate de View Rate (mais flexível)
  const vrMinPct = minViewRateByTierPct(followers);               // corte em %
  const actualVRPct = Number.isFinite(metrics.view_rate_pct)
    ? metrics.view_rate_pct                                       // já vem do main (baseViews/followers*100)
    : (followers > 0 && metrics.median_views_recent
      ? (metrics.median_views_recent / followers) * 100
      : 0);

  // aplica só se tivermos VR calculável > 0 e houver um corte > 0
  if (actualVRPct > 0 && vrMinPct > 0) {
    const GRACE_PP = 0.6; // tolerância de 0.6 pontos percentuais

    if (actualVRPct + GRACE_PP < vrMinPct) {
      motivos.push(`View rate abaixo do mínimo (${actualVRPct.toFixed(2)}% < ${vrMinPct.toFixed(2)}%)`);
    } else if (actualVRPct < vrMinPct) {
      // não reprova, só loga aviso para auditoria
      console.log(`⚠️ ATENÇÃO (VR na faixa de tolerância): ${profile.username} | VR ${actualVRPct.toFixed(2)}% ~ corte ${vrMinPct.toFixed(2)}%`);
    }
  }


  if (motivos.length > 0) {
    console.log(`❌ REPROVADO: ${profile.username} | Motivos: ${motivos.join("; ")}`);
    return false;
  }

  console.log(`✅ APROVADO: ${profile.username} | Seguidores: ${followers} | ER combinado: ${erPctCombined}%`);
  return true;
}

// ============== Fetchers IG (com fallback) ==============
async function igFetchProfile(username) {
  await ensureSession();
  const csrf = await getCsrfFromJar();
  const headers = buildHeaders(username, IG_APP_ID, csrf);

  const u1 = `https://i.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`;
  try {
    const res = await gotJsonWithRetry(u1, { headers, maxRetries: 3 });
    return res?.data?.user;
  } catch (e) {
    const sc = e?.response?.statusCode;
    if (![401, 403].includes(sc)) throw e;
  }

  const u2 = `https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`;
  const res2 = await gotJsonWithRetry(u2, { headers, maxRetries: 3 });
  return res2?.data?.user;
}

// ================== NOVO: extrair @ de link de post ==================
async function usernameFromPostUrl(postUrl) {
  const match = postUrl.match(/\/p\/([^/?#]+)/);
  if (!match) throw new Error(`Link inválido de post: ${postUrl}`);
  const shortcode = match[1];
  const url = `https://www.instagram.com/p/${shortcode}/?__a=1&__d=dis`;

  const agent = buildAgent();
  const res = await got(url, {
    agent,
    cookieJar,
    headers: {
      'User-Agent': 'Mozilla/5.0',
      'Accept': 'application/json',
      'Referer': 'https://www.instagram.com/',
      'X-IG-App-ID': IG_APP_ID,
    },
    timeout: { request: 20000 },
  }).json();

  const username = res?.graphql?.shortcode_media?.owner?.username;
  if (!username) throw new Error(`Não consegui extrair username do post: ${postUrl}`);
  return username;
}


async function igFetchPosts(username, userId, wanted = 24) {
  await ensureSession();
  const csrf = await getCsrfFromJar();
  const headers = buildHeaders(username, IG_APP_ID, csrf);

  const first = await gotJsonWithRetry(
    `https://i.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`,
    { headers, maxRetries: 3 }
  ).catch(async () => {
    return await gotJsonWithRetry(
      `https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`,
      { headers, maxRetries: 2 }
    );
  });

  const user = first?.data?.user;
  const media = user?.edge_owner_to_timeline_media;
  let edges = media?.edges?.map(e => e.node) ?? [];
  let endCursor = media?.page_info?.end_cursor;
  let hasNext = media?.page_info?.has_next_page;

  while (edges.length < wanted && hasNext && endCursor) {
    const variables = { id: userId, first: 12, after: endCursor };
    const url = 'https://www.instagram.com/graphql/query/?' + new URLSearchParams({
      doc_id: '17888483320059182',
      variables: JSON.stringify(variables),
    }).toString();

    try {
      const resp = await gotJsonWithRetry(url, { headers, maxRetries: 2 });
      const pageEdges = resp?.data?.user?.edge_owner_to_timeline_media?.edges ?? [];
      edges.push(...pageEdges.map(e => e.node));
      const pageInfo = resp?.data?.user?.edge_owner_to_timeline_media?.page_info;
      hasNext = pageInfo?.has_next_page;
      endCursor = pageInfo?.end_cursor;
      await sleep(1200 + Math.floor(Math.random() * 600));
    } catch {
      break;
    }
  }
  return edges.slice(0, wanted);
}

// ============== Métricas básicas ==============
function statsFromPosts(nodes, followers) {
  const posts = nodes.map(n => {
    const isVideo = !!n.is_video;
    const likes = n.edge_liked_by?.count ?? n.edge_media_preview_like?.count ?? 0;
    const comments = n.edge_media_to_comment?.count ?? 0;
    const views = isVideo ? (n.video_view_count ?? 0) : null;
    return {
      shortcode: n.shortcode,
      taken_at_timestamp: n.taken_at_timestamp,
      is_video: isVideo,
      likes, comments, views
    };
  });

  const validEng = posts.filter(p => (p.likes + p.comments) > 0);
  const avgEng = validEng.length
    ? validEng.reduce((s, p) => s + p.likes + p.comments, 0) / validEng.length
    : 0;
  const engagementRate = followers > 0 ? (avgEng / followers) * 100 : 0;

  const viewsArr = posts.filter(p => p.views != null).map(p => p.views).sort((a, b) => a - b);
  let medianViews = 0;
  if (viewsArr.length) {
    const mid = Math.floor(viewsArr.length / 2);
    medianViews = viewsArr.length % 2 === 0
      ? Math.round((viewsArr[mid - 1] + viewsArr[mid]) / 2)
      : viewsArr[mid];
  }

  const videoViews = posts.filter(p => p.views != null).map(p => p.views);
  const avgViews = Math.round(mean(videoViews));

  return { posts, engagementRate, medianViews, avgViews };
}

// ============== SCORE V4 ==============
function computeEngagementScoreV4({ posts, followers }) {
  const nowSec = Math.floor(Date.now() / 1000);
  const F = Math.max(1, followers);

  const engPerPost = posts.map(p => (p.likes || 0) + (p.comments || 0));
  const engPerFollower = engPerPost.map(e => e / F);
  const avgER = engPerFollower.length ? mean(engPerFollower) : 0;

  const vpfList = posts.filter(p => p.is_video && p.views != null).map(p => p.views / F);
  const medVPF = median(vpfList);

  const commentsShare = posts.map(p => {
    const tot = (p.likes || 0) + (p.comments || 0);
    return tot > 0 ? (p.comments || 0) / tot : 0;
  }).filter(Number.isFinite);

  const posts60d = posts.filter(p => (nowSec - (p.taken_at_timestamp || 0)) <= 60 * 24 * 3600).length;
  const lastPost = posts.length ? Math.max(...posts.map(p => p.taken_at_timestamp || 0)) : 0;
  const daysSince = lastPost ? (nowSec - lastPost) / 86400 : 999;

  const mEng = mean(engPerPost);
  const sdEng = stddev(engPerPost);
  const cv = mEng > 0 ? sdEng / mEng : 999;

  function expectedER(f) {
    const k = 0.03, beta = 0.30;
    const base = Math.pow(Math.max(1, f) / 10000, -beta);
    return Math.max(0.003, Math.min(0.10, k * base));
  }
  function expectedVPF(f) {
    const k = 0.12, beta = 0.25;
    const base = Math.pow(Math.max(1, f) / 10000, -beta);
    return Math.max(0.008, Math.min(0.60, k * base));
  }

  const erExp = expectedER(F);
  const vpfExp = expectedVPF(F);
  const erNorm = erExp ? (avgER / erExp) : 0;
  const vpfNorm = vpfExp ? (medVPF / vpfExp) : 0;

  const hasVideos = vpfList.length >= 3;
  const medAbsViews = hasVideos
    ? median(posts.filter(p => p.views != null).map(p => p.views))
    : median(posts.map(p => (p.likes || 0) + (p.comments || 0)));
  const expectedAbs = (hasVideos ? vpfExp : erExp) * F;
  const absRatio = expectedAbs > 0 ? (medAbsViews / expectedAbs) : 0;

  const tier =
    F < 10000 ? 'nano' :
      F < 100000 ? 'micro' :
        F < 500000 ? 'mid' :
          F < 2000000 ? 'macro' : 'mega';

  const baseW = {
    nano: { er: 0.45, vpf: 0.15, abs: 0.00, freq: 0.12, rec: 0.10, cshare: 0.08, consist: 0.10 },
    micro: { er: 0.38, vpf: 0.20, abs: 0.04, freq: 0.12, rec: 0.10, cshare: 0.08, consist: 0.08 },
    mid: { er: 0.30, vpf: 0.22, abs: 0.15, freq: 0.12, rec: 0.08, cshare: 0.07, consist: 0.06 },
    macro: { er: 0.24, vpf: 0.20, abs: 0.28, freq: 0.12, rec: 0.08, cshare: 0.05, consist: 0.03 },
    mega: { er: 0.18, vpf: 0.20, abs: 0.35, freq: 0.12, rec: 0.07, cshare: 0.05, consist: 0.03 },
  }[tier];

  const erW = hasVideos ? baseW.er : (baseW.er + baseW.vpf);
  const vpfW = hasVideos ? baseW.vpf : 0;
  const absW = baseW.abs;

  const erScore = clamp01(erNorm / 1.8);
  const vpfScore = clamp01(vpfNorm / 1.8);
  const absScore = clamp01(absRatio / 1.8);

  const freqScore = clamp01(posts60d / 10);
  const recencyScore = clamp01(Math.exp(-(daysSince) / (tier === 'mega' ? 20 : 14)));
  const medCommentsShare = median(commentsShare);
  const commentShareScore = clamp01(medCommentsShare / 0.18);
  const consistencyScore = clamp01((2.0 - Math.min(2.0, cv)) / (2.0 - 0.6));

  const verifiedBoost = posts.length ? 1.03 : 1.00;
  const sizeBoost = (() => {
    const x = Math.log10(F / 1e5);
    const bonus = Math.max(0, x) * 0.04;
    return Math.min(1.15, 1.00 + bonus);
  })();

  let score01 =
    erW * erScore +
    vpfW * vpfScore +
    absW * absScore +
    baseW.freq * freqScore +
    baseW.rec * recencyScore +
    baseW.cshare * commentShareScore +
    baseW.consist * consistencyScore;

  score01 = clamp01(score01 * verifiedBoost * sizeBoost);

  const score = Math.round(score01 * 100);
  const grade = score >= 80 ? 'A' : score >= 65 ? 'B' : 'C';

  return {
    score,
    grade,
    components: {
      tier, followers: F,
      er_pct: +(avgER * 100).toFixed(2), er_expected_pct: +(erExp * 100).toFixed(2), er_norm: +erNorm.toFixed(2),
      vpf_pct_med: +(medVPF * 100).toFixed(2), vpf_expected_pct: +(vpfExp * 100).toFixed(2), vpf_norm: +vpfNorm.toFixed(2),
      abs_median: medAbsViews, abs_expected: Math.round(expectedAbs), abs_ratio: +absRatio.toFixed(2), absScore: +absScore.toFixed(2),
      posts60d, freqScore: +freqScore.toFixed(2),
      daysSinceLastPost: Math.round(daysSince), recencyScore: +recencyScore.toFixed(2),
      comments_share_med_pct: +(medCommentsShare * 100).toFixed(2), commentShareScore: +commentShareScore.toFixed(2),
      cv_engagement: Number.isFinite(cv) ? +cv.toFixed(2) : null, consistencyScore: +consistencyScore.toFixed(2),
      weights: { er: erW, vpf: vpfW, abs: absW, freq: baseW.freq, rec: baseW.rec, cshare: baseW.cshare, consist: baseW.consist },
      authority_multipliers: { verifiedBoost, sizeBoost }
    }
  };
}

// ============== Main ==============
Actor.main(async () => {
  const input = await Actor.getInput();
  const {
    usernames = [],
    postUrls = [], // <<< NOVO
    postsLimit = 24,
    useLoginCookies = false,
    cookies = '',
    proxy = { useApifyProxy: true },
    concurrency = 1,
  } = input || {};


  // Proxy
  proxyConfiguration = undefined;
  try {
    if (proxy && (proxy.useApifyProxy || (Array.isArray(proxy.proxyUrls) && proxy.proxyUrls.length))) {
      proxyConfiguration = await Actor.createProxyConfiguration(proxy);
    }
  } catch (e) {
    console.warn('Proxy desabilitado (falha ao criar config):', e?.message || e);
  }

  // Cookies de login (opcional)
  cookieJar = new CookieJar();
  if (useLoginCookies && cookies) {
    try {
      const arr = typeof cookies === 'string' ? JSON.parse(cookies) : cookies;
      const list = Array.isArray(arr) ? arr : (arr && arr.name ? [arr] : []);
      for (const c of list) {
        await cookieJar.setCookie(`${c.name}=${c.value}; Domain=.instagram.com; Path=/;`, 'https://www.instagram.com/');
      }
    } catch (e) {
      console.warn('Cookies inválidos. Ignorando.', e?.message || e);
    }
  }

  const postUsernames = [];
  for (const url of postUrls) {
    try {
      const uname = await usernameFromPostUrl(url);
      postUsernames.push(uname);
    } catch (err) {
      console.warn('Erro ao extrair username de post:', url, err.message);
    }
  }

  const allUsernames = [...usernames, ...postUsernames];


  const limit = pLimit(Math.max(1, Number(concurrency) || 1));
  const results = [];

  await Promise.all(allUsernames.map(username => limit(async () => {

    try {
      const profile = await igFetchProfile(username);
      if (!profile) throw new Error('Perfil não encontrado.');

      const followers = profile.edge_followed_by?.count ?? 0;
      const following = profile.edge_follow?.count ?? 0;

      const nodes = await igFetchPosts(username, profile.id, postsLimit);
      const { posts, engagementRate, medianViews, avgViews } = statsFromPosts(nodes, followers);
      const scoreObj = computeEngagementScoreV4({ posts, followers });

      const accountUrl = `https://www.instagram.com/${username}/`;
      const email = extractEmailFromProfile(profile);
      const gender = guessGender(profile, username);

      // ===== métricas com views =====
      const baseViews = medianViews || avgViews || 0;
      const viewRatePct = followers > 0 ? (baseViews / followers) * 100 : 0;
      const engagementCombinedPct = +(0.6 * engagementRate + 0.4 * viewRatePct).toFixed(2);

      const approved = isApprovedInfluencer(profile, {
        followers,
        following,
        engagement_rate_pct: engagementCombinedPct, // combinado
        median_views_recent: medianViews,
        recent_posts_analyzed: posts.length
      });

      const item = {
        'Nome': profile.full_name || null,
        'Link do perfil': accountUrl,
        'Email': email,
        'Seguidores': followers,
        'Média de Views': avgViews,
        'Score de Engajamento (0-100)': scoreObj.score,
        'Masculino ou feminino': gender,

        username,
        biography: profile.biography,
        is_verified: profile.is_verified,
        external_url: profile.external_url,
        category_name: profile.category_name,
        profile_pic_url: profile.profile_pic_url_hd || profile.profile_pic_url,
        following,
        posts_count: profile.edge_owner_to_timeline_media?.count ?? null,

        // métricas de engajamento
        engagement_rate_pct_raw: Number(engagementRate.toFixed(2)),   // likes+comments
        view_rate_pct: Number(viewRatePct.toFixed(2)),                // views/seguidores
        engagement_rate_pct_combined: engagementCombinedPct,          // 0.6*ER + 0.4*VR
        median_views_recent: medianViews,
        recent_posts_analyzed: posts.length,

        recent_sample: posts.slice(0, 12),
        health_grade: scoreObj.grade,
        health_components: scoreObj.components,
        scraped_at: new Date().toISOString(),

        approved
      };

      results.push(item);
      await Actor.pushData(item);
    } catch (err) {
      const payload = { username, error: String(err) };
      results.push(payload);
      await Actor.pushData(payload);
    }
  })));

  await Actor.setValue('SUMMARY.json', results);
});
