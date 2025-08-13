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
  } catch (e) {
    // segue mesmo assim (algumas vezes o preflight redireciona)
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

function guessGender(profile, username = '') {
  const bio = (profile?.biography || '').toLowerCase();
  const cat = (profile?.category_name || '').toLowerCase();
  const uname = (username || '').toLowerCase();

  const femPron = /(ela\/dela|she\/her|mulher|blogueira|modelo feminina|moda feminina|womenswear|feminina)/i.test(bio) || /(womenswear|feminina)/i.test(cat);
  const mascPron = /(ele\/dele|he\/him|homem|blogueiro|modelo masculino|moda masculina|menswear|masculina)/i.test(bio) || /(menswear|masculina)/i.test(cat);

  if (femPron && !mascPron) return 'feminino';
  if (mascPron && !femPron) return 'masculino';
  if (/girls|garotas|feminina|dela/.test(uname)) return 'feminino';
  if (/boys|garotos|masculina|dele|mens/.test(uname)) return 'masculino';
  return 'desconhecido';
}

// ============== Fetchers IG (com fallback) ==============
async function igFetchProfile(username) {
  await ensureSession();
  const csrf = await getCsrfFromJar();
  const headers = buildHeaders(username, IG_APP_ID, csrf);

  // 1) endpoint mobile
  const u1 = `https://i.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`;
  try {
    const res = await gotJsonWithRetry(u1, { headers, maxRetries: 3 });
    return res?.data?.user;
  } catch (e) {
    const sc = e?.response?.statusCode;
    if (![401, 403].includes(sc)) throw e;
  }

  // 2) fallback host (alguns PoPs exigem)
  const u2 = `https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`;
  const res2 = await gotJsonWithRetry(u2, { headers, maxRetries: 3 });
  return res2?.data?.user;
}

async function igFetchPosts(username, userId, wanted = 24) {
  await ensureSession();
  const csrf = await getCsrfFromJar();
  const headers = buildHeaders(username, IG_APP_ID, csrf);

  // primeira página via web_profile_info
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

// ============== Score V3 (normalizado + alcance absoluto) ==============
function computeEngagementScoreV3({ posts, followers }) {
  const nowSec = Math.floor(Date.now() / 1000);
  const F = Math.max(1, followers);

  // base
  const engPerPost = posts.map(p => (p.likes || 0) + (p.comments || 0));
  const engPerFollower = engPerPost.map(e => e / F);
  const avgER = engPerFollower.length ? mean(engPerFollower) : 0; // fração

  const commentsShare = posts
    .map(p => {
      const tot = (p.likes || 0) + (p.comments || 0);
      return tot > 0 ? (p.comments || 0) / tot : 0;
    })
    .filter(Number.isFinite);

  const posts60d = posts.filter(p => (nowSec - (p.taken_at_timestamp || 0)) <= 60 * 24 * 3600).length;
  const lastPost = posts.length ? Math.max(...posts.map(p => p.taken_at_timestamp || 0)) : 0;
  const daysSince = lastPost ? (nowSec - lastPost) / 86400 : 999;

  const mEng = mean(engPerPost);
  const sdEng = stddev(engPerPost);
  const cv = mEng > 0 ? sdEng / mEng : 999;

  const vpfList = posts.filter(p => p.is_video && p.views != null).map(p => p.views / F);
  const medVPF = median(vpfList); // fração

  // expectativas por tamanho (heurísticas)
  function expectedER(f) {
    const k = 0.03, beta = 0.30; // ~3% @10k
    const base = Math.pow(Math.max(1, f) / 10000, -beta);
    return Math.max(0.004, Math.min(0.08, k * base)); // 0.4%–8%
  }
  function expectedVPF(f) {
    const k = 0.12, beta = 0.25; // ~12% @10k
    const base = Math.pow(Math.max(1, f) / 10000, -beta);
    return Math.max(0.01, Math.min(0.5, k * base)); // 1%–50%
  }

  const erExp = expectedER(F);
  const vpfExp = expectedVPF(F);

  const erNorm = erExp ? avgER / erExp : 0;      // >=1 acima do esperado
  const vpfNorm = vpfExp ? medVPF / vpfExp : 0;  // >=1 acima do esperado

  // alcance absoluto (mediana de views de vídeo se existir; senão mediana de likes+comments)
  const hasVideos = vpfList.length >= 3;
  const medAbsViews = hasVideos
    ? median(posts.filter(p => p.views != null).map(p => p.views))
    : median(posts.map(p => (p.likes || 0) + (p.comments || 0)));

  const expectedAbs = hasVideos ? (vpfExp * F) : (erExp * F); // baseline por tamanho
  const absRatio = expectedAbs > 0 ? medAbsViews / expectedAbs : 0; // >=1 acima do esperado
  const absScore = clamp01(absRatio / 1.2); // 20% acima do esperado ~ 1.0

  // tier e pesos (inclui peso de alcance absoluto para macro/mega)
  const tier =
    F < 10000 ? 'nano' :
    F < 100000 ? 'micro' :
    F < 500000 ? 'mid' :
    F < 2000000 ? 'macro' : 'mega';

  const baseW = {
    nano:  { er: 0.42, vpf: 0.18, abs: 0.00, freq: 0.12, rec: 0.10, cshare: 0.08, consist: 0.10 },
    micro: { er: 0.36, vpf: 0.22, abs: 0.02, freq: 0.12, rec: 0.10, cshare: 0.08, consist: 0.10 },
    mid:   { er: 0.30, vpf: 0.25, abs: 0.10, freq: 0.12, rec: 0.08, cshare: 0.08, consist: 0.07 },
    macro: { er: 0.24, vpf: 0.26, abs: 0.15, freq: 0.12, rec: 0.08, cshare: 0.08, consist: 0.07 },
    mega:  { er: 0.20, vpf: 0.25, abs: 0.22, freq: 0.12, rec: 0.08, cshare: 0.07, consist: 0.06 },
  }[tier];

  // se não tiver vídeos, somar VPF em ER e manter ABS (pois ABS usa eng. absoluta)
  const erW  = hasVideos ? baseW.er  : (baseW.er + baseW.vpf);
  const vpfW = hasVideos ? baseW.vpf : 0;
  const absW = baseW.abs;

  // sweet spot + menor penalidade p/ mega (agora mínimo 0.92)
  function sweetSpotMultiplier(f) {
    const mu = 250000, sigma = 700000;
    const x = (f - mu) / sigma;
    return 0.95 + 0.14 * Math.exp(-0.5 * x * x); // 0.95..1.09
  }
  function megaDiminishing(f) {
    if (f < 2000000) return 1.0;
    const t = Math.min(1, (f - 2000000) / 48000000);
    return 1.0 - 0.08 * t; // até ~0.92 em 50M+
  }
  const sizeMult = sweetSpotMultiplier(F) * megaDiminishing(F);

  // demais componentes
  const erScore  = clamp01(erNorm / 1.15);
  const vpfScore = clamp01(vpfNorm / 1.15);
  const freqScore = clamp01(posts60d / 12);
  const recencyScore = clamp01(Math.exp(-(daysSince) / 14));
  const medCommentsShare = median(commentsShare);
  const commentShareScore = clamp01(medCommentsShare / 0.20);
  const consistencyScore = clamp01((2.0 - Math.min(2.0, cv)) / (2.0 - 0.6));

  let score01 =
    erW * erScore +
    vpfW * vpfScore +
    absW * absScore +
    baseW.freq * freqScore +
    baseW.rec * recencyScore +
    baseW.cshare * commentShareScore +
    baseW.consist * consistencyScore;

  score01 = clamp01(score01 * sizeMult);
  const score = Math.round(score01 * 100);
  const grade = score >= 80 ? 'A' : score >= 65 ? 'B' : 'C';

  return {
    score,
    grade,
    components: {
      tier, followers: F,
      er_pct: +(avgER * 100).toFixed(2), er_expected_pct: +(erExp * 100).toFixed(2), er_norm: +erNorm.toFixed(3),
      vpf_pct_med: +(medVPF * 100).toFixed(2), vpf_expected_pct: +(vpfExp * 100).toFixed(2), vpf_norm: +vpfNorm.toFixed(3),
      abs_median: medAbsViews, abs_expected: Math.round(expectedAbs), abs_ratio: +absRatio.toFixed(2), absScore: +absScore.toFixed(3),
      posts60d, freqScore: +freqScore.toFixed(3),
      daysSinceLastPost: Math.round(daysSince), recencyScore: +recencyScore.toFixed(3),
      comments_share_med_pct: +(medCommentsShare * 100).toFixed(2), commentShareScore: +commentShareScore.toFixed(3),
      cv_engagement: Number.isFinite(cv) ? +cv.toFixed(2) : null, consistencyScore: +consistencyScore.toFixed(3),
      weights: { er: erW, vpf: vpfW, abs: absW, freq: baseW.freq, rec: baseW.rec, cshare: baseW.cshare, consist: baseW.consist },
      multipliers: { sweetSpot: +sweetSpotMultiplier(F).toFixed(3), megaDiminishing: +megaDiminishing(F).toFixed(3) }
    }
  };
}

// ============== Main ==============
Actor.main(async () => {
  const input = await Actor.getInput();
  const {
    usernames = [],
    postsLimit = 24,
    useLoginCookies = false,
    cookies = '',
    proxy = { useApifyProxy: true }, // defina groups se tiver acesso (ex.: ['RESIDENTIAL'])
    concurrency = 1,
  } = input || {};

  // Proxy (seguro)
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

  const limit = pLimit(Math.max(1, Number(concurrency) || 1));
  const results = [];

  await Promise.all(usernames.map(username => limit(async () => {
    try {
      const profile = await igFetchProfile(username);
      if (!profile) throw new Error('Perfil não encontrado.');

      const followers = profile.edge_followed_by?.count ?? 0;
      const following = profile.edge_follow?.count ?? 0;

      const nodes = await igFetchPosts(username, profile.id, postsLimit);
      const { posts, engagementRate, medianViews, avgViews } = statsFromPosts(nodes, followers);
      const scoreObj = computeEngagementScoreV3({ posts, followers });

      const accountUrl = `https://www.instagram.com/${username}/`;
      const email = extractEmailFromProfile(profile);
      const gender = guessGender(profile, username);

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
        engagement_rate_pct: Number(engagementRate.toFixed(2)),
        median_views_recent: medianViews,
        recent_posts_analyzed: posts.length,
        recent_sample: posts.slice(0, 12),
        health_grade: scoreObj.grade,
        health_components: scoreObj.components,
        scraped_at: new Date().toISOString()
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
