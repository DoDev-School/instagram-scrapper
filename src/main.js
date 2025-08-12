import { Actor } from 'apify';
import got from 'got';
import pLimit from 'p-limit';

const IG_APP_ID = '936619743392459';

// ---------- helpers ----------
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
function clamp01(x) { return Math.max(0, Math.min(1, x)); }
function median(arr) {
  if (!arr.length) return 0;
  const a = [...arr].sort((x,y)=>x-y);
  const m = Math.floor(a.length/2);
  return a.length % 2 ? a[m] : (a[m-1] + a[m]) / 2;
}
function mean(arr){ return arr.length ? arr.reduce((s,v)=>s+v,0)/arr.length : 0; }
function stddev(arr){
  if (arr.length < 2) return 0;
  const m = mean(arr);
  const v = mean(arr.map(x => (x-m)**2));
  return Math.sqrt(v);
}
function buildHeaders(username, appId, cookieHeader = '', csrf = '') {
  const h = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Origin': 'https://www.instagram.com',
    'Referer': `https://www.instagram.com/${username}/`,
    'X-IG-App-ID': appId,
    'X-Requested-With': 'XMLHttpRequest',
  };
  if (cookieHeader) h['Cookie'] = cookieHeader;
  if (csrf) h['X-CSRFToken'] = csrf;
  return h;
}
function parseCsrftokenFromCookieHeader(cookieHeader='') {
  const m = cookieHeader.match(/(^|;\s*)csrftoken=([^;]+)/i);
  return m ? m[2] : '';
}
async function gotJsonWithRetry(url, options={}, maxRetries=3) {
  let attempt = 0;
  let wait = 1200;
  while (true) {
    try {
      return await got(url, { http2: true, throwHttpErrors: true, ...options }).json();
    } catch (err) {
      const status = err?.response?.statusCode || err?.code;
      attempt++;
      if (attempt > maxRetries || ![401,403,429].includes(status)) throw err;
      await sleep(wait);
      wait = Math.min(wait * 2, 8000);
    }
  }
}
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
function guessGender(profile, username='') {
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

// ---------- fetchers ----------
async function igFetchProfile(username, cookieHeader) {
  const csrf = parseCsrftokenFromCookieHeader(cookieHeader);
  const headers = buildHeaders(username, IG_APP_ID, cookieHeader, csrf);
  const url = `https://i.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`;
  const res = await gotJsonWithRetry(url, { headers }, 3);
  return res?.data?.user;
}

async function igFetchPosts(username, userId, wanted = 24, cookieHeader) {
  const csrf = parseCsrftokenFromCookieHeader(cookieHeader);
  const headers = buildHeaders(username, IG_APP_ID, cookieHeader, csrf);

  const first = await gotJsonWithRetry(
    `https://i.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`,
    { headers },
    3
  );
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
      const resp = await gotJsonWithRetry(url, { headers }, 2);
      const pageEdges = resp?.data?.user?.edge_owner_to_timeline_media?.edges ?? [];
      edges.push(...pageEdges.map(e => e.node));
      const pageInfo = resp?.data?.user?.edge_owner_to_timeline_media?.page_info;
      hasNext = pageInfo?.has_next_page;
      endCursor = pageInfo?.end_cursor;
      await sleep(1500);
    } catch {
      break;
    }
  }
  return edges.slice(0, wanted);
}

// ---------- métricas ----------
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

  const viewsArr = posts.filter(p => p.views != null).map(p => p.views).sort((a,b)=>a-b);
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

function computeEngagementScore({ posts, followers }) {
  const nowSec = Math.floor(Date.now()/1000);

  const engPerPost = posts.map(p => (p.likes || 0) + (p.comments || 0));
  const engPerFollower = followers > 0 ? engPerPost.map(e => e / followers) : [];
  const commentsShare = posts
    .map(p => {
      const tot = (p.likes||0) + (p.comments||0);
      return tot > 0 ? (p.comments||0) / tot : 0;
    })
    .filter(x => isFinite(x));

  const videoViewsPerFollower = posts
    .filter(p => p.is_video && p.views != null && followers > 0)
    .map(p => p.views / followers);

  const avgER = engPerFollower.length ? mean(engPerFollower) : 0;
  const erScore = clamp01(avgER / 0.08);

  const medVpf = median(videoViewsPerFollower);
  const viewsScore = clamp01(medVpf / 0.08);
  const hasVideos = videoViewsPerFollower.length >= 3;

  const posts60d = posts.filter(p => (nowSec - (p.taken_at_timestamp||0)) <= 60*24*3600).length;
  const freqScore = clamp01(posts60d / 12);

  const lastPost = posts.length ? Math.max(...posts.map(p => p.taken_at_timestamp||0)) : 0;
  const daysSince = lastPost ? (nowSec - lastPost) / 86400 : 999;
  const recencyScore = clamp01(Math.exp(-daysSince / 10));

  const medCommentsShare = median(commentsShare);
  const commentShareScore = clamp01(medCommentsShare / 0.20);

  const mEng = mean(engPerPost);
  const sdEng = stddev(engPerPost);
  const cv = mEng > 0 ? sdEng / mEng : 999;
  const consistencyScore = clamp01((2.0 - Math.min(2.0, cv)) / (2.0 - 0.6));

  const W = { er: 0.35, views: 0.25, freq: 0.15, recency: 0.10, commentShare: 0.10, consistency: 0.05 };
  const erWeight = hasVideos ? W.er : (W.er + W.views);
  const viewsWeight = hasVideos ? W.views : 0;

  const score01 =
    erWeight * erScore +
    viewsWeight * viewsScore +
    W.freq * freqScore +
    W.recency * recencyScore +
    W.commentShare * commentShareScore +
    W.consistency * consistencyScore;

  const score = Math.round(score01 * 100);
  const grade = score >= 70 ? 'A' : score >= 55 ? 'B' : 'C';

  return {
    score,
    grade,
    components: {
      er_pct: +(avgER * 100).toFixed(2),
      erScore: +erScore.toFixed(3),
      views_per_follower_pct_med: +(medVpf * 100).toFixed(2),
      viewsScore: +viewsScore.toFixed(3),
      posts60d,
      freqScore: +freqScore.toFixed(3),
      daysSinceLastPost: Math.round(daysSince),
      recencyScore: +recencyScore.toFixed(3),
      comments_share_med_pct: +(medCommentsShare * 100).toFixed(2),
      commentShareScore: +commentShareScore.toFixed(3),
      cv_engagement: isFinite(cv) ? +cv.toFixed(2) : null,
      consistencyScore: +consistencyScore.toFixed(3),
      weights: { er: erWeight, views: viewsWeight, freq: W.freq, recency: W.recency, commentShare: W.commentShare, consistency: W.consistency }
    }
  };
}

// ---------- main ----------
Actor.main(async () => {
  const input = await Actor.getInput();
  const {
    usernames = [],
    postsLimit = 24,
    useLoginCookies = false,
    cookies = '',
    proxy = { useApifyProxy: true }
  } = input || {};

  let cookieHeader = '';
  if (useLoginCookies && cookies) {
    try {
      const arr = JSON.parse(cookies);
      cookieHeader = arr.map(c => `${c.name}=${c.value}`).join('; ');
    } catch (e) {
      console.warn('Cookies inválidos. Ignorando.');
    }
  }

  const limit = pLimit(1); // menos concorrência -> menos 401/429
  const results = [];

  await Promise.all(usernames.map(username => limit(async () => {
    try {
      const profile = await igFetchProfile(username, cookieHeader);
      if (!profile) throw new Error('Perfil não encontrado.');

      const followers = profile.edge_followed_by?.count ?? 0;
      const following = profile.edge_follow?.count ?? 0;

      const nodes = await igFetchPosts(username, profile.id, postsLimit, cookieHeader);
      const { posts, engagementRate, medianViews, avgViews } = statsFromPosts(nodes, followers);
      const scoreObj = computeEngagementScore({ posts, followers });

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
      await Actor.pushData({ username, error: String(err) });
    }
  })));

  await Actor.setValue('SUMMARY.json', results);
});
