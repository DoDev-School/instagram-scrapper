import { Actor } from 'apify';
import got from 'got';
import pLimit from 'p-limit';

const IG_APP_ID = '936619743392459'; // público do web app do IG (pode mudar)

// ---------- helpers ----------
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

  // sinais fortes por pronome PT/EN
  const femPron = /(ela\/dela|she\/her|mulher|blogueira|modelo feminina|moda feminina|womenswear|feminina)/i.test(bio) || /(womenswear|feminina)/i.test(cat);
  const mascPron = /(ele\/dele|he\/him|homem|blogueiro|modelo masculino|moda masculina|menswear|masculina)/i.test(bio) || /(menswear|masculina)/i.test(cat);

  if (femPron && !mascPron) return 'feminino';
  if (mascPron && !femPron) return 'masculino';

  // alguns hints comuns em moda no username
  if (/dicasdela|garotas|girls|mulher|feminina/.test(uname)) return 'feminino';
  if (/doiseles|garotos|boys|masculina|mens/.test(uname)) return 'masculino';

  return 'desconhecido';
}

// ---------- instagram fetchers ----------
async function igFetchProfile(username, cookieHeader) {
  const url = `https://i.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`;
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari',
    'X-IG-App-ID': IG_APP_ID,
    'Accept': 'application/json',
    'Referer': `https://www.instagram.com/${username}/`
  };
  if (cookieHeader) headers['Cookie'] = cookieHeader;

  const res = await got(url, { headers, http2: true }).json();
  return res?.data?.user; // objeto do perfil
}

async function igFetchPosts(username, userId, wanted = 24, cookieHeader) {
  const headers = {
    'User-Agent': 'Mozilla/5.0',
    'X-IG-App-ID': IG_APP_ID,
    'Accept': 'application/json',
    'Referer': `https://www.instagram.com/${username}/`
  };
  if (cookieHeader) headers.Cookie = cookieHeader;

  // 1) primeira página via web_profile_info (confiável sem login)
  const first = await got(
    `https://i.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`,
    { headers, http2: true }
  ).json();

  const user = first?.data?.user;
  const media = user?.edge_owner_to_timeline_media;
  let edges = media?.edges?.map(e => e.node) ?? [];
  let endCursor = media?.page_info?.end_cursor;
  let hasNext = media?.page_info?.has_next_page;

  // 2) pagina se precisar de mais (usa doc_id estável)
  while (edges.length < wanted && hasNext && endCursor) {
    const variables = { id: userId, first: 12, after: endCursor };

    // doc_id “UserMedia”
    const url = 'https://www.instagram.com/graphql/query/?' +
      new URLSearchParams({
        doc_id: '17888483320059182',
        variables: JSON.stringify(variables),
      }).toString();

    const resp = await got(url, { headers }).json();
    const pageEdges = resp?.data?.user?.edge_owner_to_timeline_media?.edges ?? [];
    edges.push(...pageEdges.map(e => e.node));

    const pageInfo = resp?.data?.user?.edge_owner_to_timeline_media?.page_info;
    hasNext = pageInfo?.has_next_page;
    endCursor = pageInfo?.end_cursor;

    // Respiro para não tomar rate limit
    await new Promise(res => setTimeout(res, 1200));
  }

  return edges.slice(0, wanted);
}

// ---------- métricas a partir dos posts (SOMENTE o que você já coleta) ----------
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

  // engajamento médio (likes+comments) / followers
  const validEng = posts.filter(p => (p.likes + p.comments) > 0);
  const avgEng = validEng.length
    ? validEng.reduce((s, p) => s + p.likes + p.comments, 0) / validEng.length
    : 0;
  const engagementRate = followers > 0 ? (avgEng / followers) * 100 : 0;

  // median views (vídeos/reels)
  const viewsArr = posts.filter(p => p.views != null).map(p => p.views).sort((a,b)=>a-b);
  let medianViews = 0;
  if (viewsArr.length) {
    const mid = Math.floor(viewsArr.length / 2);
    medianViews = viewsArr.length % 2 === 0
      ? Math.round((viewsArr[mid - 1] + viewsArr[mid]) / 2)
      : viewsArr[mid];
  }

  // média de views (vídeos/reels)
  const videoViews = posts.filter(p => p.views != null).map(p => p.views);
  const avgViews = Math.round(mean(videoViews));

  return { posts, engagementRate, medianViews, avgViews };
}

// ---------- score de engajamento (0–100) usando apenas likes, comments, views, timestamps, followers ----------
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

  // ER (média por post)
  const avgER = engPerFollower.length ? mean(engPerFollower) : 0; // fração
  const erScore = clamp01(avgER / 0.08); // 8% => 1.0

  // Views por seguidor (mediana)
  const medVpf = median(videoViewsPerFollower); // fração
  const viewsScore = clamp01(medVpf / 0.08); // 8% => 1.0
  const hasVideos = videoViewsPerFollower.length >= 3;

  // Frequência (últimos 60d)
  const posts60d = posts.filter(p => (nowSec - (p.taken_at_timestamp||0)) <= 60*24*3600).length;
  const freqScore = clamp01(posts60d / 12); // 12+ em 60d ~ 2/sem = 1.0

  // Recência (decaimento)
  const lastPost = posts.length ? Math.max(...posts.map(p => p.taken_at_timestamp||0)) : 0;
  const daysSince = lastPost ? (nowSec - lastPost) / 86400 : 999;
  const recencyScore = clamp01(Math.exp(-daysSince / 10)); // half-life ~10d

  // Comentários/(likes+comments) (mediana)
  const medCommentsShare = median(commentsShare);
  const commentShareScore = clamp01(medCommentsShare / 0.20); // 20% => 1.0

  // Consistência (CV do engajamento por post)
  const mEng = mean(engPerPost);
  const sdEng = stddev(engPerPost);
  const cv = mEng > 0 ? sdEng / mEng : 999;
  const consistencyScore = clamp01((2.0 - Math.min(2.0, cv)) / (2.0 - 0.6));

  const W = {
    er: 0.35,
    views: 0.25,
    freq: 0.15,
    recency: 0.10,
    commentShare: 0.10,
    consistency: 0.05,
  };
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

  // monta header de cookies se vierem
  let cookieHeader = '';
  if (useLoginCookies && cookies) {
    try {
      const arr = JSON.parse(cookies);
      cookieHeader = arr.map(c => `${c.name}=${c.value}`).join('; ');
    } catch (e) {
      console.warn('Cookies inválidos. Ignorando.');
    }
  }

  const limit = pLimit(2); // evita rate limit
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
        // ---- pedidos novos ----
        nome: profile.full_name || null,
        link_da_conta: accountUrl,
        email: email,
        seguidores: followers,
        media_de_views: avgViews, // média de views de vídeos recentes
        score_de_engajamento_0a100: scoreObj.score,
        genero: gender, // 'masculino' | 'feminino' | 'desconhecido'

        // ---- campos que você já tinha ----
        username,
        full_name: profile.full_name,
        biography: profile.biography,
        is_verified: profile.is_verified,
        external_url: profile.external_url,
        category_name: profile.category_name,
        profile_pic_url: profile.profile_pic_url_hd || profile.profile_pic_url,
        followers,
        following,
        posts_count: profile.edge_owner_to_timeline_media?.count ?? null,
        engagement_rate_pct: Number(engagementRate.toFixed(2)),
        median_views_recent: medianViews,
        recent_posts_analyzed: posts.length,
        recent_sample: posts.slice(0, 12), // amostra
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

  // também salva um output “consolidado”
  await Actor.setValue('SUMMARY.json', results);
});
