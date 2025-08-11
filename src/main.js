import { Actor } from 'apify';
import got from 'got';
import pLimit from 'p-limit';

const IG_APP_ID = '936619743392459'; // público do web app do IG (pode mudar)

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
    await Actor.sleep(1200);
  }

  return edges.slice(0, wanted);
}

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

  // median views (só vídeos/reels com views registradas)
  const viewsArr = posts.filter(p => p.views != null).map(p => p.views).sort((a,b)=>a-b);
  let medianViews = 0;
  if (viewsArr.length) {
    const mid = Math.floor(viewsArr.length / 2);
    medianViews = viewsArr.length % 2 === 0
      ? Math.round((viewsArr[mid - 1] + viewsArr[mid]) / 2)
      : viewsArr[mid];
  }

  return { posts, engagementRate, medianViews };
}

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
      const { posts, engagementRate, medianViews } = statsFromPosts(nodes, followers);

      const item = {
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
