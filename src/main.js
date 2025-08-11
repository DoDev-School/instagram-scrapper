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

async function igFetchPostsShortcode(profileId, count = 24, cookieHeader) {
  // pega posts via página pública com JSON embutido em “__additionalData” (fallback via GraphQL é mais frágil)
  // estratégia: usar endpoint web “highlights” (alternativa) – aqui vamos simplesmente puxar do próprio web_profile_info edges
  // Observação: web_profile_info já retorna primeiros 12 nós. Para mais, precisaríamos de paginação GraphQL.
  // Para simplificar, coletamos até 24 via paginação leve se disponível.

  const first = Math.min(24, count);
  const variables = {
    id: profileId,
    first,
  };

  const url = 'https://www.instagram.com/graphql/query/';
  const params = new URLSearchParams({
    query_hash: '003056d32c2554def87228bc3fd9668a', // user media
    variables: JSON.stringify(variables),
  });

  const headers = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': '*/*',
  };
  if (cookieHeader) headers['Cookie'] = cookieHeader;

  const resp = await got(`${url}?${params.toString()}`, { headers }).json();
  const edges = resp?.data?.user?.edge_owner_to_timeline_media?.edges ?? [];
  return edges.map(e => e.node);
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

      const nodes = await igFetchPostsShortcode(profile.id, postsLimit, cookieHeader);
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
