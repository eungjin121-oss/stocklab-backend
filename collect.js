/**
 * StockLab Data Collector
 * GitHub Actions에서 10분마다 실행되어 데이터를 수집하고 JSON으로 저장
 */
// Node.js 18+ 네이티브 fetch 사용 (node-fetch 불필요)
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// 공유 모듈
const { fetchJSON, fetchText, sleep, timeAgo, BROWSER_UA } = require('./lib/utils');
const {
  getYahooQuote, collectIndices, collectDXY, collectExchangeRates,
  collectStocks, collectUSStocks, collectNews, collectCalendar, collectYouTube,
  extractTrends, collectETFs, collectNaverDiscussion, collectUsdKrwChart,
  collectFearGreed,
} = require('./lib/collectors');

// ===== 수집 설정 =====
const FEAR_GREED_CACHE = path.join(__dirname, 'data', 'feargreed-cache.json');
const BASE_RATES_CACHE = path.join(__dirname, 'data', 'baserates-cache.json');
const BRIEFING_CACHE = path.join(__dirname, 'data', 'briefing-cache.json');
const DAILY_CACHE_TTL = 24 * 60 * 60 * 1000; // 24시간 (하루 최대 1번 호출)
const BRIEFING_CACHE_TTL = 12 * 60 * 60 * 1000; // 12시간 (하루 최대 2번 호출)

const CONFIG = {
  NAVER_POSTS_PER_STOCK: 10,
  DC_POSTS_LIMIT: 20,
  DC_BATCH_SIZE: 5,        // 병렬 크롤링 배치 크기
  COMMUNITY_CDN_POSTS: 30,  // CDN fallback(latest.json)에 포함할 최근 게시글 수
  COMMUNITY_HOME_POSTS: 10, // current/latest 문서에 포함할 홈 미리보기 게시글 수
  SLEEP_MS: 500,
  FETCH_TIMEOUT: 12000,
  DC_FETCH_TIMEOUT: 8000,
  FINBERT_BATCH_SIZE: 32,
};

// ===== Firebase Firestore =====
let db = null;
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  try {
    const admin = require('firebase-admin');
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    db = admin.firestore();
    console.log('[Firestore] 초기화 성공');
  } catch (e) {
    console.warn('[Firestore] 초기화 실패:', e.message);
  }
}

async function writeToFirestore(data) {
  if (!db) { console.log('[Firestore] DB 없음, 건너뜀'); return; }
  try {
    const now = new Date();
    const dateKey = now.toISOString().slice(0, 10); // "2026-03-13"
    const timeKey = String(now.getHours()).padStart(2, '0') + String(now.getMinutes()).padStart(2, '0');

    // communityPosts는 최근 10개만 current/latest에 저장 (홈 탭 미리보기용)
    // 전체 게시글은 community_posts 컬렉션에 별도 저장
    const dataForDoc = { ...data };
    if (dataForDoc.communityPosts && dataForDoc.communityPosts.length > CONFIG.COMMUNITY_HOME_POSTS) {
      dataForDoc.communityPosts = dataForDoc.communityPosts.slice(0, CONFIG.COMMUNITY_HOME_POSTS);
    }

    // 1. current/latest 덮어쓰기 (프론트엔드 최신 데이터용)
    await db.doc('current/latest').set(dataForDoc);

    // 2. snapshots/{date}/times/{HHmm} 히스토리 저장
    await db.collection('snapshots').doc(dateKey).collection('times').doc(timeKey).set(dataForDoc);

    console.log(`[Firestore] 저장 완료: current/latest + snapshots/${dateKey}/times/${timeKey}`);
  } catch (e) {
    console.error('[Firestore] 저장 실패:', e.message);
    // Non-fatal: CDN fallback 유지
  }
}

// ===== community_posts 컬렉션에 개별 문서로 저장 =====
async function saveCommunityPosts(posts) {
  if (!db || !posts || posts.length === 0) return;
  try {
    // Firestore batch write는 최대 500건이므로 나눠서 처리
    const BATCH_LIMIT = 500;
    let totalSaved = 0;

    for (let i = 0; i < posts.length; i += BATCH_LIMIT) {
      const chunk = posts.slice(i, i + BATCH_LIMIT);
      const batch = db.batch();

      for (const p of chunk) {
        // 중복 방지용 고유 ID 생성 (title + source 해시)
        const docId = Buffer.from(`${p.title}||${p.source}`).toString('base64url').slice(0, 40);
        const docRef = db.collection('community_posts').doc(docId);

        // set with merge: 이미 존재하면 업데이트, 없으면 생성
        batch.set(docRef, {
          ...p,
          createdAt: p.createdAt || new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        }, { merge: true });
      }

      await batch.commit();
      totalSaved += chunk.length;
    }

    console.log(`[Firestore] community_posts: ${totalSaved}건 저장 (${Math.ceil(posts.length / BATCH_LIMIT)}개 batch)`);
  } catch (e) {
    console.error('[Firestore] community_posts 저장 실패:', e.message);
  }
}

// ===== sentiment_history 컬렉션에 시계열 스냅샷 저장 =====
async function saveSentimentSnapshot(sentiments) {
  if (!db || !sentiments || sentiments.length === 0) return;
  try {
    const now = new Date().toISOString();
    const batch = db.batch();

    for (const s of sentiments) {
      const docId = `${s.stock}_${now.slice(0, 16).replace(/[:.]/g, '')}`;
      batch.set(db.collection('sentiment_history').doc(docId), {
        ...s,
        timestamp: now,
      });
    }

    await batch.commit();
    console.log(`[Firestore] sentiment_history: ${sentiments.length}건 저장`);
  } catch (e) {
    console.error('[Firestore] sentiment_history 저장 실패:', e.message);
  }
}

// Fear & Greed 히스토리를 Firestore에 누적 저장
async function saveFearGreedHistory(fearGreed) {
  if (!db || !fearGreed || fearGreed.score == null) return;
  try {
    const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    const docId = `fg_${today}`;
    await db.collection('feargreed_history').doc(docId).set({
      date: today,
      score: fearGreed.score,
      rating: fearGreed.rating,
      previousClose: fearGreed.previousClose,
      timestamp: new Date().toISOString(),
    }, { merge: true });
    console.log(`[Firestore] feargreed_history: ${today} 저장 (score: ${fearGreed.score})`);
  } catch (e) {
    console.error('[Firestore] feargreed_history 저장 실패:', e.message);
  }
}

// Fetch Helpers, collectors: lib/utils.js + lib/collectors.js에서 import

// ===== FinBert AI 감성분석 =====
async function analyzeWithFinBert(texts) {
  const token = process.env.HF_TOKEN;
  if (!token) { console.log('[FinBert] HF_TOKEN 없음 → 키워드 분석 fallback'); return null; }
  if (!texts || texts.length === 0) return null;

  const maxRetries = 2;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 30000);
      const res = await fetch('https://router.huggingface.co/hf-inference/models/snunlp/KR-FinBert-SC', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: texts, parameters: { top_k: null } }),
        signal: ctrl.signal,
      });
      clearTimeout(tid);

      if (res.status === 503) {
        // 모델 로딩 중 — 대기 후 재시도
        console.log(`[FinBert] 모델 로딩 중 (503), ${20}초 대기... (시도 ${attempt + 1}/${maxRetries + 1})`);
        await sleep(20000);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);

      const data = await res.json();
      // 응답 형식: [[{label,score},{label,score},{label,score}], ...] (텍스트별 3개 라벨)
      return data.map((item, i) => {
        // item이 배열이면 분류 결과, 아니면 에러
        if (!Array.isArray(item)) return { label: '중립', score: 0, model: 'keyword' };
        const scores = {};
        item.forEach(r => { scores[r.label] = r.score; });
        // 가장 높은 점수의 라벨 선택
        const best = item.reduce((a, b) => a.score > b.score ? a : b);
        // 라벨 정규화: 긍정/부정/중립
        let label = best.label;
        if (label === 'positive') label = '긍정';
        else if (label === 'negative') label = '부정';
        else if (label === 'neutral') label = '중립';
        return { label, score: Math.round(best.score * 1000) / 10, scores, model: 'finbert' };
      });
    } catch (e) {
      console.warn(`[FinBert] API 실패 (시도 ${attempt + 1}):`, e.message);
      if (attempt < maxRetries) { await sleep(5000); continue; }
      return null;
    }
  }
  return null;
}

// ===== 키워드 기반 감성 분석 (fallback + 하이브리드 보정용) =====
// 키워드 사전: shared/sentiment-keywords.json (프론트/백엔드 공유)
const _sentimentKw = require('./shared/sentiment-keywords.json');
const KoreanSentiment = {
  ..._sentimentKw,
  analyze(text) {
    if (!text) return { score: 0, label: '중립' };
    let score = 0;
    const found = { positive: [], negative: [] };
    this.strongPositive.forEach(w => { if (text.includes(w)) { score += 2; found.positive.push(w); } });
    this.positive.forEach(w => { if (text.includes(w)) { score += 1; found.positive.push(w); } });
    this.strongNegative.forEach(w => { if (text.includes(w)) { score -= 2; found.negative.push(w); } });
    this.negative.forEach(w => { if (text.includes(w)) { score -= 1; found.negative.push(w); } });
    let label = '중립';
    if (score >= 2) label = '매우 긍정';
    else if (score >= 1) label = '긍정';
    else if (score <= -2) label = '매우 부정';
    else if (score <= -1) label = '부정';
    return { score, label, positiveWords: found.positive, negativeWords: found.negative };
  }
};

// ===== 하이브리드 감성분석 (FinBert AI + 키워드 보정) =====
function hybridSentiment(finbertResult, text) {
  if (!finbertResult) return null;
  const kw = KoreanSentiment.analyze(text);

  // 키워드 신호가 강하고 AI가 중립이면 → 키워드로 보정
  const kwStrong = Math.abs(kw.score) >= 2; // 강한 키워드 신호
  const aiNeutral = finbertResult.label === '중립';
  const aiLowConf = finbertResult.score < 80; // AI 확신도 낮음

  let finalLabel = finbertResult.label;
  let corrected = false;

  if (kwStrong && aiNeutral) {
    // AI가 중립인데 키워드가 강하면 → 키워드 결과 채택
    finalLabel = normalizeLabel(kw.label);
    corrected = true;
  } else if (kwStrong && aiLowConf) {
    // AI 확신도 낮고 키워드 강하면 → 키워드 방향이 AI와 다를 때만 보정
    const kwDir = kw.score > 0 ? '긍정' : kw.score < 0 ? '부정' : '중립';
    if (kwDir !== finbertResult.label && kwDir !== '중립') {
      finalLabel = kwDir;
      corrected = true;
    }
  }

  return {
    label: finalLabel,
    score: finbertResult.score,
    scores: finbertResult.scores,
    model: 'hybrid',
    aiLabel: finbertResult.label,
    kwLabel: kw.label,
    kwScore: kw.score,
    corrected,
    positiveWords: kw.positiveWords,
    negativeWords: kw.negativeWords,
  };
}

// ===== 감성분석 헬퍼 함수 =====

/**
 * "매우 긍정" → "긍정", "매우 부정" → "부정" 정규화
 * 키워드 분석에서 나오는 강도 표현을 3단계(긍정/부정/중립)로 통일
 */
function normalizeLabel(label) {
  if (label === '매우 긍정') return '긍정';
  if (label === '매우 부정') return '부정';
  return label;
}

/**
 * 단일 텍스트에 대해 하이브리드 or 키워드 감성분석 결과 반환
 * FinBert 결과가 있으면 하이브리드 보정, 없으면 키워드 분석 fallback
 * @param {string} text - 분석할 텍스트 (제목 등)
 * @param {Object|null} finbertResult - analyzeWithFinBert 결과 중 해당 항목 ({ label, score, scores, model })
 * @returns {{ label, score, model, positiveWords?, negativeWords?, ... }}
 */
function analyzeSentiment(text, finbertResult) {
  if (finbertResult) {
    // 하이브리드: FinBert AI + 키워드 보정
    return hybridSentiment(finbertResult, text);
  }
  // 키워드 fallback
  const r = KoreanSentiment.analyze(text);
  return {
    label: normalizeLabel(r.label),
    score: Math.abs(r.score) * 10,
    model: 'keyword',
    positiveWords: r.positiveWords,
    negativeWords: r.negativeWords,
  };
}

/**
 * 게시글 배열의 긍정/부정/중립 비율(%) 집계
 * 각 게시글에 sentiment가 부착되어 있으면 그 label 기준,
 * 없으면 analyzeSentiment()로 분석 후 집계
 * @param {Array} posts - sentiment.label 또는 title을 가진 게시글 배열
 * @returns {{ positive: number, neutral: number, negative: number }}
 */
function aggregateSentiments(posts) {
  let pos = 0, neg = 0, neu = 0;
  posts.forEach(p => {
    const label = p.sentiment ? p.sentiment.label : analyzeSentiment(p.title, null).label;
    if (label === '긍정') pos++;
    else if (label === '부정') neg++;
    else neu++;
  });
  const total = posts.length || 1;
  return {
    positive: Math.round(pos / total * 100),
    neutral: Math.round(neu / total * 100),
    negative: Math.round(neg / total * 100),
  };
}

// ===== collect.js 전용 수집기 (DC Inside, 커뮤니티, 기준금리, AI briefing) =====
// 공유 수집기(환율, Yahoo, 뉴스 등)는 lib/collectors.js에서 import

async function collectDCInsideGallery() {
  try {
    const listUrl = 'https://gall.dcinside.com/mgallery/board/lists?id=stockus';
    const headers = {
      'User-Agent': BROWSER_UA,
      'Referer': listUrl,
      'Accept-Language': 'ko-KR,ko;q=0.9',
    };
    const ctrl = new AbortController();
    const tid = setTimeout(() => ctrl.abort(), 10000);
    const res = await fetch(listUrl, { signal: ctrl.signal, headers });
    clearTimeout(tid);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const html = await res.text();
    const $dc = cheerio.load(html);
    const posts = [];
    $dc('tr.ub-content').each((_, row) => {
      const $row = $dc(row);
      const num = $row.find('.gall_num').text().trim();
      if (!num || isNaN(num)) return; // 공지(텍스트)는 제외
      const titleA = $row.find('.gall_tit a').first();
      const title = titleA.text().trim();
      if (!title) return;
      // href에서 게시글 번호 추출 (더 안정적)
      const href = titleA.attr('href') || '';
      const hrefNoMatch = href.match(/no=(\d+)/);
      const postNo = hrefNoMatch ? hrefNoMatch[1] : num;
      const author = $row.find('.gall_writer').attr('data-nick') || $row.find('.gall_writer .nickname').text().trim() || $row.find('.gall_writer').text().trim();
      const date = $row.find('.gall_date').attr('title') || $row.find('.gall_date').text().trim();
      const views = parseInt($row.find('.gall_count').text().trim()) || 0;
      const likes = parseInt($row.find('.gall_recommend').text().trim()) || 0;
      posts.push({ title, author, date, views, likes, dislikes: 0, source: 'DC미국주식', stock: '미국주식', live: true, postNo: parseInt(postNo) });
    });
    // 번호 내림차순 정렬 후 최근 글만 (공지성 오래된 글 제거)
    posts.sort((a, b) => b.postNo - a.postNo);
    const recentPosts = posts.filter(p => {
      const d = new Date(p.date);
      return !isNaN(d.getTime()) && (Date.now() - d.getTime()) < 3 * 24 * 60 * 60 * 1000;
    });
    const targetPosts = recentPosts.length > 0 ? recentPosts : posts.slice(0, CONFIG.DC_POSTS_LIMIT);
    // 상위 게시글 본문 미리보기 (배치 병렬 처리)
    const previewTargets = targetPosts.slice(0, CONFIG.DC_POSTS_LIMIT);
    for (let bi = 0; bi < previewTargets.length; bi += CONFIG.DC_BATCH_SIZE) {
      const batch = previewTargets.slice(bi, bi + CONFIG.DC_BATCH_SIZE);
      const batchResults = await Promise.allSettled(batch.map(async (p) => {
        const detailUrl = `https://gall.dcinside.com/mgallery/board/view?id=stockus&no=${p.postNo}`;
        const ctrl2 = new AbortController();
        const tid2 = setTimeout(() => ctrl2.abort(), CONFIG.DC_FETCH_TIMEOUT);
        const dRes = await fetch(detailUrl, { signal: ctrl2.signal, headers: { ...headers, 'Referer': listUrl } });
        clearTimeout(tid2);
        if (dRes.ok) {
          const dHtml = await dRes.text();
          const d$ = cheerio.load(dHtml);
          const body = d$('.write_div').text().trim().replace(/\s+/g, ' ');
          return body ? body.substring(0, 150) : null;
        }
        return null;
      }));
      // 결과 반영: 실패한 요청은 preview=null
      batch.forEach((p, i) => {
        p.preview = batchResults[i].status === 'fulfilled' ? batchResults[i].value : null;
        p.url = `https://gall.dcinside.com/mgallery/board/view?id=stockus&no=${p.postNo}`;
        delete p.postNo;
      });
      // 배치 간에만 sleep 적용
      if (bi + CONFIG.DC_BATCH_SIZE < previewTargets.length) await sleep(CONFIG.SLEEP_MS);
    }
    // postNo 정리 + URL 생성
    const finalPosts = targetPosts.slice(0, CONFIG.DC_POSTS_LIMIT);
    finalPosts.forEach(p => {
      if (!p.url && p.postNo) p.url = `https://gall.dcinside.com/mgallery/board/view?id=stockus&no=${p.postNo}`;
      delete p.postNo;
    });
    console.log(`[Collect] DC미국주식갤러리: ${finalPosts.length}건 수집`);
    return finalPosts;
  } catch (e) {
    console.warn('[Collect] DC미국주식갤러리 실패:', e.message);
    return [];
  }
}

async function collectCommunityPosts() {
  const targets = [
    { name: '삼성전자', code: '005930' }, { name: 'SK하이닉스', code: '000660' },
    { name: '현대차', code: '005380' }, { name: 'KB금융', code: '105560' }, { name: '카카오', code: '035720' },
  ];
  const allPosts = []; // 커뮤니티 글 목록 (콘텐츠 허브용)
  const stockPostMap = {}; // stock별 posts 매핑

  // 1단계: 모든 종목의 게시글 수집
  for (const t of targets) {
    try {
      const posts = await collectNaverDiscussion(t.code);
      if (posts && posts.length > 0) {
        stockPostMap[t.name] = posts;
        allPosts.push(...posts.slice(0, CONFIG.NAVER_POSTS_PER_STOCK).map(p => ({ ...p, stock: t.name })));
      }
      await sleep(CONFIG.SLEEP_MS);
    } catch (e) { /* skip */ }
  }

  // 1.5단계: DCInside 미국주식갤러리 수집
  try {
    const dcPosts = await collectDCInsideGallery();
    if (dcPosts.length > 0) {
      allPosts.push(...dcPosts.slice(0, CONFIG.DC_POSTS_LIMIT));
      console.log(`[Collect] DC 게시글 ${dcPosts.length}건 추가`);
    }
  } catch (e) { console.warn('[Collect] DC 통합 실패:', e.message); }

  return { allPosts, stockPostMap, targets };
}

function collectSentiments(allPosts, stockPostMap, targets, finbertResults) {
  // FinBert 결과 적용 (외부에서 전달받은 결과 사용)
  const useFinBert = finbertResults && finbertResults.length === allPosts.length;
  const sentimentModel = useFinBert ? 'hybrid' : 'keyword';
  console.log(`[Collect] 감성분석 모델: ${sentimentModel} (게시글 ${allPosts.length}건)`);

  // 게시글에 감성 결과 부착 (analyzeSentiment 헬퍼 사용)
  let correctedCount = 0;
  allPosts.forEach((p, i) => {
    p.sentiment = analyzeSentiment(p.title, useFinBert ? finbertResults[i] : null);
    if (p.sentiment.corrected) correctedCount++;
  });
  if (useFinBert) {
    console.log(`[Collect] 하이브리드 보정: ${correctedCount}/${allPosts.length}건 키워드 보정됨`);
  }

  // 종목별 감성 집계 (전체 게시글 기준)
  // 배치에 포함된 상위 게시글은 이미 sentiment 부착됨 → 재활용
  // 나머지 게시글은 analyzeSentiment(키워드 fallback)로 부착
  const results = [];
  for (const t of targets) {
    const posts = stockPostMap[t.name];
    if (!posts || posts.length === 0) continue;

    const topPosts = allPosts.filter(p => p.stock === t.name);
    posts.forEach(p => {
      if (!p.sentiment) {
        // 배치에 포함된 게시글의 결과 재활용
        const top = topPosts.find(tp => tp.title === p.title);
        p.sentiment = top && top.sentiment ? top.sentiment : analyzeSentiment(p.title, null);
      }
    });
    const pct = aggregateSentiments(posts);
    results.push({ stock: t.name, ...pct, mentions: posts.length, live: true, sentimentModel });
  }

  return {
    sentiments: results.length > 0 ? results : null,
    communityPosts: allPosts.length > 0 ? allPosts : null,
  };
}

async function collectBaseRates() {
  // 하드코딩 fallback 없음 — API 실패 시 해당 항목은 null
  const result = { kr: null, us: null };
  try {
    // 한국은행 ECOS API (sample 키, 무료)
    const now = new Date();
    const endYM = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}`;
    const startY = now.getFullYear() - 1;
    const startYM = `${startY}01`;
    const url = `https://ecos.bok.or.kr/api/StatisticSearch/sample/json/kr/1/10/722Y001/M/${startYM}/${endYM}/0101000`;
    const data = await fetchJSON(url, 10000);
    const rows = data?.StatisticSearch?.row;
    if (rows && rows.length > 0) {
      const values = rows.map(r => parseFloat(r.DATA_VALUE));
      const krRate = values[values.length - 1];
      const krHistory = values.slice(-8);
      while (krHistory.length < 8) krHistory.unshift(krHistory[0]);
      const krPrev = krHistory.length >= 2 ? krHistory[krHistory.length - 2] : krRate;
      result.kr = { label: '기준금리 (한국)', value: krRate, unit: '%', change: Math.round((krRate - krPrev) * 100) / 100, changeUnit: '%p', history: krHistory, live: true };
    }
  } catch (e) {
    console.warn('[Collect] 한국 기준금리 ECOS 실패:', e.message);
  }
  // 미국 금리: FRED FEDFUNDS CSV (실효 연방기금금리, API 키 불필요)
  try {
    const now = new Date();
    const startDate = `${now.getFullYear() - 2}-01-01`;
    const csv = await fetchText(`https://fred.stlouisfed.org/graph/fredgraph.csv?id=FEDFUNDS&cosd=${startDate}`, 30000);
    const rows = csv.trim().split('\n').slice(1).map(line => { const [date, val] = line.split(','); return { date, value: parseFloat(val) }; }).filter(r => !isNaN(r.value));
    if (rows.length > 0) {
      const usRate = Math.round(rows[rows.length - 1].value * 100) / 100;
      const usHistory = rows.slice(-8).map(r => Math.round(r.value * 100) / 100);
      while (usHistory.length < 8) usHistory.unshift(usHistory[0]);
      const usPrev = usHistory.length >= 2 ? usHistory[usHistory.length - 2] : usRate;
      result.us = { label: '기준금리 (미국)', value: usRate, unit: '%', change: Math.round((usRate - usPrev) * 100) / 100, changeUnit: '%p', history: usHistory, live: true };
    }
  } catch (e2) {
    console.warn('[Collect] 미국 기준금리 FRED 실패:', e2.message);
  }
  // 둘 다 null이면 null 반환
  if (!result.kr && !result.us) return null;
  return result;
}

// collectFearGreed → lib/collectors.js에서 import

// ===== AI Briefing =====
function fallbackBriefing(news) {
  if (!news || news.length === 0) return null;
  const today = new Date();
  const dateStr = `${today.getFullYear()}년 ${today.getMonth() + 1}월 ${today.getDate()}일`;
  const dayNames = ['일','월','화','수','목','금','토'];
  const allText = news.map(n => n.title).join(' ');
  const candidates = ['삼성전자','SK하이닉스','현대차','FOMC','금리','환율','코스피','코스닥','반도체','AI','배당','실적','인플레','ETF','2차전지','바이오','원달러','증시','무역','수출'];
  const keywords = candidates.filter(k => allText.includes(k)).slice(0, 6);
  if (keywords.length < 3) keywords.push('증시', '경제', '투자');
  return {
    date: `${dateStr} (${dayNames[today.getDay()]})`,
    content: news.slice(0, 5).map(n => n.title + (n.source ? ` (${n.source})` : '')),
    keywords: [...new Set(keywords)].slice(0, 6),
    live: false,
  };
}

async function generateAIBriefing(news) {
  if (!news || news.length === 0) return null;
  try {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) { console.warn('[Collect] OPENAI_API_KEY 없음 → fallback'); return fallbackBriefing(news); }
    const headlines = news.slice(0, 15).map((n, i) => `${i + 1}. ${n.title}${n.source ? ` (${n.source})` : ''}`).join('\n');

    const today = new Date();
    const dateStr = `${today.getFullYear()}년 ${today.getMonth() + 1}월 ${today.getDate()}일`;
    const dayNames = ['일','월','화','수','목','금','토'];
    const dateFormatted = `${dateStr} (${dayNames[today.getDay()]})`;

    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: 'gpt-4.1-nano',
        messages: [
          {
            role: 'system',
            content: '한국 금융시장 뉴스를 요약하는 AI 브리핑 어시스턴트. 반드시 JSON으로만 응답해.',
          },
          {
            role: 'user',
            content: `오늘 주요 금융 뉴스 헤드라인:\n${headlines}\n\n위 뉴스를 분석해서 다음 JSON 형식으로 응답해줘:\n{"summary":["핵심 요약 1문장","핵심 요약 2문장","핵심 요약 3문장"], "keywords":["키워드1","키워드2","키워드3","키워드4","키워드5","키워드6"], "sentiment":"bullish 또는 bearish 또는 neutral"}\n\nsummary는 오늘 시장 동향을 3문장으로 요약. keywords는 주요 키워드 6개. sentiment는 전체 시장 분위기.`,
          },
        ],
        max_tokens: 500,
        temperature: 0.3,
      }),
    });

    if (!response.ok) {
      throw new Error(`OpenAI API 응답 오류: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    const rawContent = data.choices[0].message.content.trim();

    // JSON 파싱 (코드블록 감싸진 경우 처리)
    const jsonStr = rawContent.replace(/^```json?\s*/, '').replace(/\s*```$/, '');
    const parsed = JSON.parse(jsonStr);

    console.log(`[Collect] AI 브리핑 생성 성공 (sentiment: ${parsed.sentiment})`);

    return {
      date: dateFormatted,
      content: parsed.summary || [],
      keywords: (parsed.keywords || []).slice(0, 6),
      sentiment: parsed.sentiment || 'neutral',
      live: true,
    };
  } catch (e) {
    console.warn('[Collect] AI 브리핑 실패, fallback:', e.message);
    return fallbackBriefing(news);
  }
}

// ===== AI 브리핑 캐시 수집 (12시간 TTL) =====
async function getBriefingCached(news) {
  try {
    if (fs.existsSync(BRIEFING_CACHE)) {
      const cached = JSON.parse(fs.readFileSync(BRIEFING_CACHE, 'utf-8'));
      const age = Date.now() - (cached._fetchedAt || 0);
      if (age < BRIEFING_CACHE_TTL) {
        console.log(`[Collect] AI 브리핑 캐시 사용 (${Math.round(age / 3600000)}시간 전 생성)`);
        const { _fetchedAt, ...data } = cached;
        return { ...data, _collectedAt: new Date(_fetchedAt).toISOString() };
      }
    }
  } catch (e) { /* 캐시 읽기 실패 → 새로 생성 */ }

  try {
    const data = await generateAIBriefing(news);
    if (data && data.live) {
      const now = Date.now();
      const dataDir = path.join(__dirname, 'data');
      if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
      fs.writeFileSync(BRIEFING_CACHE, JSON.stringify({ ...data, _fetchedAt: now }));
      console.log('[Collect] AI 브리핑 생성 및 캐시 저장');
      return { ...data, _collectedAt: new Date(now).toISOString() };
    }
    return data; // fallback (non-live)
  } catch (e) {
    console.warn('[Collect] AI 브리핑 생성 실패:', e.message);
  }

  // 만료 캐시 재사용
  try {
    if (fs.existsSync(BRIEFING_CACHE)) {
      const cached = JSON.parse(fs.readFileSync(BRIEFING_CACHE, 'utf-8'));
      const { _fetchedAt, ...data } = cached;
      console.log('[Collect] AI 브리핑 만료 캐시 재사용');
      return { ...data, _collectedAt: new Date(_fetchedAt).toISOString() };
    }
  } catch (e) { /* ignore */ }

  return fallbackBriefing(news);
}

// ===== Fear & Greed 캐시 수집 (12시간 TTL) =====
async function getFearGreedCached() {
  try {
    if (fs.existsSync(FEAR_GREED_CACHE)) {
      const cached = JSON.parse(fs.readFileSync(FEAR_GREED_CACHE, 'utf-8'));
      const age = Date.now() - (cached._fetchedAt || 0);
      if (age < DAILY_CACHE_TTL) {
        console.log(`[Collect] Fear & Greed 캐시 사용 (${Math.round(age / 3600000)}시간 전 수집)`);
        const { _fetchedAt, ...data } = cached;
        return { ...data, _collectedAt: new Date(_fetchedAt).toISOString() };
      }
    }
  } catch (e) { /* 캐시 읽기 실패 → 새로 수집 */ }

  try {
    const data = await collectFearGreed();
    if (data) {
      const now = Date.now();
      const dataDir = path.join(__dirname, 'data');
      if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
      fs.writeFileSync(FEAR_GREED_CACHE, JSON.stringify({ ...data, _fetchedAt: now }));
      console.log(`[Collect] Fear & Greed 수집 성공 (score: ${data.score})`);
      return { ...data, _collectedAt: new Date(now).toISOString() };
    }
  } catch (e) {
    console.warn(`[Collect] Fear & Greed 수집 실패: ${e.message}`);
  }

  try {
    if (fs.existsSync(FEAR_GREED_CACHE)) {
      const cached = JSON.parse(fs.readFileSync(FEAR_GREED_CACHE, 'utf-8'));
      const { _fetchedAt, ...data } = cached;
      console.log('[Collect] Fear & Greed 만료 캐시 재사용');
      return { ...data, _collectedAt: new Date(_fetchedAt).toISOString() };
    }
  } catch (e) { /* ignore */ }

  return null;
}

// ===== 기준금리 캐시 수집 (24시간 TTL) =====
async function getBaseRatesCached() {
  try {
    if (fs.existsSync(BASE_RATES_CACHE)) {
      const cached = JSON.parse(fs.readFileSync(BASE_RATES_CACHE, 'utf-8'));
      const age = Date.now() - (cached._fetchedAt || 0);
      if (age < DAILY_CACHE_TTL) {
        console.log(`[Collect] 기준금리 캐시 사용 (${Math.round(age / 3600000)}시간 전 수집)`);
        const { _fetchedAt, ...data } = cached;
        return { ...data, _collectedAt: new Date(_fetchedAt).toISOString() };
      }
    }
  } catch (e) { /* 캐시 읽기 실패 → 새로 수집 */ }

  try {
    const data = await collectBaseRates();
    if (data && (data.kr?.live || data.us?.live)) {
      const now = Date.now();
      const dataDir = path.join(__dirname, 'data');
      if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
      // 기존 캐시와 병합: 한쪽만 수집 성공해도 다른 쪽 데이터 유지
      let merged = { ...data };
      try {
        if (fs.existsSync(BASE_RATES_CACHE)) {
          const prev = JSON.parse(fs.readFileSync(BASE_RATES_CACHE, 'utf-8'));
          if (!merged.kr && prev.kr) merged.kr = prev.kr;
          if (!merged.us && prev.us) merged.us = prev.us;
        }
      } catch (e) { /* 캐시 읽기 실패 무시 */ }
      fs.writeFileSync(BASE_RATES_CACHE, JSON.stringify({ ...merged, _fetchedAt: now }));
      console.log(`[Collect] 기준금리 수집 성공 (한국: ${merged.kr?.value}%, 미국: ${merged.us?.value}%)`);
      return { ...merged, _collectedAt: new Date(now).toISOString() };
    }
  } catch (e) {
    console.warn(`[Collect] 기준금리 수집 실패: ${e.message}`);
  }

  try {
    if (fs.existsSync(BASE_RATES_CACHE)) {
      const cached = JSON.parse(fs.readFileSync(BASE_RATES_CACHE, 'utf-8'));
      const { _fetchedAt, ...data } = cached;
      console.log('[Collect] 기준금리 만료 캐시 재사용');
      return { ...data, _collectedAt: new Date(_fetchedAt).toISOString() };
    }
  } catch (e) { /* ignore */ }

  return null;
}

// ===== Main =====
async function main() {
  console.log(`[Collect] 시작: ${new Date().toISOString()}`);
  const startTime = Date.now();

  // Phase 1: 병렬 수집 (Fear & Greed는 12시간 캐시로 별도 관리)
  const [fxResult, indicesResult, newsResult, calendarResult, youtubeResult, usdkrwResult, dxyResult] = await Promise.allSettled([
    collectExchangeRates(), collectIndices(), collectNews(), collectCalendar(), collectYouTube(), collectUsdKrwChart(), collectDXY(),
  ]);

  const exchangeRates = fxResult.status === 'fulfilled' ? fxResult.value : null;
  const indices = indicesResult.status === 'fulfilled' ? indicesResult.value : null;
  const news = newsResult.status === 'fulfilled' ? newsResult.value : null;
  const calendar = calendarResult.status === 'fulfilled' ? calendarResult.value : null;
  const youtube = youtubeResult.status === 'fulfilled' ? youtubeResult.value : null;
  const usdKrwChart = usdkrwResult.status === 'fulfilled' ? usdkrwResult.value : null;
  const dxy = dxyResult.status === 'fulfilled' ? dxyResult.value : null;

  // 24시간 캐시 데이터 (하루 1회 수집)
  const fearGreed = await getFearGreedCached();
  const baseRates = await getBaseRatesCached();

  // Phase 2: 뉴스 파생 (AI 브리핑 — 12시간 캐시)
  const briefing = await getBriefingCached(news);
  const trends = extractTrends(news);

  // Phase 3: 주식 (한국 + 미국) + ETF
  const stocks = await collectStocks({ withFinancials: true, fetchTimeout: CONFIG.FETCH_TIMEOUT });
  await sleep(2000);
  const usStocks = await collectUSStocks();
  await sleep(2000);
  const etfs = await collectETFs();

  // Phase 4: 커뮤니티 게시글 수집
  await sleep(2000);
  const { allPosts: communityAllPosts, stockPostMap, targets: sentimentTargets } = await collectCommunityPosts();

  // Phase 5: FinBert AI 통합 감성분석 (커뮤니티 + 뉴스 타이틀을 합쳐서 1번만 호출)
  const communityTitles = communityAllPosts.map(p => p.title);
  const newsTitles = (news && news.length > 0) ? news.map(n => n.title) : [];
  const allTitles = [...communityTitles, ...newsTitles];
  const allFinbertResults = await analyzeWithFinBert(allTitles);
  const useFinBert = allFinbertResults && allFinbertResults.length === allTitles.length;
  console.log(`[Collect] FinBert 통합 호출: 커뮤니티 ${communityTitles.length}건 + 뉴스 ${newsTitles.length}건 = ${allTitles.length}건`);

  // FinBert 결과 분리: 커뮤니티용 / 뉴스용
  const communityFinbert = useFinBert ? allFinbertResults.slice(0, communityTitles.length) : null;
  const newsFinbert = useFinBert ? allFinbertResults.slice(communityTitles.length) : null;

  // 커뮤니티 감성분석 적용
  const sentimentResult = collectSentiments(communityAllPosts, stockPostMap, sentimentTargets, communityFinbert);
  const sentiments = sentimentResult.sentiments;
  const newPosts = sentimentResult.communityPosts || [];

  // 뉴스 감성분석 적용 (analyzeSentiment 헬퍼 사용)
  let newsSentiment = null;
  if (news && news.length > 0) {
    const useNewsFinBert = newsFinbert && newsFinbert.length === newsTitles.length;
    let newsCorrected = 0;
    news.forEach((n, i) => {
      n.sentiment = analyzeSentiment(n.title, useNewsFinBert ? newsFinbert[i] : null);
      if (n.sentiment.corrected) newsCorrected++;
    });
    const model = useNewsFinBert ? 'hybrid' : 'keyword';
    newsSentiment = { ...aggregateSentiments(news), model };
    if (useNewsFinBert) {
      console.log(`[Collect] 뉴스 감성분석 (hybrid): 긍정${newsSentiment.positive}% 중립${newsSentiment.neutral}% 부정${newsSentiment.negative}% (보정 ${newsCorrected}건)`);
    }
  }

  // 커뮤니티 게시글: community_posts 컬렉션에 개별 문서로 저장 (무제한 누적)
  // current/latest 문서에는 최근 10개만 (writeToFirestore에서 처리)
  // latest.json CDN fallback에는 최근 30개만
  const communityPosts = newPosts;
  console.log(`[Collect] 커뮤니티 게시글: ${communityPosts.length}건 수집 → community_posts 컬렉션에 개별 저장`);

  // 결과 저장 (latest.json CDN fallback: communityPosts 최근 30개만)
  const now = new Date().toISOString();
  const data = {
    indices, exchangeRates, news, briefing, stocks, usStocks, etfs, trends, calendar, youtube, sentiments,
    communityPosts: communityPosts.slice(0, CONFIG.COMMUNITY_CDN_POSTS),
    newsSentiment, usdKrwChart, baseRates, dxy, fearGreed,
    updatedAt: now,
    // 항목별 개별 수집 일시
    _collectedAt: {
      indices: indices ? now : null,
      exchangeRates: exchangeRates ? now : null,
      news: news ? now : null,
      briefing: briefing ? now : null,
      stocks: stocks ? now : null,
      usStocks: usStocks ? now : null,
      etfs: etfs ? now : null,
      calendar: calendar ? now : null,
      youtube: youtube ? now : null,
      sentiments: sentiments ? now : null,
      newsSentiment: newsSentiment ? now : null,
      usdKrwChart: usdKrwChart ? now : null,
      dxy: dxy ? now : null,
      baseRates: baseRates?._collectedAt || now,
      fearGreed: fearGreed?._collectedAt || fearGreed?.timestamp || now,
    },
  };

  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  fs.writeFileSync(path.join(dataDir, 'latest.json'), JSON.stringify(data));

  // Firestore에 저장 (current/latest + snapshots)
  // writeToFirestore 내부에서 communityPosts를 최근 10개로 잘라서 저장
  await writeToFirestore(data);

  // community_posts 컬렉션에 전체 게시글 개별 문서로 저장 (무제한 누적, 중복 방지)
  await saveCommunityPosts(communityPosts);

  // sentiment_history 컬렉션에 감성분석 시계열 스냅샷 저장
  await saveSentimentSnapshot(sentiments);

  // feargreed_history 컬렉션에 공탐지수 누적 저장
  await saveFearGreedHistory(fearGreed);

  // API 키는 백엔드 프록시에서만 사용 (Firestore 저장 중단)

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`[Collect] 완료 (${elapsed}s) - 환율:${!!exchangeRates} 지수:${!!indices} 뉴스:${news?.length || 0} 주식:${stocks?.length || 0} ETF:${etfs?.length || 0} 감성:${sentiments?.length || 0}`);
}

main().catch(e => { console.error('[Collect] 치명적 오류:', e); process.exit(1); });
