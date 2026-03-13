/**
 * StockLab Data Collector
 * GitHub Actions에서 10분마다 실행되어 데이터를 수집하고 JSON으로 저장
 */
// Node.js 18+ 네이티브 fetch 사용 (node-fetch 불필요)
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// ===== 수집 설정 =====
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

const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

// ===== Fetch Helpers =====
async function fetchJSON(url, timeout = 15000) {
  const ctrl = new AbortController();
  const tid = setTimeout(() => ctrl.abort(), timeout);
  try {
    const res = await fetch(url, { signal: ctrl.signal, headers: { 'User-Agent': BROWSER_UA } });
    clearTimeout(tid);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (e) { clearTimeout(tid); throw e; }
}

async function fetchText(url, timeout = 15000) {
  const ctrl = new AbortController();
  const tid = setTimeout(() => ctrl.abort(), timeout);
  try {
    const res = await fetch(url, { signal: ctrl.signal, headers: { 'User-Agent': BROWSER_UA, 'Accept-Language': 'ko-KR,ko;q=0.9' } });
    clearTimeout(tid);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.text();
  } catch (e) { clearTimeout(tid); throw e; }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function timeAgo(date) {
  if (isNaN(date.getTime())) return '';
  const diff = Date.now() - date.getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return '방금 전';
  if (mins < 60) return `${mins}분 전`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}시간 전`;
  const days = Math.floor(hrs / 24);
  if (days < 7) return `${days}일 전`;
  return `${Math.floor(days / 7)}주 전`;
}

// generateNearHistory 삭제됨 - 실제 데이터만 사용

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
// ⚠️ 프론트엔드 js/utils/sentiment.js와 동기화 필요
// 키워드를 추가/삭제할 때 양쪽 파일을 함께 수정하세요.
const KoreanSentiment = {
  // 커뮤니티 비속어/은어까지 포함한 확장 사전
  strongPositive: ['급등','폭등','대박','상한가','역대최고','신고가','대호재','초강세','텐배거','완전체','개이득','떡상','미친상승','불장','역대급'],
  positive: ['상승','매수','호재','돌파','반등','강세','기대','수익','실적개선','추천','좋은','긍정','성장','흑자','호실적','저평가','목표가','상향','모아가자','존버','매력적','오르','올라','갈거','간다','개꿀','굿','좋다','기회','바닥','매집','줍줍','저가매수','물타기','개좋','ㅋㅋ좋','가즈아','화이팅','축하','수익인증','익절','떡상','개꿀','레전드','찐이다','존맛','갓','핵이득'],
  strongNegative: ['급락','폭락','하한가','깡통','반토막','대폭락','대참사','물린','쪽박','망','개폭락','떡락','존망','핵폭락','개망','미친하락','개잡주','쓰레기주','먹튀','사기','나락','지옥','개거품','대참사'],
  negative: ['하락','매도','악재','손절','약세','우려','손실','실적악화','위험','나쁜','부정','적자','고평가','하향','빠진','떨어','떨어진','물려','빠질','내려','걱정','불안','위기','폭망','팔자','개미털기','못하','안좋','삼성망','ㅠ',
    // 커뮤니티 비속어/은어 (strongNegative와 중복 제거)
    '쓰레기','거지','ㅅㅂ','시발','ㅂㅅ','병신','ㄱㅅㄲ','개새','씹','좆','지랄','미친','개못','망했','망할','폭삭','폭망','꼴받','열받','어이없','한심','답없','후회','최악','거품','허수','조작','작전','세력놈','ㅠㅠ','ㅜㅜ','아놔','에휴','짜증','개별로','별로'],
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

// ===== Collectors =====
async function collectExchangeRates() {
  try {
    const data = await fetchJSON('https://open.er-api.com/v6/latest/USD');
    if (data.result !== 'success') throw new Error('API error');
    const krw = data.rates.KRW;
    return [
      { pair: 'USD/KRW', rate: krw, group: '주요' },
      { pair: 'EUR/KRW', rate: krw / data.rates.EUR, group: '주요' },
      { pair: 'JPY/KRW', rate: krw / data.rates.JPY, group: '아시아' },
      { pair: 'CNY/KRW', rate: krw / data.rates.CNY, group: '아시아' },
      { pair: 'GBP/KRW', rate: krw / data.rates.GBP, group: '주요' },
      { pair: 'AUD/KRW', rate: krw / data.rates.AUD, group: '주요' },
      { pair: 'SGD/KRW', rate: krw / data.rates.SGD, group: '아시아' },
      { pair: 'THB/KRW', rate: krw / data.rates.THB, group: '아시아' },
    ].map(p => {
      const value = Math.round(p.rate * 100) / 100;
      return { pair: p.pair, value, change: 0, history: [], live: true, group: p.group };
    });
  } catch (e) { console.warn('[Collect] 환율 실패:', e.message); return null; }
}

async function collectDXY() {
  try {
    const quote = await getYahooQuote('DX=F');
    if (!quote) return null;
    return { ...quote, name: 'US Dollar Index', symbol: 'DX=F' };
  } catch (e) { console.warn('[Collect] DXY 실패:', e.message); return null; }
}

async function getYahooQuote(symbol) {
  try {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=3mo&includePrePost=false`;
    const data = await fetchJSON(url, 15000);
    if (!data.chart || !data.chart.result) throw new Error('No data');
    const result = data.chart.result[0];
    const meta = result.meta;
    const closes = (result.indicators.quote[0].close || []).filter(v => v != null);
    if (closes.length === 0) throw new Error('No price data');
    const price = meta.regularMarketPrice;
    // 일일 변동: closes 배열의 마지막-1 값 (어제 종가) 사용
    // chartPreviousClose는 range 시작 시점 (3mo)이므로 일일 변동에 부적합
    const dailyPrevClose = closes.length >= 2 ? closes[closes.length - 2] : price;
    const prevClose3mo = meta.chartPreviousClose || closes[0] || price;
    return {
      price: Math.round(price), prevClose: Math.round(dailyPrevClose),
      change: Math.round(price - dailyPrevClose),
      changePercent: Math.round((price - dailyPrevClose) / dailyPrevClose * 10000) / 100,
      change3mo: Math.round(price - prevClose3mo),
      changePercent3mo: Math.round((price - prevClose3mo) / prevClose3mo * 10000) / 100,
      history: closes.slice(-8).map(v => Math.round(v)),
      fullCloses: closes.map(v => Math.round(v)),
      timestamps: result.timestamp, live: true,
    };
  } catch (e) { console.warn(`[Collect] Yahoo ${symbol} 실패:`, e.message); return null; }
}

async function collectIndices() {
  const [kospi, kosdaq] = await Promise.allSettled([getYahooQuote('^KS11'), getYahooQuote('^KQ11')]);
  return {
    kospi: kospi.status === 'fulfilled' ? kospi.value : null,
    kosdaq: kosdaq.status === 'fulfilled' ? kosdaq.value : null,
  };
}

const MAIN_STOCKS = [
  { code: '005930', name: '삼성전자', sector: '반도체' },
  { code: '000660', name: 'SK하이닉스', sector: '반도체' },
  { code: '005380', name: '현대차', sector: '자동차' },
  { code: '005490', name: 'POSCO홀딩스', sector: '철강' },
  { code: '105560', name: 'KB금융', sector: '금융' },
  { code: '055550', name: '신한지주', sector: '금융' },
  { code: '035420', name: 'NAVER', sector: 'IT' },
  { code: '035720', name: '카카오', sector: 'IT' },
  { code: '006400', name: '삼성SDI', sector: '2차전지' },
  { code: '051910', name: 'LG화학', sector: '2차전지' },
  { code: '068270', name: '셀트리온', sector: '바이오' },
  { code: '207940', name: '삼성바이오로직스', sector: '바이오' },
];

// ===== 네이버 금융 재무지표 스크래핑 (PER, PBR, 배당률, 시총) =====
async function scrapeNaverFinancials(code) {
  try {
    const html = await fetchText(`https://finance.naver.com/item/main.naver?code=${code}`, CONFIG.FETCH_TIMEOUT);
    const $ = cheerio.load(html);
    const result = {};

    // 시가총액: #_market_sum 또는 #_totalMarketValue
    const marketCapEl = $('#_market_sum, #_totalMarketValue');
    if (marketCapEl.length) {
      const raw = marketCapEl.text().replace(/[\s,원조억]/g, '').trim();
      // 네이버는 "억 원" 단위 표시 — 전체 텍스트에서 파싱
      const fullText = marketCapEl.closest('td, em').text().replace(/\s+/g, ' ').trim();
      // 시총을 억 단위로 가져오기
      const numMatch = marketCapEl.text().replace(/[^\d,]/g, '').replace(/,/g, '');
      if (numMatch) {
        const num = parseInt(numMatch);
        if (num >= 10000) {
          const jo = num / 10000;
          result.marketCap = jo >= 10 ? Math.round(jo) + '조' : jo.toFixed(1).replace(/\.0$/, '') + '조';
        } else result.marketCap = num.toLocaleString() + '억';
      }
    }

    // PER, PBR, 배당률: 종목 요약 테이블에서 가져오기
    const tables = $('table');
    tables.each((_, table) => {
      $(table).find('tr').each((_, tr) => {
        const text = $(tr).text().replace(/\s+/g, ' ');
        // PER
        if (text.includes('PER') && !result.per) {
          const m = text.match(/PER\s*[\(（].*?[\)）]?\s*([\d,.]+)/);
          if (m) result.per = parseFloat(m[1].replace(/,/g, ''));
        }
        // PBR
        if (text.includes('PBR') && !result.pbr) {
          const m = text.match(/PBR\s*([\d,.]+)/);
          if (m) result.pbr = parseFloat(m[1].replace(/,/g, ''));
        }
        // 배당수익률
        if (text.includes('배당수익률') && !result.divYield) {
          const m = text.match(/배당수익률\s*([\d,.]+)/);
          if (m) result.divYield = parseFloat(m[1].replace(/,/g, ''));
        }
      });
    });

    // 대안: em 태그에서 직접 추출
    if (!result.per || !result.pbr) {
      $('em').each((_, el) => {
        const id = $(el).attr('id') || '';
        const val = $(el).text().replace(/,/g, '').trim();
        if (id === '_per' && !result.per) result.per = parseFloat(val) || null;
        if (id === '_pbr' && !result.pbr) result.pbr = parseFloat(val) || null;
        if (id === '_dvr' && !result.divYield) result.divYield = parseFloat(val) || null;
      });
    }

    // 시총 대안: "시가총액" 행 텍스트에서 추출
    if (!result.marketCap) {
      $('td, th').each((_, el) => {
        const text = $(el).text();
        if (text.includes('시가총액')) {
          const next = $(el).next('td, em');
          if (next.length) {
            const raw = next.text().replace(/[^\d]/g, '');
            if (raw) {
              const num = parseInt(raw);
              if (num > 10000) result.marketCap = Math.round(num / 10000) + '조';
              else if (num > 0) result.marketCap = num.toLocaleString() + '억';
            }
          }
        }
      });
    }

    const hasData = result.per || result.pbr || result.divYield || result.marketCap;
    return hasData ? result : null;
  } catch (e) {
    console.warn(`[Collect] 네이버 재무 ${code} 실패:`, e.message);
    return null;
  }
}

async function collectStocks() {
  const results = [];
  for (let i = 0; i < MAIN_STOCKS.length; i += 3) {
    const batch = MAIN_STOCKS.slice(i, i + 3);
    const batchResults = await Promise.allSettled(batch.map(s => getYahooQuote(s.code + '.KS')));
    for (let j = 0; j < batch.length; j++) {
      const def = batch[j];
      const quote = batchResults[j].status === 'fulfilled' ? batchResults[j].value : null;
      if (quote) {
        results.push({ code: def.code, name: def.name, sector: def.sector, market: 'KOSPI', price: quote.price, change: quote.change, changePercent: quote.changePercent, history: quote.history, fullCloses: quote.fullCloses, timestamps: quote.timestamps, live: true });
      }
    }
    if (i + 3 < MAIN_STOCKS.length) await sleep(1000);
  }

  // 네이버 금융에서 재무지표 수집 (PER/PBR/배당률/시총)
  if (results.length > 0) {
    console.log('[Collect] 네이버 금융 재무지표 수집 시작...');
    for (let i = 0; i < results.length; i += 3) {
      const batch = results.slice(i, i + 3);
      const fundResults = await Promise.allSettled(batch.map(s => scrapeNaverFinancials(s.code)));
      for (let j = 0; j < batch.length; j++) {
        const fund = fundResults[j].status === 'fulfilled' ? fundResults[j].value : null;
        if (fund) {
          if (fund.per != null && !isNaN(fund.per)) batch[j].per = fund.per;
          if (fund.pbr != null && !isNaN(fund.pbr)) batch[j].pbr = fund.pbr;
          if (fund.divYield != null && !isNaN(fund.divYield)) batch[j].divYield = fund.divYield;
          else if (fund.divYield == null) batch[j].divYield = 0;
          if (fund.marketCap) batch[j].marketCap = fund.marketCap;
        }
      }
      if (i + 3 < results.length) await sleep(500);
    }
    const withFund = results.filter(s => s.per || s.pbr || s.divYield || s.marketCap).length;
    console.log(`[Collect] 네이버 재무지표: ${withFund}/${results.length}개 종목 수집 완료`);
  }

  return results.length > 0 ? results : null;
}

async function collectNews(query) {
  query = query || '한국 증시 주식 경제';
  try {
    const rssUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=ko&gl=KR&ceid=KR:ko`;
    const xml = await fetchText(rssUrl, 10000);
    const $ = cheerio.load(xml, { xmlMode: true });
    const articles = [];
    $('item').slice(0, 10).each((_, item) => {
      const rawTitle = $(item).find('title').text();
      const source = $(item).find('source').text();
      const pubDate = $(item).find('pubDate').text();
      const title = rawTitle.replace(/\s*-\s*[^-]+$/, '').trim() || rawTitle;
      articles.push({ title, source: source || '뉴스', time: timeAgo(new Date(pubDate)), live: true });
    });
    return articles.length > 0 ? articles : null;
  } catch (e) { console.warn('[Collect] 뉴스 실패:', e.message); return null; }
}

async function collectCalendar() {
  const queries = ['한국 경제 일정 발표', 'FOMC 금통위 실적발표'];
  const allEvents = [];
  for (const query of queries) {
    try {
      const rssUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=ko&gl=KR&ceid=KR:ko`;
      const xml = await fetchText(rssUrl, 10000);
      const $ = cheerio.load(xml, { xmlMode: true });
      $('item').slice(0, 5).each((_, item) => {
        const title = ($(item).find('title').text() || '').replace(/\s*-\s*[^-]+$/, '').trim();
        const pubDate = $(item).find('pubDate').text();
        const source = $(item).find('source').text();
        if (title) {
          const d = new Date(pubDate);
          const dateStr = isNaN(d.getTime()) ? '' : d.toISOString().split('T')[0];
          let importance = 'low';
          if (/FOMC|금통위|금리|기준금리|실적/.test(title)) importance = 'high';
          else if (/고용|물가|GDP|수출|무역|CPI|PMI/.test(title)) importance = 'medium';
          allEvents.push({ date: dateStr, title: title.substring(0, 40), importance, source, live: true });
        }
      });
    } catch (e) { /* skip */ }
  }
  const seen = new Set();
  return allEvents.filter(e => { const k = e.title.substring(0, 20); if (seen.has(k)) return false; seen.add(k); return true; }).slice(0, 8);
}

async function collectYouTube() {
  const queries = ['한국 주식 투자 유튜브', '주식 분석 전망 2026'];
  const allItems = [];
  for (const query of queries) {
    try {
      const rssUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=ko&gl=KR&ceid=KR:ko`;
      const xml = await fetchText(rssUrl, 10000);
      const $ = cheerio.load(xml, { xmlMode: true });
      $('item').slice(0, 5).each((_, item) => {
        const rawTitle = $(item).find('title').text();
        const source = $(item).find('source').text();
        const pubDate = $(item).find('pubDate').text();
        allItems.push({ title: rawTitle.replace(/\s*-\s*[^-]+$/, '').trim() || rawTitle, channel: source || '투자 채널', time: timeAgo(new Date(pubDate)), live: true });
      });
    } catch (e) { /* skip */ }
  }
  const seen = new Set();
  return allItems.filter(i => { const k = i.title.substring(0, 25); if (seen.has(k)) return false; seen.add(k); return true; }).slice(0, 8);
}

function extractTrends(news) {
  if (!news || news.length === 0) return null;
  const allText = news.map(n => n.title).join(' ');
  const candidates = ['삼성전자','SK하이닉스','현대차','FOMC','금리','환율','코스피','코스닥','반도체','AI','배당','실적','인플레','ETF','2차전지','바이오','원달러','KB금융','NAVER','카카오','LG','POSCO','셀트리온','한은','연준','물가','고용','GDP','수출','무역','증시','투자','테슬라','엔비디아','트럼프','관세','원유','금값','비트코인','부동산','IPO','공모주'];
  const keywords = [];
  candidates.forEach(word => {
    const matches = allText.match(new RegExp(word, 'g'));
    if (matches) keywords.push({ word, count: matches.length });
  });
  keywords.sort((a, b) => b.count - a.count);
  return keywords.slice(0, 16).map((k, i) => ({ word: k.word, count: k.count, size: i < 2 ? 5 : i < 5 ? 4 : i < 8 ? 3 : i < 12 ? 2 : 1, live: true }));
}

const ETF_DEFS = [
  { name: 'TIGER S&P500', code: '360750' },
  { name: 'KODEX 200', code: '069500' },
  { name: 'ACE 미국배당다우존스', code: '402460' },
  { name: 'TIGER 미국나스닥100', code: '133690' },
  { name: 'KODEX 배당가치', code: '290130' },
];

async function collectETFs() {
  const results = [];
  for (let i = 0; i < ETF_DEFS.length; i += 3) {
    const batch = ETF_DEFS.slice(i, i + 3);
    const batchResults = await Promise.allSettled(batch.map(e => getYahooQuote(e.code + '.KS')));
    for (let j = 0; j < batch.length; j++) {
      const def = batch[j];
      const quote = batchResults[j].status === 'fulfilled' ? batchResults[j].value : null;
      if (quote) {
        const closes = quote.fullCloses || [];
        const cur = closes[closes.length - 1] || quote.price;
        const m1y = closes.length > 250 ? closes[closes.length - 251] : closes[0];
        const return1y = m1y ? Math.round((cur - m1y) / m1y * 1000) / 10 : 0;
        results.push({ name: def.name, code: def.code, price: quote.price, change: quote.change, changePercent: quote.changePercent, return1y, history: quote.history, fullCloses: quote.fullCloses, timestamps: quote.timestamps, live: true });
      }
    }
    if (i + 3 < ETF_DEFS.length) await sleep(1000);
  }
  return results.length > 0 ? results : null;
}

async function collectUsdKrwChart() {
  try {
    const data = await fetchJSON('https://query1.finance.yahoo.com/v8/finance/chart/KRW=X?interval=1d&range=3mo', CONFIG.FETCH_TIMEOUT);
    const result = data.chart.result[0];
    const rawCloses = result.indicators.quote[0].close || [];
    const rawTimestamps = result.timestamp || [];
    // null 값을 제거하면서 labels/values 동기화
    const labels = [], values = [];
    for (let i = 0; i < rawCloses.length && i < rawTimestamps.length; i++) {
      if (rawCloses[i] != null) {
        const d = new Date(rawTimestamps[i] * 1000);
        labels.push(`${d.getMonth() + 1}/${d.getDate()}`);
        values.push(Math.round(rawCloses[i] * 100) / 100);
      }
    }
    return { labels, values, live: true };
  } catch (e) { console.warn('[Collect] USD/KRW 차트 실패:', e.message); return null; }
}

async function collectNaverDiscussion(stockCode) {
  try {
    const html = await fetchText(`https://finance.naver.com/item/board.naver?code=${stockCode}`, CONFIG.FETCH_TIMEOUT);
    const $ = cheerio.load(html);
    const posts = [];
    $('table.type2 tr').each((_, row) => {
      const cells = $(row).find('td');
      if (cells.length < 6) return;
      const titleEl = $(cells[1]).find('a');
      if (!titleEl.length) return;
      const title = (titleEl.attr('title') || titleEl.text() || '').trim();
      if (!title) return;
      const href = titleEl.attr('href') || '';
      const url = href ? `https://finance.naver.com${href}` : '';
      posts.push({ title, url, author: $(cells[2]).text().trim(), date: $(cells[0]).text().trim(), views: parseInt($(cells[3]).text().trim()) || 0, likes: parseInt($(cells[4]).text().trim()) || 0, dislikes: parseInt($(cells[5]).text().trim()) || 0, source: '네이버증권', live: true });
    });
    return posts.length > 0 ? posts : null;
  } catch (e) { console.warn(`[Collect] 네이버 토론방 ${stockCode} 실패:`, e.message); return null; }
}

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
      const postNo = num;
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
  const fallback = {
    kr: { label: '기준금리 (한국)', value: 2.75, unit: '%', change: -0.25, changeUnit: '%p', history: [3.5,3.5,3.5,3.25,3.25,3.0,3.0,2.75], live: false },
    us: { label: '기준금리 (미국)', value: 3.625, unit: '%', change: 0, changeUnit: '%p', history: [5.5,5.25,5.0,4.75,4.5,4.25,3.625,3.625], live: false },
  };
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
      fallback.kr = { label: '기준금리 (한국)', value: krRate, unit: '%', change: Math.round((krRate - krPrev) * 100) / 100, changeUnit: '%p', history: krHistory, live: true };
    }
    // 미국 금리: FRED FEDFUNDS CSV (실효 연방기금금리, API 키 불필요)
    try {
      const startDate = `${now.getFullYear() - 2}-01-01`;
      const csv = await fetchText(`https://fred.stlouisfed.org/graph/fredgraph.csv?id=FEDFUNDS&cosd=${startDate}`, 30000);
      const rows = csv.trim().split('\n').slice(1).map(line => { const [date, val] = line.split(','); return { date, value: parseFloat(val) }; }).filter(r => !isNaN(r.value));
      if (rows.length > 0) {
        const usRate = Math.round(rows[rows.length - 1].value * 100) / 100;
        const usHistory = rows.slice(-8).map(r => Math.round(r.value * 100) / 100);
        while (usHistory.length < 8) usHistory.unshift(usHistory[0]);
        const usPrev = usHistory.length >= 2 ? usHistory[usHistory.length - 2] : usRate;
        fallback.us = { label: '기준금리 (미국)', value: usRate, unit: '%', change: Math.round((usRate - usPrev) * 100) / 100, changeUnit: '%p', history: usHistory, live: true };
      }
    } catch (e2) {
      console.warn('[Collect] 미국 기준금리 FRED 실패:', e2.message);
    }
    return fallback;
  } catch (e) {
    console.warn('[Collect] 기준금리 실패:', e.message);
    return fallback;
  }
}

// ===== Main =====
async function main() {
  console.log(`[Collect] 시작: ${new Date().toISOString()}`);
  const startTime = Date.now();

  // Phase 1: 병렬 수집
  const [fxResult, indicesResult, newsResult, calendarResult, youtubeResult, usdkrwResult, baseRatesResult, dxyResult] = await Promise.allSettled([
    collectExchangeRates(), collectIndices(), collectNews(), collectCalendar(), collectYouTube(), collectUsdKrwChart(), collectBaseRates(), collectDXY(),
  ]);

  const exchangeRates = fxResult.status === 'fulfilled' ? fxResult.value : null;
  const indices = indicesResult.status === 'fulfilled' ? indicesResult.value : null;
  const news = newsResult.status === 'fulfilled' ? newsResult.value : null;
  const calendar = calendarResult.status === 'fulfilled' ? calendarResult.value : null;
  const youtube = youtubeResult.status === 'fulfilled' ? youtubeResult.value : null;
  const usdKrwChart = usdkrwResult.status === 'fulfilled' ? usdkrwResult.value : null;
  const baseRates = baseRatesResult.status === 'fulfilled' ? baseRatesResult.value : null;
  const dxy = dxyResult.status === 'fulfilled' ? dxyResult.value : null;

  // Phase 2: 뉴스 파생
  const briefing = news ? (() => {
    const today = new Date();
    const dateStr = `${today.getFullYear()}년 ${today.getMonth() + 1}월 ${today.getDate()}일`;
    const dayNames = ['일','월','화','수','목','금','토'];
    const allText = news.map(n => n.title).join(' ');
    const candidates = ['삼성전자','SK하이닉스','현대차','FOMC','금리','환율','코스피','코스닥','반도체','AI','배당','실적','인플레','ETF','2차전지','바이오','원달러','증시','무역','수출'];
    const keywords = candidates.filter(k => allText.includes(k)).slice(0, 6);
    if (keywords.length < 3) keywords.push('증시', '경제', '투자');
    return { date: `${dateStr} (${dayNames[today.getDay()]})`, content: news.slice(0, 5).map(n => n.title + (n.source ? ` (${n.source})` : '')), keywords: [...new Set(keywords)].slice(0, 6), live: true };
  })() : null;
  const trends = extractTrends(news);

  // Phase 3: 주식 + ETF
  const stocks = await collectStocks();
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
  const data = {
    indices, exchangeRates, news, briefing, stocks, etfs, trends, calendar, youtube, sentiments,
    communityPosts: communityPosts.slice(0, CONFIG.COMMUNITY_CDN_POSTS), // CDN fallback용 최근 게시글
    newsSentiment, usdKrwChart, baseRates, dxy,
    updatedAt: new Date().toISOString(),
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

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`[Collect] 완료 (${elapsed}s) - 환율:${!!exchangeRates} 지수:${!!indices} 뉴스:${news?.length || 0} 주식:${stocks?.length || 0} ETF:${etfs?.length || 0} 감성:${sentiments?.length || 0}`);
}

main().catch(e => { console.error('[Collect] 치명적 오류:', e); process.exit(1); });
