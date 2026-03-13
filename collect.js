/**
 * StockLab Data Collector
 * GitHub Actions에서 10분마다 실행되어 데이터를 수집하고 JSON으로 저장
 */
const fetch = require('node-fetch');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

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

    // 1. current/latest 덮어쓰기 (프론트엔드 최신 데이터용)
    await db.doc('current/latest').set(data);

    // 2. snapshots/{date}/times/{HHmm} 히스토리 저장
    await db.collection('snapshots').doc(dateKey).collection('times').doc(timeKey).set(data);

    console.log(`[Firestore] 저장 완료: current/latest + snapshots/${dateKey}/times/${timeKey}`);
  } catch (e) {
    console.error('[Firestore] 저장 실패:', e.message);
    // Non-fatal: CDN fallback 유지
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

function generateNearHistory(current, count) {
  const arr = [];
  for (let i = count - 1; i >= 0; i--) {
    const variance = current * 0.005 * (Math.random() - 0.5);
    arr.push(Math.round((current + variance * (i + 1)) * 100) / 100);
  }
  arr[arr.length - 1] = current;
  return arr;
}

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
    finalLabel = kw.label;
    if (finalLabel === '매우 긍정') finalLabel = '긍정';
    if (finalLabel === '매우 부정') finalLabel = '부정';
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
      return { pair: p.pair, value, change: 0, history: generateNearHistory(value, 8), live: true, group: p.group, demoFields: ['change','history'] };
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
    const prevClose = meta.chartPreviousClose || meta.previousClose || closes[closes.length - 2] || price;
    return {
      price: Math.round(price), prevClose: Math.round(prevClose),
      change: Math.round(price - prevClose),
      changePercent: Math.round((price - prevClose) / prevClose * 10000) / 100,
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
        allItems.push({ title: rawTitle.replace(/\s*-\s*[^-]+$/, '').trim() || rawTitle, channel: source || '투자 채널', views: '', time: timeAgo(new Date(pubDate)), live: true });
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
    if (matches) keywords.push({ word, count: matches.length * 50 + Math.floor(Math.random() * 100) });
  });
  keywords.sort((a, b) => b.count - a.count);
  return keywords.slice(0, 16).map((k, i) => ({ word: k.word, count: k.count, size: i < 2 ? 5 : i < 5 ? 4 : i < 8 ? 3 : i < 12 ? 2 : 1, live: true, demoFields: ['count'] }));
}

const ETF_DEFS = [
  { name: 'TIGER S&P500', code: '360750', fee: 0.07, topHolding: 'AAPL 7.2%' },
  { name: 'KODEX 200', code: '069500', fee: 0.15, topHolding: '삼성전자 25%' },
  { name: 'ACE 미국배당다우존스', code: '402460', fee: 0.12, topHolding: 'JNJ 4.1%' },
  { name: 'TIGER 미국나스닥100', code: '133690', fee: 0.07, topHolding: 'MSFT 8.5%' },
  { name: 'KODEX 배당가치', code: '290130', fee: 0.12, topHolding: '하나금융 6.8%' },
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
        results.push({ name: def.name, code: def.code, fee: def.fee, price: quote.price, change: quote.change, changePercent: quote.changePercent, return1y, return3y: Math.round(return1y * 2.5 * 10) / 10, aum: '-', divYield: def.code === '290130' ? 4.2 : def.code === '402460' ? 3.5 : def.code === '069500' ? 1.8 : def.code === '133690' ? 0.5 : 1.2, topHolding: def.topHolding, history: quote.history, fullCloses: quote.fullCloses, timestamps: quote.timestamps, live: true, demoFields: ['fee','divYield','topHolding','return3y','aum'] });
      }
    }
    if (i + 3 < ETF_DEFS.length) await sleep(1000);
  }
  return results.length > 0 ? results : null;
}

async function collectUsdKrwChart() {
  try {
    const data = await fetchJSON('https://query1.finance.yahoo.com/v8/finance/chart/KRW=X?interval=1d&range=3mo', 12000);
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
    const html = await fetchText(`https://finance.naver.com/item/board.naver?code=${stockCode}`, 12000);
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
    const targetPosts = recentPosts.length > 0 ? recentPosts : posts.slice(0, 20);
    // 상위 20개 본문 미리보기
    for (const p of targetPosts.slice(0, 20)) {
      try {
        const detailUrl = `https://gall.dcinside.com/mgallery/board/view?id=stockus&no=${p.postNo}`;
        const ctrl2 = new AbortController();
        const tid2 = setTimeout(() => ctrl2.abort(), 8000);
        const dRes = await fetch(detailUrl, { signal: ctrl2.signal, headers: { ...headers, 'Referer': listUrl } });
        clearTimeout(tid2);
        if (dRes.ok) {
          const dHtml = await dRes.text();
          const d$ = cheerio.load(dHtml);
          const body = d$('.write_div').text().trim().replace(/\s+/g, ' ');
          p.preview = body ? body.substring(0, 150) : null;
        }
      } catch { p.preview = null; }
      p.url = `https://gall.dcinside.com/mgallery/board/view?id=stockus&no=${p.postNo}`;
      delete p.postNo;
      await sleep(500);
    }
    // postNo 정리 + URL 생성
    const finalPosts = targetPosts.slice(0, 20);
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

async function collectSentiments() {
  const targets = [
    { name: '삼성전자', code: '005930' }, { name: 'SK하이닉스', code: '000660' },
    { name: '현대차', code: '005380' }, { name: 'KB금융', code: '105560' }, { name: '카카오', code: '035720' },
  ];
  const results = [];
  const allPosts = []; // 커뮤니티 글 목록 (콘텐츠 허브용)
  const stockPostMap = {}; // stock별 posts 매핑

  // 1단계: 모든 종목의 게시글 수집
  for (const t of targets) {
    try {
      const posts = await collectNaverDiscussion(t.code);
      if (posts && posts.length > 0) {
        stockPostMap[t.name] = posts;
        allPosts.push(...posts.slice(0, 10).map(p => ({ ...p, stock: t.name })));
      }
      await sleep(500);
    } catch (e) { /* skip */ }
  }

  // 1.5단계: DCInside 미국주식갤러리 수집
  try {
    const dcPosts = await collectDCInsideGallery();
    if (dcPosts.length > 0) {
      allPosts.push(...dcPosts.slice(0, 20));
      console.log(`[Collect] DC 게시글 ${dcPosts.length}건 추가`);
    }
  } catch (e) { console.warn('[Collect] DC 통합 실패:', e.message); }

  // 2단계: FinBert AI 배치 분석 시도
  const allTitles = allPosts.map(p => p.title);
  const finbertResults = await analyzeWithFinBert(allTitles);
  const useFinBert = finbertResults && finbertResults.length === allTitles.length;
  const sentimentModel = useFinBert ? 'hybrid' : 'keyword';
  console.log(`[Collect] 감성분석 모델: ${sentimentModel} (게시글 ${allTitles.length}건)`);

  // 3단계: 게시글에 감성 결과 부착 (하이브리드 보정 적용)
  let correctedCount = 0;
  if (useFinBert) {
    allPosts.forEach((p, i) => {
      p.sentiment = hybridSentiment(finbertResults[i], p.title);
      if (p.sentiment.corrected) correctedCount++;
    });
    console.log(`[Collect] 하이브리드 보정: ${correctedCount}/${allPosts.length}건 키워드 보정됨`);
  } else {
    allPosts.forEach(p => {
      const r = KoreanSentiment.analyze(p.title);
      let label = r.label;
      if (label === '매우 긍정') label = '긍정';
      if (label === '매우 부정') label = '부정';
      p.sentiment = { label, score: Math.abs(r.score) * 10, model: 'keyword', positiveWords: r.positiveWords, negativeWords: r.negativeWords };
    });
  }

  // 4단계: 종목별 감성 집계 (전체 게시글 기준)
  for (const t of targets) {
    const posts = stockPostMap[t.name];
    if (!posts || posts.length === 0) continue;

    // 전체 posts에 대해 하이브리드 분석 (배치에는 상위 5개만 있으므로 나머지는 키워드 분석)
    let pos = 0, neg = 0, neu = 0;
    if (useFinBert) {
      // 상위 5개는 하이브리드 결과 사용, 나머지는 키워드 fallback
      const topPosts = allPosts.filter(p => p.stock === t.name);
      posts.forEach(p => {
        const top = topPosts.find(tp => tp.title === p.title);
        if (top && top.sentiment) {
          if (top.sentiment.label === '긍정') pos++;
          else if (top.sentiment.label === '부정') neg++;
          else neu++;
        } else {
          const r = KoreanSentiment.analyze(p.title);
          if (r.score > 0) pos++; else if (r.score < 0) neg++; else neu++;
        }
      });
    } else {
      posts.forEach(p => {
        const r = KoreanSentiment.analyze(p.title);
        if (r.score > 0) pos++; else if (r.score < 0) neg++; else neu++;
      });
    }
    const total = posts.length || 1;
    results.push({ stock: t.name, positive: Math.round(pos / total * 100), neutral: Math.round(neu / total * 100), negative: Math.round(neg / total * 100), mentions: posts.length, live: true, sentimentModel });
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

  // Phase 2: 뉴스 감성분석 (FinBert AI)
  let newsSentiment = null;
  if (news && news.length > 0) {
    const newsTitles = news.map(n => n.title);
    const newsFinbert = await analyzeWithFinBert(newsTitles);
    if (newsFinbert && newsFinbert.length === newsTitles.length) {
      let pos = 0, neg = 0, neu = 0;
      let newsCorrected = 0;
      news.forEach((n, i) => {
        n.sentiment = hybridSentiment(newsFinbert[i], n.title);
        if (n.sentiment.corrected) newsCorrected++;
        if (n.sentiment.label === '긍정') pos++;
        else if (n.sentiment.label === '부정') neg++;
        else neu++;
      });
      const total = news.length || 1;
      newsSentiment = { positive: Math.round(pos / total * 100), neutral: Math.round(neu / total * 100), negative: Math.round(neg / total * 100), model: 'hybrid' };
      console.log(`[Collect] 뉴스 감성분석 (hybrid): 긍정${newsSentiment.positive}% 중립${newsSentiment.neutral}% 부정${newsSentiment.negative}% (보정 ${newsCorrected}건)`);
    } else {
      // 키워드 fallback
      let pos = 0, neg = 0, neu = 0;
      news.forEach(n => {
        const r = KoreanSentiment.analyze(n.title);
        let label = r.label;
        if (label === '매우 긍정') label = '긍정';
        if (label === '매우 부정') label = '부정';
        n.sentiment = { label, score: Math.abs(r.score) * 10, model: 'keyword' };
        if (r.score > 0) pos++; else if (r.score < 0) neg++; else neu++;
      });
      const total = news.length || 1;
      newsSentiment = { positive: Math.round(pos / total * 100), neutral: Math.round(neu / total * 100), negative: Math.round(neg / total * 100), model: 'keyword' };
    }
  }

  // Phase 2b: 뉴스 파생
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

  // Phase 4: 감성 분석 + 커뮤니티 글
  await sleep(2000);
  const sentimentResult = await collectSentiments();
  const sentiments = sentimentResult.sentiments;
  const newPosts = sentimentResult.communityPosts || [];

  // 커뮤니티 게시글 누적 (Firestore DB에서 기존 데이터 읽기 → 신규 병합 → 중복 제거 → 최대 500개)
  let communityPosts = newPosts;
  try {
    let prevPosts = [];
    // 1순위: Firestore DB에서 기존 게시글 읽기
    if (db) {
      const doc = await db.doc('current/latest').get();
      if (doc.exists && doc.data().communityPosts) {
        prevPosts = doc.data().communityPosts;
        console.log(`[Collect] Firestore에서 기존 커뮤니티 ${prevPosts.length}건 로드`);
      }
    }
    // 2순위 fallback: 로컬 파일
    if (prevPosts.length === 0) {
      const latestPath = path.join(__dirname, 'data', 'latest.json');
      if (fs.existsSync(latestPath)) {
        const prev = JSON.parse(fs.readFileSync(latestPath, 'utf-8'));
        prevPosts = prev.communityPosts || [];
        if (prevPosts.length > 0) console.log(`[Collect] 로컬 파일에서 기존 커뮤니티 ${prevPosts.length}건 로드`);
      }
    }
    if (prevPosts.length > 0 && newPosts.length > 0) {
      const seen = new Set();
      const merged = [];
      // 새 글 우선 추가
      for (const p of newPosts) {
        const key = `${p.title}||${p.source}`;
        if (!seen.has(key)) { seen.add(key); merged.push(p); }
      }
      // 기존 글 중 중복 아닌 것 추가
      for (const p of prevPosts) {
        const key = `${p.title}||${p.source}`;
        if (!seen.has(key)) { seen.add(key); merged.push(p); }
      }
      communityPosts = merged.slice(0, 500);
      console.log(`[Collect] 커뮤니티 누적: 신규 ${newPosts.length} + 기존 ${prevPosts.length} → 병합 ${merged.length} → 저장 ${communityPosts.length}건`);
    }
  } catch (e) { console.warn('[Collect] 커뮤니티 누적 병합 실패:', e.message); }

  // 결과 저장
  const data = {
    indices, exchangeRates, news, briefing, stocks, etfs, trends, calendar, youtube, sentiments, communityPosts, newsSentiment, usdKrwChart, baseRates, dxy,
    updatedAt: new Date().toISOString(),
  };

  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  fs.writeFileSync(path.join(dataDir, 'latest.json'), JSON.stringify(data));

  // Firestore에 저장 (CDN과 병행)
  await writeToFirestore(data);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`[Collect] 완료 (${elapsed}s) - 환율:${!!exchangeRates} 지수:${!!indices} 뉴스:${news?.length || 0} 주식:${stocks?.length || 0} ETF:${etfs?.length || 0} 감성:${sentiments?.length || 0}`);
}

main().catch(e => { console.error('[Collect] 치명적 오류:', e); process.exit(1); });
