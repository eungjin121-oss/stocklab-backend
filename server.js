const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const cheerio = require('cheerio');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;
const COLLECT_INTERVAL = 10 * 60 * 1000; // 10분

// ===== In-Memory Data Store =====
const store = {
  indices: null,
  exchangeRates: null,
  news: null,
  briefing: null,
  stocks: null,
  etfs: null,
  trends: null,
  calendar: null,
  youtube: null,
  sentiments: null,
  dividends: null,
  updatedAt: null,
  collecting: false,
};

// ===== 감성 분석 모듈 =====
const KoreanSentiment = {
  strongPositive: ['급등','폭등','대박','상한가','역대최고','신고가','대호재','초강세','텐배거','완전체'],
  positive: ['상승','매수','호재','돌파','반등','강세','기대','수익','실적개선','추천',
    '좋은','긍정','성장','흑자','호실적','저평가','목표가','상향','모아가자','존버',
    '매력적','오르','올라','갈거','간다','개꿀','굿','좋다','기회','바닥'],
  strongNegative: ['급락','폭락','하한가','깡통','반토막','대폭락','대참사','물린','쪽박','망'],
  negative: ['하락','매도','악재','손절','약세','우려','손실','실적악화','위험','나쁜',
    '부정','적자','고평가','하향','빠진','떨어','떨어진','물려','빠질','내려',
    '걱정','불안','위기','폭망','팔자','개미털기','못하','안좋','삼성망','ㅠ'],
  analyze(text) {
    if (!text) return { score: 0, label: '중립', positiveWords: [], negativeWords: [] };
    const found = { positive: [], negative: [] };
    let score = 0;
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
  },
  analyzeBatch(posts) {
    let totalPositive = 0, totalNegative = 0, totalNeutral = 0;
    const keywordCount = {};
    const analyzed = posts.map(p => {
      const result = this.analyze(p.title);
      if (result.score > 0) totalPositive++;
      else if (result.score < 0) totalNegative++;
      else totalNeutral++;
      [...result.positiveWords, ...result.negativeWords].forEach(w => {
        keywordCount[w] = (keywordCount[w] || 0) + 1;
      });
      return { ...p, sentiment: result };
    });
    const total = posts.length || 1;
    return {
      posts: analyzed,
      summary: {
        total: posts.length, positive: totalPositive, negative: totalNegative, neutral: totalNeutral,
        positivePercent: Math.round(totalPositive / total * 100),
        negativePercent: Math.round(totalNegative / total * 100),
        neutralPercent: Math.round(totalNeutral / total * 100),
      },
      topKeywords: Object.entries(keywordCount).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([word, count]) => ({ word, count })),
    };
  }
};

// ===== Fetch Helpers =====
const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

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

// ===== Data Collectors =====

// 1. 환율
async function collectExchangeRates() {
  try {
    const data = await fetchJSON('https://open.er-api.com/v6/latest/USD');
    if (data.result !== 'success') throw new Error('API error');
    const krw = data.rates.KRW;
    const pairDefs = [
      { pair: 'USD/KRW', rate: krw },
      { pair: 'EUR/KRW', rate: krw / data.rates.EUR },
      { pair: 'JPY/KRW', rate: krw / data.rates.JPY },
      { pair: 'CNY/KRW', rate: krw / data.rates.CNY },
    ];
    return pairDefs.map(p => {
      const value = Math.round(p.rate * 100) / 100;
      return { pair: p.pair, value, change: 0, history: [], live: true };
    });
  } catch (e) { console.warn('[Collect] 환율 실패:', e.message); return null; }
}

// 2. Yahoo Finance 개별 종목
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
    const dailyPrevClose = closes.length >= 2 ? closes[closes.length - 2] : price;
    const prevClose3mo = meta.chartPreviousClose || closes[0] || price;
    return {
      price: Math.round(price),
      prevClose: Math.round(dailyPrevClose),
      change: Math.round(price - dailyPrevClose),
      changePercent: Math.round((price - dailyPrevClose) / dailyPrevClose * 10000) / 100,
      change3mo: Math.round(price - prevClose3mo),
      changePercent3mo: Math.round((price - prevClose3mo) / prevClose3mo * 10000) / 100,
      history: closes.slice(-8).map(v => Math.round(v)),
      fullCloses: closes.map(v => Math.round(v)),
      timestamps: result.timestamp,
      live: true,
    };
  } catch (e) { console.warn(`[Collect] Yahoo ${symbol} 실패:`, e.message); return null; }
}

// 3. 시장 지수
async function collectIndices() {
  try {
    const [kospi, kosdaq] = await Promise.allSettled([
      getYahooQuote('^KS11'),
      getYahooQuote('^KQ11'),
    ]);
    return {
      kospi: kospi.status === 'fulfilled' ? kospi.value : null,
      kosdaq: kosdaq.status === 'fulfilled' ? kosdaq.value : null,
    };
  } catch (e) { console.warn('[Collect] 지수 실패:', e.message); return null; }
}

// 4. 주요 한국 주식 가격
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
  try {
    const results = [];
    for (let i = 0; i < MAIN_STOCKS.length; i += 3) {
      const batch = MAIN_STOCKS.slice(i, i + 3);
      const batchResults = await Promise.allSettled(
        batch.map(s => getYahooQuote(s.code + '.KS'))
      );
      for (let j = 0; j < batch.length; j++) {
        const def = batch[j];
        const quote = batchResults[j].status === 'fulfilled' ? batchResults[j].value : null;
        if (quote) {
          results.push({
            code: def.code, name: def.name, sector: def.sector, market: 'KOSPI',
            price: quote.price, change: quote.change, changePercent: quote.changePercent,
            history: quote.history, fullCloses: quote.fullCloses, timestamps: quote.timestamps,
            live: true,
          });
        }
      }
      if (i + 3 < MAIN_STOCKS.length) await sleep(1000); // rate limit
    }
    return results.length > 0 ? results : null;
  } catch (e) { console.warn('[Collect] 주식 실패:', e.message); return null; }
}

// 5. 뉴스 (Google News RSS)
async function collectNews(query) {
  query = query || '한국 증시 주식 경제';
  try {
    const rssUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=ko&gl=KR&ceid=KR:ko`;
    const xml = await fetchText(rssUrl, 10000);
    const $ = cheerio.load(xml, { xmlMode: true });
    const items = $('item');
    if (items.length === 0) throw new Error('No items');
    const articles = [];
    items.slice(0, 10).each((_, item) => {
      const rawTitle = $(item).find('title').text();
      const source = $(item).find('source').text();
      const pubDate = $(item).find('pubDate').text();
      const title = rawTitle.replace(/\s*-\s*[^-]+$/, '').trim() || rawTitle;
      articles.push({ title, source: source || '뉴스', time: timeAgo(new Date(pubDate)), live: true });
    });
    return articles.length > 0 ? articles : null;
  } catch (e) { console.warn('[Collect] 뉴스 실패:', e.message); return null; }
}

// 6. AI 브리핑
async function collectBriefing(news) {
  try {
    if (!news || news.length === 0) return null;
    const today = new Date();
    const dateStr = `${today.getFullYear()}년 ${today.getMonth() + 1}월 ${today.getDate()}일`;
    const dayNames = ['일', '월', '화', '수', '목', '금', '토'];
    const allText = news.map(n => n.title).join(' ');
    const candidates = ['삼성전자','SK하이닉스','현대차','FOMC','금리','환율','코스피','코스닥',
      '반도체','AI','배당','실적','인플레','ETF','2차전지','바이오','원달러','증시','무역','수출',
      'KB금융','NAVER','카카오','LG','POSCO','셀트리온','한은','연준','물가','고용','GDP'];
    const keywords = candidates.filter(k => allText.includes(k)).slice(0, 6);
    if (keywords.length < 3) keywords.push('증시', '경제', '투자');
    return {
      date: `${dateStr} (${dayNames[today.getDay()]})`,
      content: news.slice(0, 5).map(n => n.title + (n.source ? ` (${n.source})` : '')),
      keywords: [...new Set(keywords)].slice(0, 6),
      live: true,
    };
  } catch (e) { console.warn('[Collect] 브리핑 실패:', e.message); return null; }
}

// 7. 경제 캘린더
async function collectCalendar() {
  try {
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
    return allEvents.filter(e => {
      const key = e.title.substring(0, 20);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    }).slice(0, 8);
  } catch (e) { console.warn('[Collect] 캘린더 실패:', e.message); return null; }
}

// 8. 유튜브 콘텐츠
async function collectYouTube() {
  try {
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
          const title = rawTitle.replace(/\s*-\s*[^-]+$/, '').trim() || rawTitle;
          allItems.push({ title, channel: source || '투자 채널', time: timeAgo(new Date(pubDate)), live: true });
        });
      } catch (e) { /* skip */ }
    }
    const seen = new Set();
    return allItems.filter(i => {
      const key = i.title.substring(0, 25);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    }).slice(0, 8);
  } catch (e) { console.warn('[Collect] 유튜브 실패:', e.message); return null; }
}

// 9. 트렌드 키워드
function extractTrends(news) {
  if (!news || news.length === 0) return null;
  const allText = news.map(n => n.title).join(' ');
  const candidates = [
    '삼성전자','SK하이닉스','현대차','FOMC','금리','환율','코스피','코스닥',
    '반도체','AI','배당','실적','인플레','ETF','2차전지','바이오','원달러',
    'KB금융','NAVER','카카오','LG','POSCO','셀트리온','한은','연준',
    '물가','고용','GDP','수출','무역','증시','투자','테슬라','엔비디아',
    '트럼프','관세','원유','금값','비트코인','부동산','IPO','공모주',
    '삼성바이오','LG에너지','현대모비스','기아','네이버','삼성SDI'
  ];
  const keywords = [];
  candidates.forEach(word => {
    const regex = new RegExp(word, 'g');
    const matches = allText.match(regex);
    if (matches) keywords.push({ word, count: matches.length });
  });
  keywords.sort((a, b) => b.count - a.count);
  return keywords.slice(0, 16).map((k, i) => ({
    word: k.word, count: k.count,
    size: i < 2 ? 5 : i < 5 ? 4 : i < 8 ? 3 : i < 12 ? 2 : 1,
    live: true,
  }));
}

// 10. ETF 데이터
const ETF_DEFS = [
  { name: 'TIGER S&P500', code: '360750' },
  { name: 'KODEX 200', code: '069500' },
  { name: 'ACE 미국배당다우존스', code: '402460' },
  { name: 'TIGER 미국나스닥100', code: '133690' },
  { name: 'KODEX 배당가치', code: '290130' },
];

async function collectETFs() {
  try {
    const results = [];
    for (let i = 0; i < ETF_DEFS.length; i += 3) {
      const batch = ETF_DEFS.slice(i, i + 3);
      const batchResults = await Promise.allSettled(
        batch.map(e => getYahooQuote(e.code + '.KS'))
      );
      for (let j = 0; j < batch.length; j++) {
        const def = batch[j];
        const quote = batchResults[j].status === 'fulfilled' ? batchResults[j].value : null;
        if (quote) {
          const closes = quote.fullCloses || [];
          const cur = closes[closes.length - 1] || quote.price;
          const m1y = closes.length > 250 ? closes[closes.length - 251] : closes[0];
          const return1y = m1y ? Math.round((cur - m1y) / m1y * 1000) / 10 : 0;
          results.push({
            name: def.name, code: def.code,
            price: quote.price, change: quote.change, changePercent: quote.changePercent,
            return1y, history: quote.history,
            fullCloses: quote.fullCloses, timestamps: quote.timestamps, live: true,
          });
        }
      }
      if (i + 3 < ETF_DEFS.length) await sleep(1000);
    }
    return results.length > 0 ? results : null;
  } catch (e) { console.warn('[Collect] ETF 실패:', e.message); return null; }
}

// 11. 네이버 종목토론방 (감성 분석용)
async function collectNaverDiscussion(stockCode) {
  try {
    const url = `https://finance.naver.com/item/board.naver?code=${stockCode}`;
    const html = await fetchText(url, 12000);
    const $ = cheerio.load(html);
    const posts = [];
    $('table.type2 tr').each((_, row) => {
      const cells = $(row).find('td');
      if (cells.length < 6) return;
      const titleEl = $(cells[1]).find('a');
      if (!titleEl.length) return;
      const title = (titleEl.attr('title') || titleEl.text() || '').trim();
      if (!title) return;
      posts.push({
        title, author: $(cells[2]).text().trim(),
        date: $(cells[0]).text().trim(),
        views: parseInt($(cells[3]).text().trim()) || 0,
        likes: parseInt($(cells[4]).text().trim()) || 0,
        dislikes: parseInt($(cells[5]).text().trim()) || 0,
        source: '네이버증권', live: true,
      });
    });
    return posts.length > 0 ? posts : null;
  } catch (e) { console.warn(`[Collect] 네이버 토론방 ${stockCode} 실패:`, e.message); return null; }
}

// 12. 감성 분석
async function collectSentiments() {
  try {
    const targets = [
      { name: '삼성전자', code: '005930' },
      { name: 'SK하이닉스', code: '000660' },
      { name: '현대차', code: '005380' },
      { name: 'KB금융', code: '105560' },
      { name: '카카오', code: '035720' },
    ];
    const results = [];
    for (const t of targets) {
      try {
        const posts = await collectNaverDiscussion(t.code);
        if (posts && posts.length > 0) {
          const analyzed = KoreanSentiment.analyzeBatch(posts);
          results.push({
            stock: t.name,
            positive: analyzed.summary.positivePercent,
            neutral: analyzed.summary.neutralPercent,
            negative: analyzed.summary.negativePercent,
            mentions: analyzed.summary.total,
            live: true,
          });
        }
        await sleep(500);
      } catch (e) { /* skip */ }
    }
    return results.length > 0 ? results : null;
  } catch (e) { console.warn('[Collect] 감성 분석 실패:', e.message); return null; }
}

// 13. 네이버 펀더멘털
async function collectFundamentals(code) {
  try {
    const html = await fetchText(`https://finance.naver.com/item/main.naver?code=${code}`, 12000);
    const $ = cheerio.load(html);
    const result = { code, live: true };
    $('table').each((_, table) => {
      const text = $(table).text();
      if (text.includes('PER') && text.includes('PBR')) {
        $(table).find('tr').each((_, row) => {
          const th = $(row).find('th, td:first-child').first();
          const label = th.text().trim();
          if (label.includes('PER') && !label.includes('업종')) {
            const val = $(row).find('td em, td:nth-child(2)').first().text().trim();
            if (val) result.per = parseFloat(val.replace(/,/g, '')) || null;
          }
          if (label.includes('PBR')) {
            const val = $(row).find('td em, td:nth-child(2)').first().text().trim();
            if (val) result.pbr = parseFloat(val.replace(/,/g, '')) || null;
          }
          if (label.includes('배당수익률')) {
            const val = $(row).find('td em, td:nth-child(2)').first().text().trim();
            if (val) result.divYield = parseFloat(val.replace(/,/g, '')) || null;
          }
        });
      }
    });
    const capEl = $('#_market_sum');
    if (capEl.length) {
      const capNum = parseInt(capEl.text().replace(/[\s\n,]/g, '').trim());
      if (!isNaN(capNum)) {
        result.marketCap = capNum >= 10000 ? Math.round(capNum / 10000) + '조' : capNum.toLocaleString() + '억';
      }
    }
    return (result.per || result.pbr || result.divYield || result.marketCap) ? result : null;
  } catch (e) { console.warn(`[Collect] 펀더멘털 ${code} 실패:`, e.message); return null; }
}

// USD/KRW 차트
async function collectUsdKrwChart() {
  try {
    const url = 'https://query1.finance.yahoo.com/v8/finance/chart/KRW=X?interval=1d&range=3mo';
    const data = await fetchJSON(url, 12000);
    const result = data.chart.result[0];
    const closes = (result.indicators.quote[0].close || []).filter(v => v != null);
    const timestamps = result.timestamp || [];
    return {
      labels: timestamps.map(t => {
        const d = new Date(t * 1000);
        return `${d.getMonth() + 1}/${d.getDate()}`;
      }),
      values: closes.map(v => Math.round(v * 100) / 100),
      live: true,
    };
  } catch (e) { console.warn('[Collect] USD/KRW 차트 실패:', e.message); return null; }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ===== Main Collection =====
async function collectAll() {
  if (store.collecting) {
    console.log('[Collect] 이미 수집 중... 건너뜀');
    return;
  }
  store.collecting = true;
  const startTime = Date.now();
  console.log(`[Collect] 데이터 수집 시작: ${new Date().toISOString()}`);

  try {
    // Phase 1: 병렬 수집 (독립적인 것들)
    const [fxResult, indicesResult, newsResult, calendarResult, youtubeResult, usdkrwResult] = await Promise.allSettled([
      collectExchangeRates(),
      collectIndices(),
      collectNews(),
      collectCalendar(),
      collectYouTube(),
      collectUsdKrwChart(),
    ]);

    if (fxResult.status === 'fulfilled' && fxResult.value) store.exchangeRates = fxResult.value;
    if (indicesResult.status === 'fulfilled' && indicesResult.value) store.indices = indicesResult.value;
    const news = newsResult.status === 'fulfilled' ? newsResult.value : null;
    if (news) store.news = news;
    if (calendarResult.status === 'fulfilled' && calendarResult.value) store.calendar = calendarResult.value;
    if (youtubeResult.status === 'fulfilled' && youtubeResult.value) store.youtube = youtubeResult.value;
    if (usdkrwResult.status === 'fulfilled' && usdkrwResult.value) store.usdKrwChart = usdkrwResult.value;

    // Phase 2: 뉴스 기반 파생 데이터
    if (news) {
      store.briefing = await collectBriefing(news);
      store.trends = extractTrends(news);
    }

    // Phase 3: 주식 + ETF (rate limit 주의)
    const stocksResult = await collectStocks();
    if (stocksResult) store.stocks = stocksResult;

    await sleep(2000);

    const etfsResult = await collectETFs();
    if (etfsResult) store.etfs = etfsResult;

    // Phase 4: 감성 분석 (네이버 스크래핑, rate limit 주의)
    await sleep(2000);
    const sentiments = await collectSentiments();
    if (sentiments) store.sentiments = sentiments;

    store.updatedAt = new Date().toISOString();
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Collect] 수집 완료 (${elapsed}s) - 환율:${!!store.exchangeRates} 지수:${!!store.indices} 뉴스:${!!store.news} 주식:${store.stocks?.length || 0} ETF:${store.etfs?.length || 0} 감성:${store.sentiments?.length || 0}`);
  } catch (e) {
    console.error('[Collect] 수집 중 오류:', e.message);
  } finally {
    store.collecting = false;
  }
}

// ===== API Routes =====
app.get('/health', (req, res) => {
  res.json({ status: 'ok', updatedAt: store.updatedAt, collecting: store.collecting });
});

// 전체 데이터 (대시보드용)
app.get('/api/all', (req, res) => {
  res.json({
    indices: store.indices,
    exchangeRates: store.exchangeRates,
    news: store.news,
    briefing: store.briefing,
    stocks: store.stocks,
    etfs: store.etfs,
    trends: store.trends,
    calendar: store.calendar,
    youtube: store.youtube,
    sentiments: store.sentiments,
    usdKrwChart: store.usdKrwChart,
    updatedAt: store.updatedAt,
  });
});

app.get('/api/indices', (req, res) => res.json({ data: store.indices, updatedAt: store.updatedAt }));
app.get('/api/exchange-rates', (req, res) => res.json({ data: store.exchangeRates, updatedAt: store.updatedAt }));
app.get('/api/news', (req, res) => res.json({ data: store.news, updatedAt: store.updatedAt }));
app.get('/api/briefing', (req, res) => res.json({ data: store.briefing, updatedAt: store.updatedAt }));
app.get('/api/stocks', (req, res) => res.json({ data: store.stocks, updatedAt: store.updatedAt }));
app.get('/api/etfs', (req, res) => res.json({ data: store.etfs, updatedAt: store.updatedAt }));
app.get('/api/trends', (req, res) => res.json({ data: store.trends, updatedAt: store.updatedAt }));
app.get('/api/calendar', (req, res) => res.json({ data: store.calendar, updatedAt: store.updatedAt }));
app.get('/api/youtube', (req, res) => res.json({ data: store.youtube, updatedAt: store.updatedAt }));
app.get('/api/sentiments', (req, res) => res.json({ data: store.sentiments, updatedAt: store.updatedAt }));
app.get('/api/usdkrw-chart', (req, res) => res.json({ data: store.usdKrwChart, updatedAt: store.updatedAt }));

// 온디맨드: 개별 종목 차트 (캐시 없으면 실시간 fetch)
app.get('/api/stock/:code/chart', async (req, res) => {
  const { code } = req.params;
  const range = req.query.range || '3mo';
  try {
    const symbol = code + '.KS';
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1d&range=${range}`;
    const data = await fetchJSON(url, 15000);
    const result = data.chart.result[0];
    const closes = (result.indicators.quote[0].close || []).filter(v => v != null);
    const timestamps = result.timestamp || [];
    res.json({
      data: {
        labels: timestamps.map(t => { const d = new Date(t * 1000); return `${d.getMonth() + 1}/${d.getDate()}`; }),
        values: closes.map(v => Math.round(v)),
        live: true,
      }
    });
  } catch (e) { res.json({ data: null, error: e.message }); }
});

// 온디맨드: 개별 종목 펀더멘털
app.get('/api/stock/:code/fundamentals', async (req, res) => {
  try {
    const data = await collectFundamentals(req.params.code);
    res.json({ data });
  } catch (e) { res.json({ data: null, error: e.message }); }
});

// 온디맨드: 커뮤니티 글
app.get('/api/community/:code', async (req, res) => {
  try {
    const posts = await collectNaverDiscussion(req.params.code);
    res.json({ data: posts });
  } catch (e) { res.json({ data: null, error: e.message }); }
});

// 수동 수집 트리거
app.post('/api/collect', async (req, res) => {
  if (store.collecting) return res.json({ status: 'already collecting' });
  collectAll(); // 비동기로 실행
  res.json({ status: 'collection started' });
});

// ===== Start =====
app.listen(PORT, () => {
  console.log(`StockLab Backend running on port ${PORT}`);
  // 시작 시 즉시 수집
  collectAll();
  // 10분마다 수집
  setInterval(collectAll, COLLECT_INTERVAL);
});
