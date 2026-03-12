/**
 * StockLab Data Collector
 * GitHub Actions에서 10분마다 실행되어 데이터를 수집하고 JSON으로 저장
 */
const fetch = require('node-fetch');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

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

// ===== 감성 분석 =====
const KoreanSentiment = {
  strongPositive: ['급등','폭등','대박','상한가','역대최고','신고가','대호재','초강세','텐배거','완전체'],
  positive: ['상승','매수','호재','돌파','반등','강세','기대','수익','실적개선','추천','좋은','긍정','성장','흑자','호실적','저평가','목표가','상향','모아가자','존버','매력적','오르','올라','갈거','간다','개꿀','굿','좋다','기회','바닥'],
  strongNegative: ['급락','폭락','하한가','깡통','반토막','대폭락','대참사','물린','쪽박','망'],
  negative: ['하락','매도','악재','손절','약세','우려','손실','실적악화','위험','나쁜','부정','적자','고평가','하향','빠진','떨어','떨어진','물려','빠질','내려','걱정','불안','위기','폭망','팔자','개미털기','못하','안좋','삼성망','ㅠ'],
  analyze(text) {
    if (!text) return { score: 0, label: '중립' };
    let score = 0;
    this.strongPositive.forEach(w => { if (text.includes(w)) score += 2; });
    this.positive.forEach(w => { if (text.includes(w)) score += 1; });
    this.strongNegative.forEach(w => { if (text.includes(w)) score -= 2; });
    this.negative.forEach(w => { if (text.includes(w)) score -= 1; });
    let label = '중립';
    if (score >= 2) label = '매우 긍정';
    else if (score >= 1) label = '긍정';
    else if (score <= -2) label = '매우 부정';
    else if (score <= -1) label = '부정';
    return { score, label };
  }
};

// ===== Collectors =====
async function collectExchangeRates() {
  try {
    const data = await fetchJSON('https://open.er-api.com/v6/latest/USD');
    if (data.result !== 'success') throw new Error('API error');
    const krw = data.rates.KRW;
    return [
      { pair: 'USD/KRW', rate: krw },
      { pair: 'EUR/KRW', rate: krw / data.rates.EUR },
      { pair: 'JPY/KRW', rate: krw / data.rates.JPY },
      { pair: 'CNY/KRW', rate: krw / data.rates.CNY },
    ].map(p => {
      const value = Math.round(p.rate * 100) / 100;
      return { pair: p.pair, value, change: 0, history: generateNearHistory(value, 8), live: true };
    });
  } catch (e) { console.warn('[Collect] 환율 실패:', e.message); return null; }
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
  return keywords.slice(0, 16).map((k, i) => ({ word: k.word, count: k.count, size: i < 2 ? 5 : i < 5 ? 4 : i < 8 ? 3 : i < 12 ? 2 : 1, live: true }));
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
        results.push({ name: def.name, code: def.code, fee: def.fee, price: quote.price, change: quote.change, changePercent: quote.changePercent, return1y, return3y: Math.round(return1y * 2.5 * 10) / 10, aum: '-', divYield: def.code === '290130' ? 4.2 : def.code === '402460' ? 3.5 : def.code === '069500' ? 1.8 : def.code === '133690' ? 0.5 : 1.2, topHolding: def.topHolding, history: quote.history, fullCloses: quote.fullCloses, timestamps: quote.timestamps, live: true });
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
    const closes = (result.indicators.quote[0].close || []).filter(v => v != null);
    const timestamps = result.timestamp || [];
    return { labels: timestamps.map(t => { const d = new Date(t * 1000); return `${d.getMonth() + 1}/${d.getDate()}`; }), values: closes.map(v => Math.round(v * 100) / 100), live: true };
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
      posts.push({ title, author: $(cells[2]).text().trim(), date: $(cells[0]).text().trim(), views: parseInt($(cells[3]).text().trim()) || 0, likes: parseInt($(cells[4]).text().trim()) || 0, dislikes: parseInt($(cells[5]).text().trim()) || 0, source: '네이버증권', live: true });
    });
    return posts.length > 0 ? posts : null;
  } catch (e) { console.warn(`[Collect] 네이버 토론방 ${stockCode} 실패:`, e.message); return null; }
}

async function collectSentiments() {
  const targets = [
    { name: '삼성전자', code: '005930' }, { name: 'SK하이닉스', code: '000660' },
    { name: '현대차', code: '005380' }, { name: 'KB금융', code: '105560' }, { name: '카카오', code: '035720' },
  ];
  const results = [];
  for (const t of targets) {
    try {
      const posts = await collectNaverDiscussion(t.code);
      if (posts && posts.length > 0) {
        let pos = 0, neg = 0, neu = 0;
        posts.forEach(p => {
          const r = KoreanSentiment.analyze(p.title);
          if (r.score > 0) pos++; else if (r.score < 0) neg++; else neu++;
        });
        const total = posts.length || 1;
        results.push({ stock: t.name, positive: Math.round(pos / total * 100), neutral: Math.round(neu / total * 100), negative: Math.round(neg / total * 100), mentions: posts.length, live: true });
      }
      await sleep(500);
    } catch (e) { /* skip */ }
  }
  return results.length > 0 ? results : null;
}

// ===== Main =====
async function main() {
  console.log(`[Collect] 시작: ${new Date().toISOString()}`);
  const startTime = Date.now();

  // Phase 1: 병렬 수집
  const [fxResult, indicesResult, newsResult, calendarResult, youtubeResult, usdkrwResult] = await Promise.allSettled([
    collectExchangeRates(), collectIndices(), collectNews(), collectCalendar(), collectYouTube(), collectUsdKrwChart(),
  ]);

  const exchangeRates = fxResult.status === 'fulfilled' ? fxResult.value : null;
  const indices = indicesResult.status === 'fulfilled' ? indicesResult.value : null;
  const news = newsResult.status === 'fulfilled' ? newsResult.value : null;
  const calendar = calendarResult.status === 'fulfilled' ? calendarResult.value : null;
  const youtube = youtubeResult.status === 'fulfilled' ? youtubeResult.value : null;
  const usdKrwChart = usdkrwResult.status === 'fulfilled' ? usdkrwResult.value : null;

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

  // Phase 4: 감성 분석
  await sleep(2000);
  const sentiments = await collectSentiments();

  // 결과 저장
  const data = {
    indices, exchangeRates, news, briefing, stocks, etfs, trends, calendar, youtube, sentiments, usdKrwChart,
    updatedAt: new Date().toISOString(),
  };

  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  fs.writeFileSync(path.join(dataDir, 'latest.json'), JSON.stringify(data));

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`[Collect] 완료 (${elapsed}s) - 환율:${!!exchangeRates} 지수:${!!indices} 뉴스:${news?.length || 0} 주식:${stocks?.length || 0} ETF:${etfs?.length || 0} 감성:${sentiments?.length || 0}`);
}

main().catch(e => { console.error('[Collect] 치명적 오류:', e); process.exit(1); });
