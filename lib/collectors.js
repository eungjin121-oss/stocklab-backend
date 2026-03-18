/* ========================================
   공통 데이터 수집기 (collect.js, server.js 공유)
   ======================================== */

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const { fetchJSON, fetchText, sleep, timeAgo, BROWSER_UA } = require('./utils');
const MAIN_STOCKS = require('../shared/stocks-config.json');

// ===== Yahoo Finance =====
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

async function collectDXY() {
  try {
    const quote = await getYahooQuote('DX=F');
    if (!quote) return null;
    return { ...quote, name: 'US Dollar Index', symbol: 'DX=F' };
  } catch (e) { console.warn('[Collect] DXY 실패:', e.message); return null; }
}

// ===== 환율 =====
async function collectExchangeRates(dataDir) {
  try {
    const data = await fetchJSON('https://open.er-api.com/v6/latest/USD');
    if (data.result !== 'success') throw new Error('API error');
    const krw = data.rates.KRW;

    let prevRates = {};
    try {
      const latestPath = dataDir
        ? path.join(dataDir, 'latest.json')
        : path.join(__dirname, '..', 'data', 'latest.json');
      const prevData = JSON.parse(fs.readFileSync(latestPath, 'utf-8'));
      if (prevData.exchangeRates) {
        for (const r of prevData.exchangeRates) {
          prevRates[r.pair] = { value: r.value, history: r.history || [] };
        }
      }
    } catch (_) {}

    const pairDefs = [
      { pair: 'USD/KRW', rate: krw, group: '주요' },
      { pair: 'EUR/KRW', rate: krw / data.rates.EUR, group: '주요' },
      { pair: 'JPY/KRW', rate: krw / data.rates.JPY, group: '아시아' },
      { pair: 'CNY/KRW', rate: krw / data.rates.CNY, group: '아시아' },
      { pair: 'GBP/KRW', rate: krw / data.rates.GBP, group: '주요' },
      { pair: 'AUD/KRW', rate: krw / data.rates.AUD, group: '주요' },
      { pair: 'SGD/KRW', rate: krw / data.rates.SGD, group: '아시아' },
      { pair: 'THB/KRW', rate: krw / data.rates.THB, group: '아시아' },
    ];
    return pairDefs.map(p => {
      const value = Math.round(p.rate * 100) / 100;
      const prev = prevRates[p.pair];
      const prevValue = prev?.value || value;
      const change = Math.round((value - prevValue) * 100) / 100;
      let history = prev?.history ? [...prev.history] : [];
      if (history.length === 0 || history[history.length - 1] !== value) {
        history.push(value);
      }
      if (history.length > 8) history = history.slice(-8);
      return { pair: p.pair, value, change, history, live: true, group: p.group };
    });
  } catch (e) { console.warn('[Collect] 환율 실패:', e.message); return null; }
}

// ===== 주식 =====
async function collectStocks(opts = {}) {
  const { withFinancials = false, fetchTimeout = 15000 } = opts;
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

  // 네이버 금융 재무지표 (collect.js에서만 사용, withFinancials=true)
  if (withFinancials && results.length > 0) {
    console.log('[Collect] 네이버 금융 재무지표 수집 시작...');
    for (let i = 0; i < results.length; i += 3) {
      const batch = results.slice(i, i + 3);
      const fundResults = await Promise.allSettled(batch.map(s => scrapeNaverFinancials(s.code, fetchTimeout)));
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

async function scrapeNaverFinancials(code, timeout = 15000) {
  try {
    const html = await fetchText(`https://finance.naver.com/item/main.naver?code=${code}`, timeout);
    const $ = cheerio.load(html);
    const result = {};

    const marketCapEl = $('#_market_sum, #_totalMarketValue');
    if (marketCapEl.length) {
      const numMatch = marketCapEl.text().replace(/[^\d,]/g, '').replace(/,/g, '');
      if (numMatch) {
        const num = parseInt(numMatch);
        if (num >= 10000) {
          const jo = num / 10000;
          result.marketCap = jo >= 10 ? Math.round(jo) + '조' : jo.toFixed(1).replace(/\.0$/, '') + '조';
        } else result.marketCap = num.toLocaleString() + '억';
      }
    }

    $('table').each((_, table) => {
      $(table).find('tr').each((_, tr) => {
        const text = $(tr).text().replace(/\s+/g, ' ');
        if (text.includes('PER') && !result.per) {
          const m = text.match(/PER\s*[\(（].*?[\)）]?\s*([\d,.]+)/);
          if (m) result.per = parseFloat(m[1].replace(/,/g, ''));
        }
        if (text.includes('PBR') && !result.pbr) {
          const m = text.match(/PBR\s*([\d,.]+)/);
          if (m) result.pbr = parseFloat(m[1].replace(/,/g, ''));
        }
        if (text.includes('배당수익률') && !result.divYield) {
          const m = text.match(/배당수익률\s*([\d,.]+)/);
          if (m) result.divYield = parseFloat(m[1].replace(/,/g, ''));
        }
      });
    });

    if (!result.per || !result.pbr) {
      $('em').each((_, el) => {
        const id = $(el).attr('id') || '';
        const val = $(el).text().replace(/,/g, '').trim();
        if (id === '_per' && !result.per) result.per = parseFloat(val) || null;
        if (id === '_pbr' && !result.pbr) result.pbr = parseFloat(val) || null;
        if (id === '_dvr' && !result.divYield) result.divYield = parseFloat(val) || null;
      });
    }

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

    return (result.per || result.pbr || result.divYield || result.marketCap) ? result : null;
  } catch (e) { console.warn(`[Collect] 네이버 재무 ${code} 실패:`, e.message); return null; }
}

// ===== 뉴스 =====
async function collectNews(query) {
  query = query || '한국 증시 주식 경제';
  try {
    const rssUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}+when:3d&hl=ko&gl=KR&ceid=KR:ko`;
    const xml = await fetchText(rssUrl, 10000);
    const $ = cheerio.load(xml, { xmlMode: true });
    const articles = [];
    $('item').slice(0, 10).each((_, item) => {
      const rawTitle = $(item).find('title').text();
      const source = $(item).find('source').text();
      const pubDate = $(item).find('pubDate').text();
      const title = rawTitle.replace(/\s*-\s*[^-]+$/, '').trim() || rawTitle;
      const pd = new Date(pubDate);
      articles.push({ title, source: source || '뉴스', time: timeAgo(pd), pubDate: isNaN(pd.getTime()) ? '' : pd.toISOString(), live: true });
    });
    return articles.length > 0 ? articles : null;
  } catch (e) { console.warn('[Collect] 뉴스 실패:', e.message); return null; }
}

// ===== 경제 캘린더 =====
async function collectCalendar() {
  const queries = [
    '한국 경제 금리 환율 물가',
    'FOMC 금통위 기준금리 통화정책',
    '고용 실업률 CPI GDP 수출',
    '코스피 코스닥 증시 주식시장',
  ];
  const allEvents = [];
  for (const query of queries) {
    try {
      const rssUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}+when:3d&hl=ko&gl=KR&ceid=KR:ko`;
      const xml = await fetchText(rssUrl, 10000);
      const $ = cheerio.load(xml, { xmlMode: true });
      $('item').slice(0, 8).each((_, item) => {
        const title = ($(item).find('title').text() || '').replace(/\s*-\s*[^-]+$/, '').trim();
        const pubDate = $(item).find('pubDate').text();
        const source = $(item).find('source').text();
        if (title) {
          const d = new Date(pubDate);
          const dateStr = isNaN(d.getTime()) ? '' : d.toISOString();
          let importance = 'low';
          if (/FOMC|금통위|금리|기준금리|통화정책|한은|연준|BOK|Fed/.test(title)) importance = 'high';
          else if (/CPI|고용|실업|GDP|수출|무역|물가|인플레|소비자물가|PMI|경상수지/.test(title)) importance = 'medium';
          let category = '기타';
          if (/금리|FOMC|금통위|통화정책|한은|연준|BOK|Fed|기준금리/.test(title)) category = '금리';
          else if (/환율|원달러|달러|원화|외환|DXY|엔화|위안/.test(title)) category = '환율';
          else if (/고용|실업|일자리|취업|노동/.test(title)) category = '고용';
          else if (/물가|CPI|인플레|소비자물가|생산자물가|PPI/.test(title)) category = '물가';
          else if (/코스피|코스닥|증시|주가|주식|상장|KOSPI|KOSDAQ|나스닥|S&P|다우/.test(title)) category = '증시';
          allEvents.push({ date: dateStr, title: title.substring(0, 60), importance, category, source, live: true });
        }
      });
    } catch (e) { /* skip */ }
  }
  const seen = new Set();
  const unique = allEvents.filter(e => { const k = e.title.substring(0, 25); if (seen.has(k)) return false; seen.add(k); return true; });
  unique.sort((a, b) => (b.date || '').localeCompare(a.date || ''));
  return unique.slice(0, 15);
}

// ===== 유튜브/콘텐츠 =====
async function collectYouTube() {
  const queries = ['주식 투자 전망', '증시 분석 전문가'];
  const allItems = [];
  for (const query of queries) {
    try {
      const rssUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}+when:7d&hl=ko&gl=KR&ceid=KR:ko`;
      const xml = await fetchText(rssUrl, 10000);
      const $ = cheerio.load(xml, { xmlMode: true });
      $('item').slice(0, 5).each((_, item) => {
        const rawTitle = $(item).find('title').text();
        const source = $(item).find('source').text();
        const pubDate = $(item).find('pubDate').text();
        const pd = new Date(pubDate);
        allItems.push({ title: rawTitle.replace(/\s*-\s*[^-]+$/, '').trim() || rawTitle, channel: source || '투자 채널', time: timeAgo(pd), pubDate: isNaN(pd.getTime()) ? '' : pd.toISOString(), live: true });
      });
    } catch (e) { /* skip */ }
  }
  const seen = new Set();
  return allItems.filter(i => { const k = i.title.substring(0, 25); if (seen.has(k)) return false; seen.add(k); return true; }).slice(0, 8);
}

// ===== 트렌드 =====
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

// ===== ETF =====
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

// ===== 네이버 종목토론방 =====
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
      const href = titleEl.attr('href') || '';
      const url = href ? `https://finance.naver.com${href}` : '';
      posts.push({
        title, url, author: $(cells[2]).text().trim(),
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

// ===== USD/KRW 차트 =====
async function collectUsdKrwChart(timeout = 12000) {
  try {
    const data = await fetchJSON('https://query1.finance.yahoo.com/v8/finance/chart/KRW=X?interval=1d&range=3mo', timeout);
    const result = data.chart.result[0];
    const rawCloses = result.indicators.quote[0].close || [];
    const rawTimestamps = result.timestamp || [];
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

// ===== Fear & Greed =====
async function collectFearGreed() {
  try {
    const response = await fetch('https://production.dataviz.cnn.io/index/fearandgreed/graphdata', {
      headers: {
        'User-Agent': BROWSER_UA,
        'Accept': 'application/json',
        'Referer': 'https://edition.cnn.com/',
      },
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    const fg = data.fear_and_greed;
    if (!fg || fg.score == null) throw new Error('데이터 없음');

    // 히스토리 데이터 (최근 30일)
    let history = [];
    if (data.fear_and_greed_historical && data.fear_and_greed_historical.data) {
      const thirtyDaysAgo = Date.now() - 30 * 24 * 60 * 60 * 1000;
      history = data.fear_and_greed_historical.data
        .filter(d => d.x >= thirtyDaysAgo)
        .map(d => ({
          date: new Date(d.x).toISOString().slice(0, 10),
          score: Math.round(d.y * 10) / 10,
          rating: d.rating,
        }));
      // 날짜 중복 제거 (같은 날 마지막 값만)
      const byDate = {};
      history.forEach(h => { byDate[h.date] = h; });
      history = Object.values(byDate).sort((a, b) => a.date.localeCompare(b.date));
    }

    return {
      score: Math.round(fg.score * 10) / 10,
      rating: fg.rating,
      previousClose: Math.round((fg.previous_close || 0) * 10) / 10,
      oneWeekAgo: Math.round((fg.previous_1_week || 0) * 10) / 10,
      oneMonthAgo: Math.round((fg.previous_1_month || 0) * 10) / 10,
      oneYearAgo: Math.round((fg.previous_1_year || 0) * 10) / 10,
      timestamp: fg.timestamp,
      history,
      live: true,
    };
  } catch (e) { console.warn(`[Collect] Fear & Greed 실패: ${e.message}`); return null; }
}

module.exports = {
  getYahooQuote,
  collectIndices,
  collectDXY,
  collectExchangeRates,
  collectStocks,
  scrapeNaverFinancials,
  collectNews,
  collectCalendar,
  collectYouTube,
  extractTrends,
  collectETFs,
  collectNaverDiscussion,
  collectUsdKrwChart,
  collectFearGreed,
  ETF_DEFS,
  MAIN_STOCKS,
};
