// dotenv: .env 파일에서 환경 변수 로드 (없으면 무시)
try { require('dotenv').config(); } catch (_) { /* dotenv 미설치 시 무시 */ }

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

// 공유 모듈
const { fetchJSON, fetchText, sleep } = require('./lib/utils');
const {
  collectIndices, collectExchangeRates, collectStocks, collectNews,
  collectCalendar, collectYouTube, extractTrends, collectETFs,
  collectNaverDiscussion, collectUsdKrwChart, scrapeNaverFinancials,
  getYahooQuote,
} = require('./lib/collectors');
const log = require('./lib/logger');

// 감성 분석 (shared/sentiment-keywords.json 사용)
const _sentimentKw = require('./shared/sentiment-keywords.json');
const KoreanSentiment = {
  ..._sentimentKw,
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

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;
const COLLECT_INTERVAL = 10 * 60 * 1000; // 10분

// ===== In-Memory Data Store =====
const store = {
  indices: null, exchangeRates: null, news: null, briefing: null,
  stocks: null, etfs: null, trends: null, calendar: null,
  youtube: null, sentiments: null, usdKrwChart: null,
  updatedAt: null, collecting: false,
};

// ===== Server-specific Collectors =====

// 간단 브리핑 (server.js 전용 — collect.js는 OpenAI GPT 사용)
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
  } catch (e) { log.warn('Briefing', '브리핑 실패:', e.message); return null; }
}

// 감성 분석 수집
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
  } catch (e) { log.warn('Sentiment', '감성 분석 실패:', e.message); return null; }
}

// ===== Main Collection =====
async function collectAll() {
  if (store.collecting) {
    log.info('Collect', '이미 수집 중... 건너뜀');
    return;
  }
  store.collecting = true;
  const startTime = Date.now();
  log.info('Collect', '데이터 수집 시작');

  try {
    // Phase 1: 병렬 수집
    const [fxResult, indicesResult, newsResult, calendarResult, youtubeResult, usdkrwResult] = await Promise.allSettled([
      collectExchangeRates(path.join(__dirname, 'data')),
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

    // Phase 3: 주식 + ETF
    const stocksResult = await collectStocks();
    if (stocksResult) store.stocks = stocksResult;
    await sleep(2000);
    const etfsResult = await collectETFs();
    if (etfsResult) store.etfs = etfsResult;

    // Phase 4: 감성 분석
    await sleep(2000);
    const sentiments = await collectSentiments();
    if (sentiments) store.sentiments = sentiments;

    store.updatedAt = new Date().toISOString();
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    log.info('Collect', `수집 완료 (${elapsed}s)`);
    log.collectSummary({
      환율: store.exchangeRates, 지수: store.indices, 뉴스: store.news,
      주식: store.stocks, ETF: store.etfs, 감성: store.sentiments,
    });
  } catch (e) {
    log.error('Collect', '수집 중 오류:', e.message);
  } finally {
    store.collecting = false;
  }
}

// ===== API Routes =====
app.get('/health', (req, res) => {
  res.json({ status: 'ok', updatedAt: store.updatedAt, collecting: store.collecting });
});

app.get('/api/all', (req, res) => {
  res.json({
    indices: store.indices, exchangeRates: store.exchangeRates,
    news: store.news, briefing: store.briefing, stocks: store.stocks,
    etfs: store.etfs, trends: store.trends, calendar: store.calendar,
    youtube: store.youtube, sentiments: store.sentiments,
    usdKrwChart: store.usdKrwChart, updatedAt: store.updatedAt,
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

// 온디맨드: 개별 종목 차트
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
    const data = await scrapeNaverFinancials(req.params.code);
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

// ===== AI 프록시 엔드포인트 =====
const AI_RATE_LIMITS = {
  stock_report: 3,
  news_summary: 10,
  trade_pattern: 1,
  portfolio: 3,
};
const aiUsage = {}; // { 'IP_feature_YYYY-MM-DD': count }

function checkAILimit(ip, feature) {
  const limit = AI_RATE_LIMITS[feature];
  if (!limit) return { ok: true };
  const key = `${ip}_${feature}_${new Date().toISOString().slice(0, 10)}`;
  const used = aiUsage[key] || 0;
  if (used >= limit) return { ok: false, used, max: limit };
  return { ok: true, used, max: limit };
}

function incrementAIUsage(ip, feature) {
  const key = `${ip}_${feature}_${new Date().toISOString().slice(0, 10)}`;
  aiUsage[key] = (aiUsage[key] || 0) + 1;
}

// 오래된 사용량 기록 정리 (자정마다)
setInterval(() => {
  const today = new Date().toISOString().slice(0, 10);
  for (const key of Object.keys(aiUsage)) {
    if (!key.endsWith(today)) delete aiUsage[key];
  }
}, 60 * 60 * 1000);

app.post('/api/ai/chat', async (req, res) => {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    return res.status(503).json({ error: 'AI 서비스가 설정되지 않았습니다.' });
  }

  const { system, prompt, maxTokens, temperature, feature } = req.body;
  if (!system || !prompt) {
    return res.status(400).json({ error: 'system과 prompt는 필수입니다.' });
  }

  // Rate limit 체크
  const ip = req.headers['x-forwarded-for'] || req.ip || 'unknown';
  if (feature) {
    const limit = checkAILimit(ip, feature);
    if (!limit.ok) {
      return res.status(429).json({ error: `일일 한도 초과 (${limit.used}/${limit.max})`, used: limit.used, max: limit.max });
    }
  }

  try {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: 'gpt-4.1-nano',
        messages: [
          { role: 'system', content: system },
          { role: 'user', content: prompt },
        ],
        max_tokens: Math.min(maxTokens || 1500, 3000),
        temperature: temperature ?? 0.4,
      }),
    });

    if (!response.ok) {
      const errData = await response.json().catch(() => ({}));
      log.error('AI', `OpenAI 오류 (${response.status}):`, errData.error?.message || 'unknown');
      return res.status(response.status).json({ error: errData.error?.message || `OpenAI 오류 (${response.status})` });
    }

    const data = await response.json();
    if (feature) incrementAIUsage(ip, feature);
    log.info('AI', `${feature || 'chat'} 완료 (${data.usage?.total_tokens || 0} tokens)`);

    res.json({
      content: data.choices[0].message.content,
      tokens: data.usage?.total_tokens || 0,
    });
  } catch (e) {
    log.error('AI', '프록시 오류:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 수동 수집 트리거
app.post('/api/collect', async (req, res) => {
  if (store.collecting) return res.json({ status: 'already collecting' });
  collectAll();
  res.json({ status: 'collection started' });
});

// ===== Start =====
app.listen(PORT, () => {
  log.info('Server', `StockLab Backend running on port ${PORT}`);
  collectAll();
  setInterval(collectAll, COLLECT_INTERVAL);
});
