/**
 * StockLab Collectors 기본 테스트
 * 실행: node test/collectors.test.js
 *
 * 외부 API 의존적이므로 네트워크 연결 필요
 * 각 테스트는 독립 실행, 실패해도 다음 테스트 계속 진행
 */

const path = require('path');
const { fetchJSON, fetchText, sleep, timeAgo } = require('../lib/utils');
const {
  collectIndices, collectExchangeRates, collectStocks, collectNews,
  collectCalendar, collectYouTube, extractTrends, collectETFs,
  collectNaverDiscussion, collectUsdKrwChart, collectFearGreed,
  collectDXY, scrapeNaverFinancials, getYahooQuote,
  MAIN_STOCKS, ETF_DEFS,
} = require('../lib/collectors');
const logger = require('../lib/logger');

let passed = 0, failed = 0, skipped = 0;

async function test(name, fn, { timeout = 20000 } = {}) {
  process.stdout.write(`  ${name}... `);
  try {
    const result = await Promise.race([
      fn(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('TIMEOUT')), timeout)),
    ]);
    console.log('✓ PASS');
    passed++;
  } catch (e) {
    if (e.message === 'TIMEOUT') {
      console.log('⏳ SKIP (timeout)');
      skipped++;
    } else {
      console.log(`✗ FAIL: ${e.message}`);
      failed++;
    }
  }
}

function assert(condition, msg) {
  if (!condition) throw new Error(msg || 'Assertion failed');
}

async function main() {
  console.log('\n=== StockLab Collectors Test ===\n');

  // ── 유틸리티 테스트 ──
  console.log('[Utils]');

  await test('fetchJSON - 유효한 URL', async () => {
    const data = await fetchJSON('https://httpbin.org/json', 10000);
    assert(data && typeof data === 'object', 'JSON 응답이 아님');
  });

  await test('fetchText - 유효한 URL', async () => {
    const text = await fetchText('https://httpbin.org/html', 10000);
    assert(typeof text === 'string' && text.length > 0, '텍스트 응답이 아님');
  });

  await test('timeAgo - 정상 동작', async () => {
    assert(timeAgo(new Date()) === '방금 전', '방금 전 실패');
    assert(timeAgo(new Date(Date.now() - 3600000)).includes('시간 전'), '시간 전 실패');
    assert(timeAgo(new Date(Date.now() - 86400000 * 3)).includes('일 전'), '일 전 실패');
  });

  await test('sleep - 지연 동작', async () => {
    const start = Date.now();
    await sleep(100);
    assert(Date.now() - start >= 90, 'sleep이 너무 빨리 끝남');
  });

  // ── Config exports 테스트 ──
  console.log('\n[Config Exports]');

  await test('MAIN_STOCKS 배열', async () => {
    assert(Array.isArray(MAIN_STOCKS), 'MAIN_STOCKS는 배열이어야 함');
    assert(MAIN_STOCKS.length > 0, 'MAIN_STOCKS가 비어있음');
    assert(MAIN_STOCKS[0].code && MAIN_STOCKS[0].name, 'code/name 필드 필요');
  });

  await test('ETF_DEFS 배열', async () => {
    assert(Array.isArray(ETF_DEFS), 'ETF_DEFS는 배열이어야 함');
    assert(ETF_DEFS.length > 0, 'ETF_DEFS가 비어있음');
    assert(ETF_DEFS[0].code && ETF_DEFS[0].name, 'code/name 필드 필요');
  });

  // ── Collector 테스트 (외부 API) ──
  console.log('\n[Collectors - 네트워크 필요]');

  await test('collectIndices', async () => {
    const result = await collectIndices();
    assert(result !== null, '결과가 null');
    assert(result.kospi || result.kosdaq, 'kospi/kosdaq 중 하나는 있어야 함');
  });

  await test('collectExchangeRates', async () => {
    const dataDir = path.join(__dirname, '..', 'data');
    const result = await collectExchangeRates(dataDir);
    assert(result !== null, '결과가 null');
  });

  await test('collectNews', async () => {
    const result = await collectNews();
    assert(Array.isArray(result), '배열이어야 함');
    if (result.length > 0) {
      assert(result[0].title, 'title 필드 필요');
    }
  });

  await test('extractTrends', async () => {
    const mockNews = [
      { title: '삼성전자 반도체 실적 호조' },
      { title: 'SK하이닉스 AI 반도체 수출 증가' },
      { title: '코스피 상승 반도체 강세' },
    ];
    const result = extractTrends(mockNews);
    assert(Array.isArray(result), '배열이어야 함');
    assert(result.length > 0, '키워드 추출 실패');
    assert(result[0].word && result[0].count, 'word/count 필드 필요');
  });

  await test('collectNaverDiscussion - 삼성전자', async () => {
    const result = await collectNaverDiscussion('005930');
    assert(Array.isArray(result), '배열이어야 함');
    if (result.length > 0) {
      assert(result[0].title, 'title 필드 필요');
    }
  });

  await test('collectFearGreed', async () => {
    const result = await collectFearGreed();
    // null일 수 있음 (API 불안정)
    if (result) {
      assert(typeof result.value === 'number' || typeof result.score === 'number', 'value/score 필드 필요');
    }
  });

  // ── 로거 테스트 ──
  console.log('\n[Logger]');

  await test('logger 기본 동작', async () => {
    // 에러 없이 호출되는지 확인
    logger.info('Test', '테스트 메시지');
    logger.warn('Test', '경고 메시지');
    logger.debug('Test', '디버그 메시지');
    logger.collectSummary({ 환율: { usd: 1300 }, 뉴스: null, 주식: [1, 2, 3] });
  });

  // ── 결과 요약 ──
  console.log(`\n=== 결과: ${passed} passed, ${failed} failed, ${skipped} skipped ===\n`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(e => {
  console.error('테스트 실행 오류:', e);
  process.exit(1);
});
