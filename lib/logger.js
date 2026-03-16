/* ========================================
   간단한 구조화 로거
   console.log 래핑 - 타임스탬프 + 카테고리 자동 추가
   ======================================== */

const LOG_LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
const currentLevel = LOG_LEVELS[process.env.LOG_LEVEL || 'info'];

function timestamp() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

function formatArgs(args) {
  return args.map(a => typeof a === 'object' ? JSON.stringify(a) : a).join(' ');
}

const logger = {
  debug(category, ...args) {
    if (currentLevel <= LOG_LEVELS.debug) {
      console.log(`[${timestamp()}] [DEBUG] [${category}]`, ...args);
    }
  },
  info(category, ...args) {
    if (currentLevel <= LOG_LEVELS.info) {
      console.log(`[${timestamp()}] [INFO] [${category}]`, ...args);
    }
  },
  warn(category, ...args) {
    if (currentLevel <= LOG_LEVELS.warn) {
      console.warn(`[${timestamp()}] [WARN] [${category}]`, ...args);
    }
  },
  error(category, ...args) {
    if (currentLevel <= LOG_LEVELS.error) {
      console.error(`[${timestamp()}] [ERROR] [${category}]`, ...args);
    }
  },
  /** 수집 결과 요약 로그 */
  collectSummary(results) {
    const parts = Object.entries(results)
      .map(([key, val]) => {
        if (val === null || val === undefined) return `${key}:✗`;
        if (Array.isArray(val)) return `${key}:${val.length}`;
        return `${key}:✓`;
      });
    this.info('Collect', '수집 결과 -', parts.join(' '));
  }
};

module.exports = logger;
