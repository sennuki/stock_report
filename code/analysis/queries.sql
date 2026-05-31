-- 横断分析クエリライブラリ
-- query.py が `-- name: <id>` で区切ってパースする。
-- 実行例: python code/analysis/query.py --named valuation_cheap
--
-- テーブル:
--   stocks       : 1 銘柄 = 1 行（最新スナップショット）
--   transcripts  : 1 四半期 = 1 行（決算トランスクリプト）
-- 比率系（margin, growth, yield, roe...）は小数（0.15 = 15%）で格納。


-- name: schema
-- 利用可能な列を確認する。
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_name IN ('stocks', 'transcripts', 'theme_metrics')
ORDER BY table_name, ordinal_position;


-- name: coverage
-- データの充足状況（主要指標の非 NULL 件数）。
SELECT
  count(*)                                   AS symbols,
  count(pe_forward)                          AS has_pe_fwd,
  count(revenue_growth)                      AS has_rev_growth,
  count(roe)                                 AS has_roe,
  count(target_upside)                       AS has_target_upside,
  count(latest_period)                       AS has_transcript
FROM stocks;


-- name: valuation_cheap
-- バリュエーション割安候補: 予想 PER が低く、黒字（forward EPS > 0）で
-- 一定の成長が見込めるもの。PER 昇順。
SELECT symbol, security_ja, sector, pe_forward, pe_ttm,
       revenue_growth, earnings_growth, dividend_yield, target_upside
FROM stocks
WHERE pe_forward IS NOT NULL AND pe_forward BETWEEN 0 AND 18
  AND eps_forward > 0
  AND coalesce(earnings_growth, 0) > 0
ORDER BY pe_forward ASC;


-- name: dcf_undervalued
-- DCF 公正価値に対して株価が割安（上振れ余地が大きい）銘柄。
-- ※ dcf_fair_value / dcf_upside が取得できている銘柄のみ。
SELECT symbol, security_ja, sector, current_price, dcf_fair_value, dcf_upside, pe_forward
FROM stocks
WHERE dcf_upside IS NOT NULL
ORDER BY dcf_upside DESC;


-- name: growth_quality
-- 成長性 × 収益性の両立（高成長かつ高 ROE・高利益率）。
SELECT symbol, security_ja, sector,
       revenue_growth, earnings_growth, roe, operating_margin, profit_margin, pe_forward
FROM stocks
WHERE revenue_growth >= 0.10
  AND roe >= 0.15
  AND operating_margin >= 0.15
ORDER BY revenue_growth DESC;


-- name: high_quality_value
-- クオリティ × バリュー（高 ROE・低負債なのに予想 PER が控えめ）。
SELECT symbol, security_ja, sector, roe, debt_to_equity, operating_margin,
       pe_forward, target_upside
FROM stocks
WHERE roe >= 0.15
  AND (debt_to_equity IS NULL OR debt_to_equity <= 100)  -- yfinance は % 表記
  AND pe_forward BETWEEN 0 AND 20
ORDER BY roe DESC;


-- name: dividend_income
-- インカム狙い: 配当利回りが高く、配当性向が過大でない（持続可能）銘柄。
SELECT symbol, security_ja, sector, dividend_yield, payout_ratio,
       earnings_growth, debt_to_equity
FROM stocks
WHERE dividend_yield >= 0.03
  AND (payout_ratio IS NULL OR payout_ratio <= 0.8)
ORDER BY dividend_yield DESC;


-- name: analyst_upside
-- アナリスト目標株価に対する上振れ余地が大きく、カバー人数も十分な銘柄。
SELECT symbol, security_ja, sector, current_price, target_mean, target_upside,
       num_analysts, rec_strong_buy, rec_buy
FROM stocks
WHERE target_upside IS NOT NULL
  AND num_analysts >= 5
ORDER BY target_upside DESC;


-- name: sector_valuation_summary
-- セクター横断: バリュエーション・成長性・収益性の中央値サマリ。
SELECT sector,
       count(*)                          AS n,
       median(pe_forward)                AS pe_fwd_median,
       median(revenue_growth)            AS rev_growth_median,
       median(roe)                       AS roe_median,
       median(operating_margin)          AS op_margin_median,
       median(dividend_yield)            AS div_yield_median
FROM stocks
WHERE sector IS NOT NULL
GROUP BY sector
ORDER BY pe_fwd_median;


-- name: cheap_vs_sector
-- 各銘柄の予想 PER がセクター中央値より割安なものを抽出（相対割安）。
WITH sector_med AS (
  SELECT sector, median(pe_forward) AS sector_pe
  FROM stocks WHERE pe_forward > 0 GROUP BY sector
)
SELECT s.symbol, s.security_ja, s.sector, s.pe_forward, m.sector_pe,
       s.pe_forward / m.sector_pe AS pe_vs_sector, s.roe, s.revenue_growth
FROM stocks s JOIN sector_med m USING (sector)
WHERE s.pe_forward > 0 AND s.pe_forward < m.sector_pe
ORDER BY pe_vs_sector ASC;


-- name: transcript_sentiment_movers
-- 直近決算トランスクリプトのセンチメントが特に高い / 低い銘柄。
SELECT s.symbol, s.security_ja, s.sector,
       s.latest_period, s.latest_sentiment_overall, s.latest_sentiment_analyst,
       s.latest_rev_yoy, s.latest_eps_yoy
FROM stocks s
WHERE s.latest_sentiment_overall IS NOT NULL
ORDER BY s.latest_sentiment_overall DESC;


-- name: earnings_accelerating
-- 決算財務: 直近四半期の増収増益（YoY）が顕著な銘柄。
SELECT symbol, period, revenue_yoy, eps_yoy, operating_margin, net_margin,
       sentiment_overall, analyst_concern_level
FROM transcripts
WHERE revenue_yoy IS NOT NULL AND eps_yoy IS NOT NULL
  AND revenue_yoy > 0.10 AND eps_yoy > 0.15
ORDER BY eps_yoy DESC;


-- name: sentiment_vs_growth
-- 「経営陣トーンは強気だが実績が伴っていない / その逆」を探す（期待と実績の乖離）。
SELECT symbol, period, sentiment_management, sentiment_analyst,
       revenue_yoy, eps_yoy, hedge_density, qa_ratio
FROM transcripts
WHERE sentiment_management IS NOT NULL AND eps_yoy IS NOT NULL
ORDER BY (sentiment_management - 100 * eps_yoy) DESC;


-- ============================================================================
-- テーマ別分析（theme_metrics）。先に `python thematic/to_duckdb.py` で
-- theme_metrics を作る。比率系は小数（-0.5 = -50%）。
-- ============================================================================

-- name: theme_cohort_summary
-- テーマ別 × コホート別の中央値サマリ（theme_metrics のみで完結）。
-- affected と resilient の「下落幅・成長・論調」の差が一目で分かる。
SELECT theme, cohort,
       count(*)                    AS n,
       median(drawdown_52w)        AS drawdown_median,
       median(ret_since_event)     AS since_event_median,
       median(excess_event)        AS excess_vs_bm_median,
       median(revenue_yoy_latest)  AS rev_yoy_median,
       median(operating_margin)    AS op_margin_median,
       median(net_signal)          AS net_signal_median
FROM theme_metrics
GROUP BY theme, cohort
ORDER BY theme, cohort;


-- name: theme_price_fundamental_divergence
-- 価格は大きく下落（52週高値比 < -40%）だが売上 YoY が減速していない
-- ＝「売られ過ぎ」候補（価格とファンダの乖離）。theme_metrics のみで完結。
SELECT theme, symbol, cohort, drawdown_52w, ret_since_event,
       revenue_yoy_latest, revenue_yoy_prev, net_signal
FROM theme_metrics
WHERE drawdown_52w < -0.4
  AND revenue_yoy_latest IS NOT NULL
  AND (revenue_yoy_prev IS NULL OR revenue_yoy_latest >= revenue_yoy_prev)
ORDER BY drawdown_52w ASC;


-- name: theme_oversold_value
-- 【要 stocks】懸念コホート（affected）で売られているのに、予想 PER が低く
-- DCF 上振れ余地が大きい ＝ 逆張り候補。stocks（build_dataset.py）が必要。
SELECT t.theme, t.symbol, t.cohort, t.drawdown_52w, t.net_signal,
       s.pe_forward, s.dcf_upside, s.target_upside, s.revenue_growth
FROM theme_metrics t
LEFT JOIN stocks s ON s.symbol_yf = t.symbol
WHERE t.cohort = 'affected'
  AND s.dcf_upside IS NOT NULL
ORDER BY t.drawdown_52w ASC;


-- name: theme_signal_vs_sentiment
-- 【要 stocks】語彙ベースの net_signal（経営陣が反証/懸念どちらを語ったか）と、
-- レポートの LLM センチメント（stocks.latest_sentiment_overall）を突合し乖離を探す。
SELECT t.theme, t.symbol, t.cohort, t.net_signal, t.bear_density, t.bull_density,
       s.latest_sentiment_overall, s.latest_hedge_density, s.latest_rev_yoy
FROM theme_metrics t
LEFT JOIN stocks s ON s.symbol_yf = t.symbol
ORDER BY t.net_signal ASC;
