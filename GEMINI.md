# GEMINI.md

## Important Mandates (Precedence over General Workflows)

1. **Current Context (2026):** 
   - My internal knowledge baseline may be outdated (e.g., assuming it's 2024 or 2025). 
   - ALWAYS verify the current date and available AI models (Gemini 3.1, Gemma 4 series, etc.) using `google_web_search` before making technical judgments or suggesting model changes.
   
2. **Model Awareness & Standardization (April 2026):**
   - **Market Movement Reasons:** ALWAYS use `models/gemma-4-31b-it` with Google Search grounding (`tools: [{ google_search: {} }]`). 株価が大きく変動した銘柄の変動理由調査に使用する。Gemma 4 31B は Gemini API 経由で Google 検索グラウンディングに対応する。
   - **Translations, Summaries, and General Text:** ALWAYS use `models/gemma-4-26b-a4b-it` (Gemma 4 26B MoE). It is the most efficient and high-quality open-weight model for these tasks.
   - **Legacy Models:** Models from the 1.5, 2.0, and early 3.1 series are deprecated or shut down. Do NOT use them in any new or existing code.

3. **Behavioral Correction:**
   - I have a tendency to rely on older training data regarding API capabilities. 
   - I MUST treat the results of `google_web_search` as the ground truth for any information related to the 2026 technology landscape.

4. **Chart Layout Conventions:**
   - **Income Statement:** ALWAYS position the "Net Income" (純利益) bar on the far right of the chart. In visualization components like `ChartJs.astro`, ensure its priority (e.g., `groupPriority`) is set higher than other metrics such as Revenue or Operating Income to maintain this order.
