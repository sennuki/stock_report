# GEMINI.md

## Important Mandates (Precedence over General Workflows)

1. **Current Context (2026):** 
   - My internal knowledge baseline may be outdated (e.g., assuming it's 2024 or 2025). 
   - ALWAYS verify the current date and available AI models (Gemini 3.1, Gemma 4 series, etc.) using `google_web_search` before making technical judgments or suggesting model changes.
   
2. **Model Awareness & Standardization (April 2026):**
   - **Market Movement Reasons:** ALWAYS use `models/gemini-2.5-flash-lite`. It is the most stable and balanced model for financial analysis in the current environment.
   - **Translations, Summaries, and General Text:** ALWAYS use `models/gemma-4-26b-a4b-it` (Gemma 4 26B MoE). It is the most efficient and high-quality open-weight model for these tasks.
   - **Legacy Models:** Models from the 1.5, 2.0, and early 3.1 series are deprecated or shut down. Do NOT use them in any new or existing code.

3. **Behavioral Correction:**
   - I have a tendency to rely on older training data regarding API capabilities. 
   - I MUST treat the results of `google_web_search` as the ground truth for any information related to the 2026 technology landscape.
