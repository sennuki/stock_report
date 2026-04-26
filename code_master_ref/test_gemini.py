from utils import get_gemini_client, log_event
import os

def test_translation():
    print("--- Gemma 4 26B (MoE) テスト ---")
    client = get_gemini_client()
    model_name = "models/gemma-4-26b-a4b-it"
    
    if not client:
        print("❌ クライアントの初期化に失敗しました。")
        return

    test_text = "Apple Inc. is an American multinational corporation and technology company headquartered in Cupertino, California."
    print(f"原文: {test_text}")
    
    try:
        response = client.models.generate_content(
            model=model_name,
            contents=f"以下の英文を日本語に翻訳してください: {test_text}",
            config={
                "system_instruction": "回答は翻訳後のテキストのみを出力してください"
            }
        )
        print(f"✅ 翻訳結果: {response.text}")
        log_event("INFO", "SYSTEM", "Gemini 3.1 API test successful.")
    except Exception as e:
        print(f"❌ エラー: {e}")
        log_event("ERROR", "SYSTEM", f"Gemini 3.1 API test failed: {e}")

if __name__ == "__main__":
    test_translation()
