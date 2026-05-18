# Stock Blog プロジェクト構成ドキュメント (Astro SSR)

`stock-blog/` ディレクトリは、[Astro](https://astro.build/) フレームワークを使用したサーバーサイドレンダリング（SSR）プロジェクトです。Cloudflare Pages 上で動作し、R2 バケットに格納された銘柄データを動的に表示します。

## 1. プロジェクト設定・構成

| ファイル名 | 役割 |
| :--- | :--- |
| `astro.config.ts` | **Astro メイン設定**。`output: "server"` と `cloudflare` アダプターを使用し、SSR モードで動作します。 |
| `package.json` | プロジェクトの依存関係と、開発 (`dev`)・ビルド (`build`) 用のスクリプト。 |
| `tsconfig.json` | TypeScript の型定義とパス設定。 |
| `src/config.ts` | サイトタイトル、説明などのグローバル設定。 |
| `src/content.config.ts` | **コンテンツ・レイヤー設定**。現在は Markdown 記事を使用せず、動的ルーティングに移行済み。 |

## 2. ページとレイアウト (`src/`)

| ディレクトリ | 役割 |
| :--- | :--- |
| `src/pages/` | **ルーティング**。 |
| `src/pages/index.astro` | **トップページ**。S&P 500 銘柄の一覧と、前日比ランキングを表示します。 |
| `src/pages/report/[symbol].astro` | **動的レポートページ**。Cloudflare R2 から銘柄データを取得し、オンデマンドでレポートを生成・表示します。 |
| `src/layouts/` | ページ共通の HTML 構造（Header, Footer, メタタグ等）。 |
| `src/components/` | TradingView ウィジェットや、各種 UI コンポーネント。 |

## 3. データと外部連携

| ディレクトリ/サービス | 役割 |
| :--- | :--- |
| `src/data/stocks.json` | 銘柄リストのインデックスデータ。トップページの一覧生成に使用されます。 |
| **Cloudflare R2** | 銘柄別の生データ (JSON) を格納するオブジェクトストレージ。SSR 時にここからデータを読み込みます。 |

## 4. 廃止されたコンポーネント

| パス | 理由 |
| :--- | :--- |
| `scripts/generate-blog-posts.js` | SSR 移行により Markdown ファイルの事前生成が不要になったため。 |
| `public/output_reports_full/` | HTML レポートから動的レンダリングへ移行したため。 |

## 開発フロー

1.  **データ更新**: ルートの `code/main.py` を実行し、`src/data/stocks.json` の更新と R2 へのデータアップロードを行う。
2.  **プレビュー**: `wrangler pages dev` または `pnpm dev` でローカル確認。
3.  **デプロイ**: GitHub Actions により、ビルドおよび Cloudflare Pages へのデプロイが自動実行される。
