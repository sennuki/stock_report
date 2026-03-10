# Stock Blog プロジェクト構成ドキュメント (Astro)

`stock-blog/` ディレクトリは、[Astro](https://astro.build/) フレームワークを使用した静的サイト生成（SSG）プロジェクトです。Python 側で生成されたデータを Web サイトとして表示する役割を担います。

## 1. プロジェクト設定・構成

| ファイル名 | 役割 |
| :--- | :--- |
| `astro.config.ts` | **Astro メイン設定**。サイトのベースURL、インテグレーション、ビルド設定を定義します。 |
| `package.json` | プロジェクトの依存関係と、開発 (`dev`)・ビルド (`build`) 用のスクリプトを定義します。 |
| `tsconfig.json` | TypeScript の型定義とパス設定。 |
| `src/config.ts` | サイトタイトル、説明、SNSリンクなどのグローバル設定。 |
| `src/content.config.ts` | **コンテンツ・レイヤー設定**。Markdown 記事（銘柄レポート）のスキーマや読み込みルールを定義します。 |

## 2. ページとレイアウト (`src/`)

| ディレクトリ | 役割 |
| :--- | :--- |
| `src/pages/` | **ルーティング**。ファイル名がそのまま URL になります (例: `index.astro` → `/`)。 |
| `src/pages/report/[symbol].astro` | **銘柄別レポートページ**。各銘柄のレポートを動的に表示するテンプレートです。 |
| `src/layouts/` | ページ共通の HTML 構造（Header, Footer, メタタグ等）を定義します。 |
| `src/components/` | ボタン、カード、パンくずリストなどの再利用可能な UI パーツ。 |

## 3. コンテンツとデータ

| ディレクトリ | 役割 |
| :--- | :--- |
| `public/reports/` | **銘柄データ (JSON)**。Python 側で生成された詳細な財務データ、チャート用データが格納されます。 |
| `src/data/stocks.json` | 銘柄リストのインデックスデータ。銘柄一覧ページ (`index.astro`) の生成に使用されます。 |
| `src/data/blog/` | (非推奨) 旧形式のレポート本文。現在は `src/pages/report/[symbol].astro` による動的生成に移行したため、基本的には使用されません。 |
| `public/output_reports_full/` | (非推奨) 旧形式の HTML レポート。現在は JSON データから直接コンポーネントをレンダリングするため、使用されません。 |

## 4. スクリプト

| ファイル名 | 役割 |
| :--- | :--- |
| `scripts/generate-blog-posts.js` | (非推奨) 旧形式の Markdown 記事生成スクリプト。現在は使用されていません。 |

## 開発フロー

1.  **データ更新**: ルートの `code/` ディレクトリにある Python スクリプトを実行し、`src/data/stocks.json` と `public/reports/` を更新。
2.  **プレビュー**: `pnpm run dev` でローカルサーバーを起動し確認。
3.  **ビルド**: `pnpm run build` で静的ファイルを生成。
