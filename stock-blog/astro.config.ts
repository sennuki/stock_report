import { defineConfig, envField } from "astro/config";
import cloudflare from "@astrojs/cloudflare";
import tailwindcss from "@tailwindcss/vite";
import sitemap from "@astrojs/sitemap";
import remarkToc from "remark-toc";
import remarkCollapse from "remark-collapse";
import {
  transformerNotationDiff,
  transformerNotationHighlight,
  transformerNotationWordHighlight,
} from "@shikijs/transformers";
import { transformerFileName } from "./src/utils/transformers/fileName";
import { SITE } from "./src/config";

// https://astro.build/config
export default defineConfig({
  site: SITE.website,
  base: "/",
  output: "server",
  adapter: cloudflare({}),

  // セッション機能はサイト内で未使用 (Astro.session を呼び出していない)。
  // 設定を省略すると @astrojs/cloudflare が KV バックエンドのセッションを
  // 自動で有効化し、デプロイ時に "SESSION" 用 KV ネームスペースの作成を
  // 要求する (API トークンに Workers KV Storage:Edit 権限が必要になる)。
  // 非 KV の memory ドライバを明示することで KV プロビジョニング自体を
  // 回避する。セッションは未使用のため非永続でも実害はない。
  session: {
    driver: "memory",
  },

  integrations: [
    sitemap({
      filter: page => SITE.showArchives || !page.endsWith("/archives"),
    }),
  ],

  markdown: {
    remarkPlugins: [remarkToc, [remarkCollapse, { test: "Table of contents" }]],
    shikiConfig: {
      // For more themes, visit https://shiki.style/themes
      themes: { light: "min-light", dark: "night-owl" },
      defaultColor: false,
      wrap: false,
      transformers: [
        transformerFileName({ style: "v2", hideDot: false }),
        transformerNotationHighlight(),
        transformerNotationWordHighlight(),
        transformerNotationDiff({ matchAlgorithm: "v3" }),
      ],
    },
  },

  vite: {
    // eslint-disable-next-line
    // @ts-ignore
    // This will be fixed in Astro 6 with Vite 7 support
    // See: https://github.com/withastro/astro/issues/14030
    plugins: [tailwindcss()],
  },

  image: {
    responsiveStyles: true,
    layout: "constrained",
  },

  env: {
    schema: {
      PUBLIC_GOOGLE_SITE_VERIFICATION: envField.string({
        access: "public",
        context: "server",
        optional: true,
      }),
      PUBLIC_GOOGLE_ANALYTICS_ID: envField.string({
        access: "public",
        context: "server",
        optional: true,
      }),
      // PUBLIC_WORKER_API_URL は schema 登録すると import.meta.env 経由で
      // 取りにくくなるため、Vite の素の PUBLIC_* プレフィックス挙動に任せる。
    },
  },
});