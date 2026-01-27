export const SITE = {
  website: "https://sennuki.github.io/stock_report/", // replace this with your deployed domain
  author: "Sat Naing",
  profile: "https://satnaing.dev/",
  desc: "株式銘柄の分析レポートを公開するブログです。",
  title: "銘柄分析ブログ",
  ogImage: "astropaper-og.jpg",
  lightAndDarkMode: true,
  postPerIndex: 4,
  postPerPage: 4,
  scheduledPostMargin: 15 * 60 * 1000, // 15 minutes
  showArchives: false,
  showBackButton: true, // show back button in post detail
  editPost: {
    enabled: true,
    text: "ページを編集",
    url: "https://github.com/satnaing/astro-paper/edit/main/",
  },
  dynamicOgImage: true,
  dir: "ltr", // "rtl" | "auto"
  lang: "ja", // html lang code. Set this empty and default will be "en"
  timezone: "Asia/Tokyo", // Default global timezone (IANA format) https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
} as const;
