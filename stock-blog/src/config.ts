export const SITE = {
  website: "https://amerikabu.com/",
  author: "アメリカ株インサイト 管理人",
  profile: "https://amerikabu.com/",
  desc: "S&P 500・400・600（米国の大型・中型・小型株）の構成銘柄について、株価・PER・ROE・配当利回り・財務データを日本語で整理した、米国株のファンダメンタル分析サイトです。",
  title: "アメリカ株インサイト",
  ogImage: "og-image.jpg",
  lightAndDarkMode: true,
  postPerIndex: 4,
  postPerPage: 4,
  scheduledPostMargin: 15 * 60 * 1000, // 15 minutes
  showArchives: false,
  showBackButton: true, // show back button in post detail
  editPost: {
    enabled: false,
    text: "ページを編集",
    url: "https://github.com/satnaing/astro-paper/edit/main/",
  },
  dynamicOgImage: false,
  dir: "ltr", // "rtl" | "auto"
  lang: "ja", // html lang code. Set this empty and default will be "en"
  timezone: "Asia/Tokyo", // Default global timezone (IANA format) https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
} as const;
