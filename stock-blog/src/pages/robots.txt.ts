import type { APIRoute } from "astro";

const getRobotsTxt = (sitemapURL: URL, siteURL: URL) => `
User-agent: *
Allow: /
Disallow: /reports/
Disallow: /og.png
Crawl-delay: 1

Host: ${siteURL.hostname}
Sitemap: ${sitemapURL.href}
`;

export const GET: APIRoute = ({ site }) => {
  const sitemapURL = new URL("sitemap-index.xml", site);
  return new Response(getRobotsTxt(sitemapURL, site!));
};
