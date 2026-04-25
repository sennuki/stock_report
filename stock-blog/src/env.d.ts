/// <reference types="astro/client" />

interface Env {
  STOCK_DATA: any; // R2Bucket type would be better but requires @cloudflare/workers-types
}

declare namespace App {
  interface Locals {
    runtime: import("@astrojs/cloudflare").Runtime<Env>;
  }
}

interface Window {
  theme?: {
    themeValue: string;
    setPreference: () => void;
    reflectPreference: () => void;
    getTheme: () => string;
    setTheme: (val: string) => void;
  };
}
