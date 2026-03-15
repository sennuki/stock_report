import { mkdir, writeFile, rm } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import path from "node:path";
import AdmZip from "adm-zip"; // pnpm install adm-zip が必要

const REPO = "sennuki/stock_report";
const ARTIFACT_NAME = "stock-reports-json";
const TARGET_DIR = "public/output_reports_full";
const TOKEN = process.env.GITHUB_TOKEN;

if (!TOKEN) {
  console.error("Error: GITHUB_TOKEN environment variable is not set.");
  process.exit(1);
}

async function fetchLatestArtifact() {
  console.log(`Fetching latest artifact: ${ARTIFACT_NAME}...`);
  
  const response = await fetch(`https://api.github.com/repos/${REPO}/actions/artifacts?name=${ARTIFACT_NAME}`, {
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "Node.js-Fetch",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch artifacts: ${response.statusText}`);
  }

  const data = await response.json();
  const artifact = data.artifacts[0];

  if (!artifact) {
    throw new Error(`No artifact found with name: ${ARTIFACT_NAME}`);
  }

  console.log(`Found artifact ID: ${artifact.id} (created at ${artifact.created_at})`);
  return artifact.id;
}

async function downloadAndExtract(artifactId) {
  const downloadUrl = `https://api.github.com/repos/${REPO}/actions/artifacts/${artifactId}/zip`;
  console.log("Downloading...");

  const response = await fetch(downloadUrl, {
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      "User-Agent": "Node.js-Fetch",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to download artifact: ${response.statusText}`);
  }

  // Node.js 18+ の fetch body は直接 stream できない場合があるため arrayBuffer 経由にする
  const buffer = Buffer.from(await response.arrayBuffer());
  const zipPath = "reports_temp.zip";
  await writeFile(zipPath, buffer);

  console.log(`Extracting to ${TARGET_DIR}...`);
  await mkdir(TARGET_DIR, { recursive: true });

  const zip = new AdmZip(zipPath);
  const zipEntries = zip.getEntries();

  for (const entry of zipEntries) {
    if (entry.isDirectory) continue;

    // ファイル名（base）のみ取得して、TARGET_DIR 直下に保存
    const fileName = path.basename(entry.entryName);
    
    // 特定のファイル（stocks.json）は src/data に、それ以外は public/output_reports_full に分ける処理
    if (fileName === "stocks.json") {
      const dataDir = "src/data";
      await mkdir(dataDir, { recursive: true });
      await writeFile(path.join(dataDir, fileName), entry.getData());
      console.log(`- Extracted: ${fileName} to ${dataDir}`);
    } else if (fileName.endsWith(".json")) {
      await writeFile(path.join(TARGET_DIR, fileName), entry.getData());
      console.log(`- Extracted: ${fileName} to ${TARGET_DIR}`);
    }
  }

  await rm(zipPath);
  console.log("Successfully updated reports!");
}

try {
  const id = await fetchLatestArtifact();
  await downloadAndExtract(id);
} catch (error) {
  console.error("Failed:", error.message);
  process.exit(1);
}
