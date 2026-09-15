import { pathToFileURL } from "node:url";
import path from "node:path";
import { fileURLToPath } from "node:url";
import puppeteer from "puppeteer-core";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const htmlPath = path.join(__dirname, "index.html");
const outPath = path.join(__dirname, "..", "Makai-Async-Job-Architecture.pdf");
const chrome =
  process.env.CHROME_PATH ||
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

const browser = await puppeteer.launch({
  executablePath: chrome,
  headless: true,
});

try {
  const page = await browser.newPage();
  await page.goto(pathToFileURL(htmlPath).href, {
    waitUntil: "networkidle0",
    timeout: 60000,
  });
  await page.pdf({
    path: outPath,
    format: "Letter",
    printBackground: true,
    displayHeaderFooter: true,
    headerTemplate: `<div></div>`,
    footerTemplate: `
      <div style="font-size:8px; color:#5c6770; width:100%; padding:0 28px; display:flex; justify-content:space-between; font-family:Segoe UI, Arial, sans-serif;">
        <span>Makai Labs · Async Technical Exercise · Alex Pintado</span>
        <span><span class="pageNumber"></span> / <span class="totalPages"></span></span>
      </div>`,
    margin: {
      top: "0.65in",
      bottom: "0.7in",
      left: "0.65in",
      right: "0.65in",
    },
  });
  console.log("wrote", outPath);
} finally {
  await browser.close();
}
