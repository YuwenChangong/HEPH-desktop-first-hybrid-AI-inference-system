const { app, BrowserWindow, dialog, shell } = require("electron");
const path = require("path");
const fs = require("fs");
const { spawn } = require("child_process");
const http = require("http");

const DEFAULT_FRONTEND_BASE_URL = "http://127.0.0.1:5173";
const HEALTH_URL = "http://127.0.0.1:8000/healthz";
const DEV_ROOT_DIR = path.resolve(__dirname, "..");
const RUNTIME_ROOT_DIR = app.isPackaged
  ? path.join(process.resourcesPath, "runtime")
  : DEV_ROOT_DIR;
const OPS_DIR = path.join(RUNTIME_ROOT_DIR, "ops");
const FRONTEND_URL_FILE = path.join(OPS_DIR, "runtime", "frontend.url");
const POWERSHELL = "powershell";
const START_SCRIPT = path.join(OPS_DIR, "restart-all.ps1");
const STOP_SCRIPT = path.join(OPS_DIR, "stop-all.ps1");

let win = null;
let frontendBaseUrl = DEFAULT_FRONTEND_BASE_URL;
const singleInstanceLock = app.requestSingleInstanceLock();

if (!singleInstanceLock) {
  app.quit();
}

function ensureExternalConfigTemplates() {
  try {
    const configDir = path.join(process.env.LOCALAPPDATA || path.dirname(process.execPath), "heph", "config");
    fs.mkdirSync(configDir, { recursive: true });

    const templateDir = path.join(RUNTIME_ROOT_DIR, "config-templates");
    for (const name of ["gateway.env.example", "miner.env.example"]) {
      const source = path.join(templateDir, name);
      const target = path.join(configDir, name);
      if (fs.existsSync(source) && !fs.existsSync(target)) {
        fs.copyFileSync(source, target);
      }
    }
  } catch {
    // Ignore template sync errors so startup can continue.
  }
}

function getInstalledAppLanguage() {
  const fallback = "en";
  try {
    if (!app.isPackaged) return fallback;
    const filePath = path.join(path.dirname(process.execPath), "installer-language.txt");
    if (!fs.existsSync(filePath)) return fallback;
    const value = String(fs.readFileSync(filePath, "utf8") || "").trim().toLowerCase();
    return ["en", "zh", "ja", "fr"].includes(value) ? value : fallback;
  } catch {
    return fallback;
  }
}

function runPsScript(scriptPath) {
  return new Promise((resolve, reject) => {
    const args = ["-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", scriptPath];
    const child = spawn(POWERSHELL, args, {
      cwd: RUNTIME_ROOT_DIR,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
      env: {
        ...process.env,
        APP_RUNTIME_ROOT: RUNTIME_ROOT_DIR,
      },
    });

    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk || "");
    });

    child.on("error", (err) => reject(err));
    child.on("close", (code) => {
      if (code === 0) return resolve();
      reject(new Error(`PowerShell script failed (${code}): ${stderr.trim()}`));
    });
  });
}

function getFrontendBaseUrl() {
  try {
    if (fs.existsSync(FRONTEND_URL_FILE)) {
      const value = String(fs.readFileSync(FRONTEND_URL_FILE, "utf8") || "").trim();
      if (/^http:\/\/127\.0\.0\.1:\d+$/.test(value)) {
        return value;
      }
    }
  } catch {
    // Ignore malformed frontend url hints and fall back to the default port.
  }
  return DEFAULT_FRONTEND_BASE_URL;
}

async function waitFrontendBaseUrl(timeoutMs = 12000, intervalMs = 400) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const value = getFrontendBaseUrl();
    if (value !== DEFAULT_FRONTEND_BASE_URL) {
      return value;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  return getFrontendBaseUrl();
}

function waitHealthz(timeoutMs = 45000, intervalMs = 800) {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const check = () => {
      const req = http.get(HEALTH_URL, (res) => {
        if (res.statusCode === 200) {
          res.resume();
          resolve();
          return;
        }
        res.resume();
        if (Date.now() - start >= timeoutMs) {
          reject(new Error(`Gateway health timeout: HTTP ${res.statusCode}`));
          return;
        }
        setTimeout(check, intervalMs);
      });

      req.on("error", () => {
        if (Date.now() - start >= timeoutMs) {
          reject(new Error("Gateway health timeout: cannot connect"));
          return;
        }
        setTimeout(check, intervalMs);
      });
    };
    check();
  });
}

function waitFrontendReady(baseUrl, timeoutMs = 30000, intervalMs = 800) {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const check = () => {
      const req = http.get(`${baseUrl}/index.html`, (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          body += String(chunk || "");
          if (body.length > 8192) {
            body = body.slice(0, 8192);
          }
        });
        res.on("end", () => {
          const looksReady =
            res.statusCode === 200 &&
            body.includes("<title>HEPH</title>") &&
            body.includes('id="app"') &&
            body.includes("./app.js");

          if (looksReady) {
            resolve();
            return;
          }

          if (Date.now() - start >= timeoutMs) {
            reject(new Error(`Frontend ready timeout: invalid frontend response on ${baseUrl}`));
            return;
          }
          setTimeout(check, intervalMs);
        });
      });

      req.on("error", () => {
        if (Date.now() - start >= timeoutMs) {
          reject(new Error("Frontend ready timeout: cannot connect"));
          return;
        }
        setTimeout(check, intervalMs);
      });
    };
    check();
  });
}

async function bootServices() {
  ensureExternalConfigTemplates();
  await runPsScript(START_SCRIPT);
  frontendBaseUrl = await waitFrontendBaseUrl();
  await waitHealthz();
  await waitFrontendReady(frontendBaseUrl);
}

async function bootServicesWithRetry() {
  try {
    await bootServices();
    return;
  } catch (firstErr) {
    // First-run race can happen right after install; do one controlled retry.
    await new Promise((resolve) => setTimeout(resolve, 1500));
    await bootServices();
  }
}

function createWindow() {
  const installerLang = getInstalledAppLanguage();
  win = new BrowserWindow({
    width: 1440,
    height: 900,
    minWidth: 1200,
    minHeight: 760,
    backgroundColor: "#0b0d10",
    autoHideMenuBar: true,
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  const cacheBuster = Date.now();
  const frontendUrl = `${frontendBaseUrl}/index.html?appLang=${encodeURIComponent(installerLang)}&v=${cacheBuster}#chat`;
  win.webContents.session.clearCache().catch(() => {
    // Ignore cache clear errors and continue loading the app shell.
  });
  win.webContents.loadURL(frontendUrl, {
    extraHeaders: "Cache-Control: no-cache\r\nPragma: no-cache\r\n",
  });
  win.on("closed", () => {
    win = null;
  });

  win.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: "deny" };
  });
}

app.on("second-instance", () => {
  if (win) {
    if (win.isMinimized()) win.restore();
    win.focus();
  }
});

app.whenReady().then(async () => {
  try {
    await bootServicesWithRetry();
    createWindow();
  } catch (error) {
    dialog.showErrorBox(
      "Startup failed",
      String(error?.message || error || "Unknown startup error")
    );
    app.quit();
  }

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on("window-all-closed", async () => {
  try {
    await runPsScript(STOP_SCRIPT);
  } catch {
    // Ignore stop errors to avoid blocking quit.
  }
  if (process.platform !== "darwin") {
    app.quit();
  }
});
