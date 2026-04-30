const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawnSync } = require("child_process");

const desktopRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(desktopRoot, "..");
const stageRoot = path.join(__dirname, "runtime-staged");
const pythonExe = path.join(repoRoot, ".venv", "Scripts", "python.exe");

function resetDir(target) {
  if (fs.existsSync(target)) {
    const archived = `${target}.stale-${Date.now()}`;
    try {
      fs.renameSync(target, archived);
      fs.rmSync(archived, { recursive: true, force: true });
    } catch {
      for (const entry of fs.readdirSync(target, { withFileTypes: true })) {
        const child = path.join(target, entry.name);
        try {
          fs.rmSync(child, { recursive: true, force: true });
        } catch {
          // Leave locked stale entries in place; current run only writes whitelisted files.
        }
      }
    }
  }
  fs.mkdirSync(target, { recursive: true });
}

function ensureDir(target) {
  fs.mkdirSync(target, { recursive: true });
}

function copyFile(src, dest) {
  ensureDir(path.dirname(dest));
  fs.copyFileSync(src, dest);
}

function copyTree(src, dest, shouldSkip) {
  const stat = fs.statSync(src);
  if (stat.isDirectory()) {
    ensureDir(dest);
    for (const entry of fs.readdirSync(src, { withFileTypes: true })) {
      const srcPath = path.join(src, entry.name);
      const destPath = path.join(dest, entry.name);
      if (shouldSkip && shouldSkip(srcPath, entry)) {
        continue;
      }
      copyTree(srcPath, destPath, shouldSkip);
    }
    return;
  }
  copyFile(src, dest);
}

function compileEntrypoint(sourcePath, outputPath) {
  if (!fs.existsSync(pythonExe)) {
    throw new Error(`Python runtime not found: ${pythonExe}`);
  }
  ensureDir(path.dirname(outputPath));
  const tempCompilerScript = path.join(
    os.tmpdir(),
    `heph-compile-entrypoint-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.py`
  );
  const tempRunnerScript = path.join(
    os.tmpdir(),
    `heph-run-compiler-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.ps1`
  );
  const compilerBody = [
    "import importlib._bootstrap_external as be",
    "import pathlib",
    "import sys",
    "import time",
    "source_path = pathlib.Path(sys.argv[1])",
    "output_path = pathlib.Path(sys.argv[2])",
    "virtual_name = sys.argv[3]",
    "output_path.parent.mkdir(parents=True, exist_ok=True)",
    "source_bytes = source_path.read_bytes()",
    "code = compile(source_bytes, virtual_name, 'exec')",
    "data = be._code_to_timestamp_pyc(code, int(time.time()), len(source_bytes))",
    "output_path.write_bytes(data)",
  ].join("\n");
  fs.writeFileSync(tempCompilerScript, compilerBody, "utf8");
  const runnerBody = [
    `$python = ${JSON.stringify(pythonExe)}`,
    `$script = ${JSON.stringify(tempCompilerScript)}`,
    `$source = ${JSON.stringify(sourcePath)}`,
    `$output = ${JSON.stringify(outputPath)}`,
    `$virtual = ${JSON.stringify(path.basename(sourcePath))}`,
    "& $python -X utf8 $script $source $output $virtual",
    "exit $LASTEXITCODE",
  ].join("\n");
  fs.writeFileSync(tempRunnerScript, runnerBody, "utf8");
  const result = spawnSync("powershell.exe", ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", tempRunnerScript], {
    cwd: repoRoot,
    encoding: "utf8",
    stdio: "pipe",
  });
  try {
    fs.rmSync(tempCompilerScript, { force: true });
    fs.rmSync(tempRunnerScript, { force: true });
  } catch {
    // Ignore temp cleanup failures on Windows file locking.
  }
  if (result.error) {
    throw new Error(`Failed to spawn Python for ${sourcePath}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(
      `Failed to compile ${sourcePath}: ${String(result.stderr || result.stdout || "").trim()}`
    );
  }
}

function main() {
  resetDir(stageRoot);

  copyTree(
    path.join(repoRoot, "ollama"),
    path.join(stageRoot, "ollama"),
    (srcPath, entry) => {
      const normalized = srcPath.replace(/\\/g, "/");
      if (normalized.includes("/__pycache__/")) return true;
      if (entry.isFile() && normalized.endsWith(".pyc")) return true;
      if (entry.isFile() && normalized.endsWith(".log")) return true;
      return false;
    }
  );

  copyTree(
    path.join(repoRoot, "ops"),
    path.join(stageRoot, "ops"),
    (srcPath) => {
      const normalized = srcPath.replace(/\\/g, "/");
      return (
        normalized.includes("/logs/") ||
        normalized.includes("/runtime/") ||
        normalized.endsWith("/GO_LIVE_CHECKLIST.md")
      );
    }
  );

  copyTree(
    path.join(repoRoot, ".venv"),
    path.join(stageRoot, ".venv"),
    (srcPath, entry) => {
      const normalized = srcPath.replace(/\\/g, "/");
      if (normalized.includes("/__pycache__/")) return true;
      if (entry.isFile() && normalized.endsWith(".pyc")) return true;
      if (normalized.includes("/pip-cache/")) return true;
      return false;
    }
  );

  copyTree(
    path.join(repoRoot, "前后端", "前端"),
    path.join(stageRoot, "前后端", "前端")
  );

  copyTree(
    path.join(desktopRoot, "build", "config-templates"),
    path.join(stageRoot, "config-templates")
  );

  compileEntrypoint(
    path.join(repoRoot, "v12_gateway", "api", "index.py"),
    path.join(stageRoot, "v12_gateway", "api", "index.pyc")
  );

  compileEntrypoint(
    path.join(repoRoot, "前后端", "miner.py1", "heph.py"),
    path.join(stageRoot, "前后端", "miner.py1", "heph.pyc")
  );

  console.log(`Prepared staged runtime at ${stageRoot}`);
}

main();
