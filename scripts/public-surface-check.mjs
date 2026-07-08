import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { extname, join, relative } from "node:path";

const roots = [
  "README.md",
  "CONTRIBUTING.md",
  "Makefile",
  "SECURITY.md",
  "NOTICE",
  "package.json",
  "examples",
  "go",
  "proto",
  "python",
  "src",
  "test",
];

const skippedPathParts = new Set([
  ".git",
  ".venv",
  "coverage",
  "dist",
  "node_modules",
  "__pycache__",
]);

const scannedExtensions = new Set([
  "",
  ".go",
  ".json",
  ".md",
  ".mod",
  ".proto",
  ".py",
  ".toml",
  ".ts",
]);

const binaryExtensions = new Set([".binpb", ".gz", ".lock", ".png", ".tgz", ".whl", ".zip"]);

const checks = [
  {
    name: "SSH Git URL",
    pattern: /\b(?:git@github\.com|ssh:\/\/)/i,
  },
  {
    name: "private Medallion repository URL",
    pattern: /\bgithub\.com\/jim-technologies\/medallion-(?:connect|ontology|storage)(?:[/?#.\s"'`)]|$)/i,
  },
  {
    name: "sibling private repository path",
    pattern: /(?:^|[\s"'`(])\.\.\/medallion-(?:connect|ontology|storage)\b/i,
  },
  {
    name: "internal deployment or secrets tooling",
    pattern: /\b(?:Nomad|SOPS)\b/,
  },
  {
    name: "internal project wording",
    pattern: /\b(?:dogfood|first-client|monorepo|preshared|temporaless)\b/i,
  },
  {
    name: "private JimTech hostname",
    pattern:
      /\bhttps?:\/\/(?!(?:github|codeload)\.com\/jim-technologies\/(?:medallion-sdk|invariantprotocol)(?:[/?#.\s"'`)]|$))[^"'\s)]+(?:jimtech|jim-technologies)[^"'\s)]*/i,
  },
];

const files = [];
for (const root of roots) {
  collect(root, files);
}

const failures = [];
for (const file of files) {
  const text = readFileSync(file, "utf8");
  const lines = text.split(/\r?\n/);
  for (const [index, line] of lines.entries()) {
    for (const check of checks) {
      if (check.pattern.test(line)) {
        failures.push({
          file,
          line: index + 1,
          check: check.name,
          text: line.trim(),
        });
      }
    }
  }
}

if (failures.length > 0) {
  console.error("Public SDK surface check failed:");
  for (const failure of failures.slice(0, 50)) {
    console.error(
      `- ${failure.file}:${failure.line}: ${failure.check}: ${truncate(failure.text)}`,
    );
  }
  if (failures.length > 50) {
    console.error(`...and ${failures.length - 50} more findings.`);
  }
  console.error(
    "Keep public SDK docs, examples, package metadata, tests, and vendored proto comments free of private repo/deployment details.",
  );
  process.exit(1);
}

console.log(`Public SDK surface check passed (${files.length} files scanned).`);

function collect(path, output) {
  if (!existsSync(path)) return;

  const stats = statSync(path);
  if (stats.isDirectory()) {
    for (const child of readdirSync(path)) {
      if (skippedPathParts.has(child)) continue;
      collect(join(path, child), output);
    }
    return;
  }

  const extension = extname(path);
  if (binaryExtensions.has(extension)) return;
  if (!scannedExtensions.has(extension)) return;

  output.push(relative(process.cwd(), path));
}

function truncate(text) {
  return text.length > 180 ? `${text.slice(0, 177)}...` : text;
}
