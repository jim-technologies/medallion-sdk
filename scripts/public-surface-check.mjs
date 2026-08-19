import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { extname, join, relative } from "node:path";

const roots = [
  ".ci",
  ".flox/.gitattributes",
  ".flox/.gitignore",
  ".flox/env.json",
  ".flox/env/manifest.lock",
  ".flox/env/manifest.toml",
  ".github",
  ".gitattributes",
  ".gitignore",
  ".gitleaks.toml",
  "README.md",
  "CHANGELOG.md",
  "CONTRIBUTING.md",
  "MAKEFILE-CONTRACT.md",
  "Makefile",
  "SECURITY.md",
  "NOTICE",
  "VERSION",
  "biome.json",
  "buf.gen.yaml",
  "buf.yaml",
  "go.mod",
  "go.sum",
  "package.json",
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "examples",
  "go",
  "proto",
  "python",
  "scripts",
  "src",
  "test",
  "tsconfig.build.json",
  "tsconfig.json",
  "tsup.config.ts",
  "vitest.config.ts",
];

const skippedPathParts = new Set([
  ".git",
  ".venv",
  "coverage",
  "dist",
  "node_modules",
  "__pycache__",
]);

const generatedFloxPathParts = new Set(["cache", "log", "run"]);

const scannedExtensions = new Set([
  "",
  ".go",
  ".json",
  ".lock",
  ".md",
  ".mod",
  ".proto",
  ".py",
  ".sh",
  ".sum",
  ".toml",
  ".ts",
  ".yaml",
  ".yml",
]);

const binaryExtensions = new Set([
  ".binpb",
  ".gz",
  ".png",
  ".tgz",
  ".whl",
  ".zip",
]);

// Build private implementation markers from fragments so this boundary check
// does not itself place those markers in the public source tree.
const privateImplementationName = ["Onto", "logy"].join("");
const privateRepositoryName = ["medallion-", privateImplementationName].join(
  "",
);
const retiredRootVariable = [
  "MEDALLION_",
  privateImplementationName.toUpperCase(),
  "_ROOT",
].join("");
const retiredPolicyName = [
  privateImplementationName.toLowerCase(),
  "-idempotency-policy",
].join("");
const privateServiceName = [
  "Medallion",
  privateImplementationName,
  "Service",
].join("");
const privatePackageName = [
  "medallion.",
  privateImplementationName.toLowerCase(),
  ".v1",
].join("");
const privateRepositoryFamily = [
  "medallion-(?:cloud|connect|",
  privateImplementationName.toLowerCase(),
  "|storage|terminal)",
].join("");
const retiredConsumerProfilesName = ["consumer", "profiles"].join("-");
const privateSourceRepositoryKey = ["source", "repository"].join("_");
const terminalProductName = ["Medallion", "Terminal"].join(" ");
const compassProductName = ["Com", "pass"].join("");
const terminalProfileName = ["term", "inal"].join("");
const compassProfileName = ["com", "pass"].join("");
const retiredTenantSnake = ["tenant", "id"].join("_");
const retiredTenantCamel = ["tenant", "Id"].join("");
const retiredTenantGo = ["Tenant", "ID"].join("");
const retiredOrganizationSnake = ["organization", "id"].join("_");
const retiredOrganizationCamel = ["organization", "Id"].join("");
const retiredOrganizationGo = ["Organization", "ID"].join("");
const retiredTenantHeader = ["X-Jimtech-", "Tenant-Id"].join("");

const retiredScopeChecks = [
  {
    name: "retired tenant snake-case scope",
    pattern: new RegExp(`\\b${retiredTenantSnake}\\b`),
    sample: retiredTenantSnake,
  },
  {
    name: "retired tenant camel-case scope",
    pattern: new RegExp(`\\b${retiredTenantCamel}\\b`),
    sample: retiredTenantCamel,
  },
  {
    name: "retired tenant Go scope",
    pattern: new RegExp(`\\b${retiredTenantGo}\\b`),
    sample: retiredTenantGo,
  },
  {
    name: "retired organization snake-case scope",
    pattern: new RegExp(`\\b${retiredOrganizationSnake}\\b`),
    sample: retiredOrganizationSnake,
  },
  {
    name: "retired organization camel-case scope",
    pattern: new RegExp(`\\b${retiredOrganizationCamel}\\b`),
    sample: retiredOrganizationCamel,
  },
  {
    name: "retired organization Go scope",
    pattern: new RegExp(`\\b${retiredOrganizationGo}\\b`),
    sample: retiredOrganizationGo,
  },
  {
    name: "retired tenant-selection header",
    pattern: new RegExp(`\\b${retiredTenantHeader}\\b`, "i"),
    sample: retiredTenantHeader,
  },
];

const checks = [
  {
    name: "SSH Git URL",
    pattern: /\b(?:git@github\.com|ssh:\/\/)/i,
  },
  {
    name: "private Medallion repository URL",
    pattern: new RegExp(
      `\\bgithub\\.com/jim-technologies/${privateRepositoryFamily}(?:[/?#.\\s"')]|$)`,
      "i",
    ),
    sample: `https://github.com/jim-technologies/${privateRepositoryName}`,
  },
  {
    name: "sibling private repository path",
    pattern: new RegExp(
      `(?:^|[\\s"'(])\\.\\./${privateRepositoryFamily}\\b`,
      "i",
    ),
    sample: `../${privateRepositoryName}`,
  },
  {
    name: "private backend implementation name",
    pattern: new RegExp(`\\b${privateImplementationName}\\b`, "i"),
    sample: privateImplementationName,
  },
  {
    name: "private implementation repository name",
    pattern: new RegExp(`\\b${privateRepositoryFamily}\\b`, "i"),
    sample: privateRepositoryName,
  },
  {
    name: "private backend service identity",
    pattern: new RegExp(`\\b${privateServiceName}\\b`),
    sample: privateServiceName,
  },
  {
    name: "private backend protobuf package",
    pattern: new RegExp(`\\b${privatePackageName.replaceAll(".", "\\.")}\\b`),
    sample: privatePackageName,
  },
  {
    name: "retired private contract policy",
    pattern: new RegExp(`\\b${retiredPolicyName}\\b`, "i"),
    sample: retiredPolicyName,
  },
  {
    name: "retired private contract root variable",
    pattern: new RegExp(`\\b${retiredRootVariable}\\b`),
    sample: retiredRootVariable,
  },
  {
    name: "private repository provenance field",
    pattern: new RegExp(`\\b${privateSourceRepositoryKey}\\b`, "i"),
    sample: privateSourceRepositoryKey,
  },
  {
    name: "complete consumer-profile handoff",
    pattern: new RegExp(`\\b${retiredConsumerProfilesName}\\.json\\b`, "i"),
    sample: `${retiredConsumerProfilesName}.json`,
  },
  {
    name: "first-party application profile",
    pattern: new RegExp(
      `\\b(?:${terminalProfileName}[-_]${compassProfileName}|first[-_]party[-_]${terminalProfileName})`,
      "i",
    ),
    sample: [terminalProfileName, compassProfileName, "read", "v1"].join("_"),
  },
  {
    name: "first-party product implementation",
    pattern: new RegExp(
      `\\b(?:${terminalProductName}|${compassProductName})\\b`,
      "i",
    ),
    sample: terminalProductName,
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
      /\bhttps?:\/\/(?!(?:github\.com|codeload\.github\.com)\/jim-technologies\/(?:medallion-sdk|invariantprotocol)(?:[/?#.\s"'`)]|$))(?!(?:medallion\.jimtech\.io\/contracts\/public\/v1\/))[^"'\s)]+(?:jimtech|jim-technologies)[^"'\s)]*/i,
  },
];

checks.push(...retiredScopeChecks);

for (const check of checks) {
  if (check.sample !== undefined && !check.pattern.test(check.sample)) {
    throw new Error(`Invalid public-boundary pattern: ${check.name}`);
  }
}

const neutralPublicExample =
  "https://api.example.com medallion.connect.v1.MedallionConnectService";
for (const check of checks) {
  if (check.pattern.test(neutralPublicExample)) {
    throw new Error(
      `Public-boundary pattern rejects the supported API: ${check.name}`,
    );
  }
}

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
        if (
          check.name === "retired organization snake-case scope" &&
          isGeneratedReservedOrganizationName(file, line)
        ) {
          continue;
        }
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
    for (const child of readdirSync(path).sort()) {
      if (skippedPathParts.has(child)) continue;
      if (
        path.split(/[\\/]/).includes(".flox") &&
        generatedFloxPathParts.has(child)
      ) {
        continue;
      }
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

function isGeneratedReservedOrganizationName(file, line) {
  if (file === "proto/medallion/connect/v1/connect.proto") {
    return line.trim() === `reserved "${retiredOrganizationSnake}";`;
  }
  if (file === "go/gen/medallion/connect/v1/connect.pb.go") {
    return line.includes(`R\\x0f${retiredOrganizationSnake}`);
  }
  if (file === "python/src/medallion/connect/v1/connect_pb2.py") {
    return (
      line.startsWith(
        "DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(",
      ) && line.includes(`R\\x0f${retiredOrganizationSnake}`)
    );
  }
  return false;
}
