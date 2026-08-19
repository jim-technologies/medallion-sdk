import assert from "node:assert/strict";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

import {
  buildThirdPartyLicenseInventory,
  checkThirdPartyLicenses,
  writeThirdPartyLicenses,
} from "./generate_third_party_licenses.mjs";

test("inventory contains exactly packages contributing to the CJS output", () => {
  withFixture((fixture) => {
    fixture.package("z-package", "2.0.0", "MIT", {
      LICENSE: "z package full MIT terms\n",
      "index.js": "export const z = true;\n",
    });
    fixture.package("a-package", "1.0.0", "BSD-3-Clause", {
      LICENSE: "a package full BSD terms\n",
      "index.js": "export const a = true;\n",
    });
    fixture.package("unused-package", "9.0.0", "MIT", {
      LICENSE: "unused license\n",
      "index.js": "export const unused = true;\n",
    });
    fixture.metafile({
      "node_modules/z-package/index.js": 10,
      "node_modules/a-package/index.js": 20,
      "node_modules/unused-package/index.js": 0,
    });

    const inventory = buildThirdPartyLicenseInventory(fixture.options());
    assert.deepEqual(
      inventory.packages.map((item) => item.id),
      ["a-package@1.0.0", "z-package@2.0.0"],
    );
    assert.match(inventory.text, /a package full BSD terms/);
    assert.match(inventory.text, /z package full MIT terms/);
    assert.doesNotMatch(inventory.text, /unused-package/);
  });
});

test("inventory check rejects missing and stale generated output", () => {
  withFixture((fixture) => {
    fixture.package("example", "1.0.0", "MIT", {
      LICENSE: "complete MIT terms\n",
      "index.js": "export const example = true;\n",
    });
    fixture.metafile({ "node_modules/example/index.js": 10 });

    assert.throws(
      () => checkThirdPartyLicenses(fixture.options()),
      /THIRD_PARTY_LICENSES\.txt is missing/,
    );
    writeThirdPartyLicenses(fixture.options());
    assert.equal(checkThirdPartyLicenses(fixture.options()).packages.length, 1);

    writeFileSync(
      fixture.outputPath,
      `${readFileSync(fixture.outputPath, "utf8")}stale entry\n`,
    );
    assert.throws(
      () => checkThirdPartyLicenses(fixture.options()),
      /THIRD_PARTY_LICENSES\.txt is stale/,
    );
  });
});

test("packages without license files use auditable installed sources", () => {
  withFixture((fixture) => {
    fixture.package("apache-donor", "1.0.0", "Apache-2.0", {
      LICENSE: "Apache License\nVersion 2.0\ncomplete canonical terms\n",
      "index.js": "export const donor = true;\n",
    });
    fixture.package(
      "dual-license-package",
      "3.0.0",
      "(Apache-2.0 AND BSD-3-Clause)",
      {
        "index.js": `// Copyright 2026 Example. All rights reserved.
//
// Redistribution and use in source and binary forms are permitted.
// Neither the name of Example nor its contributors may be used to endorse.
// THIS SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY.
export const dual = true;
`,
      },
    );
    fixture.metafile({
      "node_modules/apache-donor/index.js": 10,
      "node_modules/dual-license-package/index.js": 10,
    });

    const { text } = buildThirdPartyLicenseInventory(fixture.options());
    assert.match(
      text,
      /apache-donor@1\.0\.0\/LICENSE \(canonical Apache-2\.0 text for dual-license-package@3\.0\.0\)/,
    );
    assert.match(
      text,
      /dual-license-package@3\.0\.0\/index\.js \(embedded BSD-3-Clause notice\)/,
    );
    assert.match(text, /complete canonical terms/);
    assert.match(text, /Redistribution and use in source and binary forms/);
  });
});

test("missing package license material fails closed", () => {
  withFixture((fixture) => {
    fixture.package("missing-license", "1.0.0", "MIT", {
      "index.js": "export const missing = true;\n",
    });
    fixture.metafile({ "node_modules/missing-license/index.js": 10 });

    assert.throws(
      () => buildThirdPartyLicenseInventory(fixture.options()),
      /unsupported expression MIT/,
    );
  });
});

function withFixture(callback) {
  const root = mkdtempSync(join(tmpdir(), "medallion-license-test-"));
  try {
    const metafilePath = join(root, "dist", "metafile-cjs.json");
    const outputPath = join(root, "dist", "THIRD_PARTY_LICENSES.txt");
    callback({
      outputPath,
      options() {
        return { sdkRoot: root, metafilePath, outputPath };
      },
      package(name, version, license, files) {
        const packageRoot = join(root, "node_modules", ...name.split("/"));
        mkdirSync(packageRoot, { recursive: true });
        writeFileSync(
          join(packageRoot, "package.json"),
          `${JSON.stringify({ name, version, license }, null, 2)}\n`,
        );
        for (const [relative, contents] of Object.entries(files)) {
          const path = join(packageRoot, relative);
          mkdirSync(dirname(path), { recursive: true });
          writeFileSync(path, contents);
        }
      },
      metafile(inputs) {
        mkdirSync(dirname(metafilePath), { recursive: true });
        const metadataInputs = Object.fromEntries(
          Object.keys(inputs).map((input) => [
            input,
            { bytes: 1, imports: [] },
          ]),
        );
        writeFileSync(
          metafilePath,
          JSON.stringify({
            inputs: metadataInputs,
            outputs: {
              "dist/index.cjs": {
                inputs: Object.fromEntries(
                  Object.entries(inputs).map(([input, bytesInOutput]) => [
                    input,
                    { bytesInOutput },
                  ]),
                ),
              },
            },
          }),
        );
      },
    });
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
}
