import { defineConfig } from "tsup";

const common = {
  entry: ["src/index.ts"],
  target: "es2024",
  sourcemap: true,
  splitting: false,
  treeshake: true,
  outExtension({ format }) {
    return {
      js: format === "esm" ? ".js" : ".cjs",
    };
  },
} satisfies Parameters<typeof defineConfig>[0];

export default defineConfig([
  {
    ...common,
    format: ["esm"],
    clean: true,
  },
  {
    ...common,
    format: ["cjs"],
    clean: false,
    metafile: true,
    noExternal: ["@bufbuild/protobuf", "@jim-technologies/invariant-protocol"],
  },
]);
