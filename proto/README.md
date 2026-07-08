# Proto Contracts

This directory contains minimal public client-facing route/spec metadata and vendored public protobuf contracts used by SDK compatibility tests, generated Go/Python protocol bindings, and the TypeScript invariantprotocol descriptor.

`medallion-*.descriptor.binpb` files are generated from the vendored public proto contracts and embedded into `src/*-descriptor.ts` for TypeScript runtime use:

```sh
corepack pnpm proto:descriptor
```

SDK consumers do not need Buf or Flox to install or use the package.

Do not add private repository references, internal-only contracts, secrets, or deployment details here.
