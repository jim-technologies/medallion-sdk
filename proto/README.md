# Proto Contracts

This directory contains minimal public client-facing route/spec metadata and
vendored public protobuf contracts used by SDK compatibility tests, generated
Go/Python protocol bindings, and the TypeScript invariantprotocol descriptor.
These contracts are bounded initial SDK subsets, not the entire administration
or internal surface of each service. Every shared message, field, enum, RPC
signature, and validation constraint must remain identical to its canonical
service. Additive canonical response fields used by an included RPC must also
be vendored so strict descriptor-backed decoding remains forward-compatible.

`medallion-*.descriptor.binpb` files are generated from the vendored public proto contracts and embedded into `src/*-descriptor.ts` for TypeScript runtime use:

```sh
make proto-bindings
make proto-descriptor
make generated-check
```

SDK consumers do not need Buf or Flox to install or use the package.

Do not add private repository references, internal-only contracts, secrets, or deployment details here.
