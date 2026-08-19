# Changelog

All notable changes to the Medallion SDK are documented in this file. Every
language SDK shares the repository-root `VERSION` and ships together from one
annotated `vX.Y.Z` tag.

## [Unreleased]

- Adopt the jim-technologies open-source Makefile contract
  (`MAKEFILE-CONTRACT.md`): the gate verb is `make validate`, formatting is
  `make fmt`, schema regeneration is `make generate`, and `make release` is a
  fail-closed stub while distribution stays git-install based.
- Narrow the SDK to the four customer-ingestion RPCs and vendor a sanitized,
  attested external-ingestion contract with offline drift checks.
- Scope ingestion credentials to an immutable workspace selected at client
  construction.

## [0.1.1] - 2026-07-18

- Fix release validation for the unified root tag.

## [0.1.0] - 2026-07-17

- First unified release: TypeScript, Go, and Python SDKs ship together from
  one root tag with lockstep versions.
