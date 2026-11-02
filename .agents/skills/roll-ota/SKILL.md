---
name: roll-ota
description: Roll the MediaCircle mobile OTA version in apps/mobile/package.json and apps/mobile/app.config.ts, validate both values match, and create the OTA version commit. Use when the user says roll-ota, asks to roll the OTA version, or asks to bump the mobile OTA release version.
disable-model-invocation: true
---

# Roll OTA

## Purpose

Roll the mobile OTA release version in both required files and commit only that version change.

## Files

- `apps/mobile/package.json`
- `apps/mobile/app.config.ts`

## Version Rule

Both files must contain the same version before and after the change.
Treat `expo.runtimeVersion` without its `-native` suffix as the immutable native
base version for the OTA sequence. Never modify `runtimeVersion` during an OTA
roll.

Required MediaCircle OTA format:

```text
MAJOR.MINOR.PATCH-ota.NUMBER
```

If both files contain the native base version without an OTA suffix, initialize
the sequence at `ota.1`.

Example:

```text
version: 1.0.5 + runtimeVersion: 1.0.5-native -> version: 1.0.5-ota.1
```

If both files already use the OTA format, increment only `NUMBER`.

Example:

```text
1.0.5-ota.12 -> 1.0.5-ota.13
```

The version base must match the `runtimeVersion` base. If it does not, or if the
files use any other format, stop and ask the user which exact next version to
use. Never increment the native patch, minor, or major version during an OTA
roll.

## Workflow

1. Confirm the repo is not on `main`.
2. Read `apps/mobile/package.json` and `apps/mobile/app.config.ts`.
3. Extract:
   - `version` in `apps/mobile/package.json`
   - `expo.version` in `apps/mobile/app.config.ts`
   - `expo.runtimeVersion` in `apps/mobile/app.config.ts`
4. Stop if the two current version values differ or their native base does not
   match `runtimeVersion`.
5. Compute the next version using the Version Rule.
6. Update only those two version values.
7. Validate:
   - The two files contain the same new version.
   - `runtimeVersion` is unchanged.
   - The new version's native base matches `runtimeVersion`.
   - `git diff -- apps/mobile/package.json apps/mobile/app.config.ts` shows only the intended version changes.
   - No package manager install/remove command was run.
   - No Expo command was run.
8. Run the repository's normal lightweight validation for edited files if available without running Expo or install commands.
9. Commit only these two files with:

```text
chore(mobile): roll OTA version to VERSION
```
 run the commit with --no-verify
 
10. push the changes to the remote
Replace `VERSION` with the new version.

## Guardrails

- Never run Expo commands.
- Never run package install or remove commands.
- Never edit lockfiles.
- Never commit on `main`.
- Never include unrelated files in the commit.
- If unrelated working tree changes exist, leave them untouched and commit only the two OTA version files.
- If validation fails, stop and report the failure instead of committing.
