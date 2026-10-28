---
name: build
description: Plans and implements MediaCircle repo changes with senior React Native and Firebase engineering discipline. Use when the user invokes /build, asks to use the build skill, or requests feature or fix implementation planning and execution.
disable-model-invocation: true
---

# Build

## Purpose

Plan and implement changes in this repository with senior engineering judgment across React Native, Expo, Firebase Functions, and shared backend code.

## Required Workflow

1. Check the current git branch and working tree before planning or editing.
2. Determine the state of the repo. If not on `main` and you have not contributed files to this branch yet, ask whether the build should be done as a commit on this already-in-progress branch or wait for the user to get the repo on `main`.
3. If on clean `main`, create a new branch. Use a `feature/` prefix for features and a `fix/` prefix for fixes.
4. Build the implementation plan as a senior engineer, React Native expert, and Firebase expert.
5. Follow existing repo patterns before introducing new structure.
6. Keep files within the repo's component and file size discipline. If a file is already large, prefer a scoped extraction plan instead of adding more responsibilities to it.
7. Do not introduce new dependencies, libraries, Expo components, or package manager changes unless they are explicitly called out in the approved plan.
8. If implementing new Firebase functionality, include required test coverage and get the tests passing before considering the work code-complete.
9. If changes affect the frontend, run TypeScript validation so the app is not left in a broken state.
10. Run the repo's committed-code lint check with `yarn check --fix` before final review.
11. Perform a senior engineer review of the final diff and state whether the work is approved for PR.

## Planning Standards

- Identify the user-facing behavior, backend behavior, and data contract changes.
- Call out Firebase Function, Firestore, security rule, cache, and deployment-script implications when relevant.
- Identify files likely to change and any files that should not be touched.
- State whether new tests are required. New Firebase functionality requires tests.
- State whether new dependencies are required. If they are not explicitly part of the plan, do not add them.
- Keep the plan scoped to the requested work and defer unrelated cleanup to future PR suggestions.

## Implementation Guardrails

- Prefer local repo helpers, existing service boundaries, and established UI/component patterns.
- Keep React Native UI, hooks, services, types, and Firebase logic separated by responsibility.
- Avoid expanding large components unless the requested change cannot be completed safely elsewhere.
- Do not run Expo commands.
- Do not install packages or modify dependency manifests unless explicitly approved.
- Do not deploy anything.

## Verification

Use verification proportional to the touched surfaces:

- Frontend TypeScript changes: run the repo's TypeScript check for the affected app/package.
- Firebase functionality changes: run the relevant Firebase tests and ensure they pass.
- Committed-code readiness: run `yarn check --fix`.
- After verification, review the diff as a senior engineer and explicitly approve or disapprove PR readiness.

## Final Response

Summarize:

- What changed.
- What validation ran and whether it passed.
- Any unresolved requirements, risks, or follow-up questions.
- Senior engineer PR decision: approved or disapproved.
