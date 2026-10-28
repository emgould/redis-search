---
name: pr-message-creation
description: Drafts and creates pull request titles and bodies for this repository with required sections for summary, impacted systems, frontend components, backend endpoints, manual test plan, and deployment checklist. Use when the user asks to create a PR, write PR copy, summarize branch work for GitHub, or run `gh pr create`.
---

# PR Message Creation

## Purpose

Use this skill when preparing a pull request for a completed body of work in this repository.

The PR message must clearly describe:

- what changed
- which systems were impacted
- which frontend components were changed, if applicable
- which backend endpoints were affected, if applicable
- how a tester should validate the work
- what deployment actions are required, if any

## Workflow

1. If the user wants an actual PR created, confirm the work is accepted and ready for PR creation.
2. Review the branch diff, staged and unstaged changes, and commit history for the full body of work.
3. Identify the impacted systems from the actual changes. Common examples:
   - Mobile
   - Firebase Function
   - Firebase Python Function
   - Firebase Rule
   - index or other aggregation/export surfaces
4. Determine whether frontend-specific and backend-specific sections are needed.
5. Draft a concise PR title and a structured markdown body using the rules below.
6. If asked to create the PR, use `gh pr create` with the drafted title and body.

## Title Rules

- The title must summarize the completed body of work, not a single file change.
- Keep it concise and specific.
- Prefer action-oriented phrasing such as `Add`, `Update`, `Refine`, `Fix`, or `Refactor`.
- Mention the primary feature area, flow, or subsystem.
- Do not include ticket prefixes unless the user or repository convention requires them.
- Do not use vague titles such as `updates`, `misc fixes`, or `work in progress`.

## Title Template

Use this default shape unless the repository history clearly indicates a different convention:

```text
[Action] [feature area or subsystem] for [user-facing or system outcome]
```

Examples:

- `Refine watch queue schedule card for clearer airing states`
- `Add public circle default follow handling for new memberships`
- `Fix training response persistence for backend-driven module flow`

## Required PR Sections

Always include these sections in the PR body:

### Summary

- Provide a short explanation of the body of work.
- Focus on the user-visible or system-level outcome, not a file-by-file changelog.
- Prefer 2-4 bullets.
- Lead with the primary outcome, then include important secondary changes.
- Each bullet should stand on its own and be understandable by a reviewer scanning quickly.

### Scope

- List the systems impacted by the change.
- Use concrete labels such as `Mobile`, `Firebase Function`, `Firebase Python Function`, `Firebase Rule`, or `index`.
- Do not list systems that were not actually touched.
- Keep this section short and normalized so reviewers can identify blast radius at a glance.

### Primary Frontend Changes

- Include this section only if frontend work was part of the change.
- Name the primary screens, components, hooks, or UI flows that changed.
- Prefer concrete identifiers over vague phrases.
- Do not include every touched component. Include only the primary surfaces a reviewer or tester should care about.

### Backend Endpoints Affected

- Include this section only if backend behavior changed.
- List the affected endpoints, callable functions, HTTP handlers, triggers, or public interfaces.
- If backend logic changed without altering an endpoint surface, name the entrypoints that are behaviorally affected.
- When possible, use the actual exported function or handler name.

### Test Plan

- Write this as a checklist for a human tester.
- Each item must describe:
  - the setup or entry condition
  - the action to take
  - the expected result to validate
- Prefer explicit step-by-step scenarios over generic statements like `Verify it works`.
- Cover the main success path first, then any high-risk regression or edge validation that materially matters.
- Do not include unit, lint, or automated checks unless they are relevant to the human validation the PR needs.

### Deployment Plan
- IF ANY OF YOUR CODE CHANGES ARE IN CODE THAT IN DEV/PRODUCT RESIDES IN THE CLOUD(GC, AWS, FIREBASE, ETC) THEN YOU ARE REQUIRED TO GENERATE THE MINIMAL REQUIRED COMMANDS THAT NEED TO BE EXECUTED TO PUT IN TO DEV/PRODUCTION
- Include a checklist of deployment actions only when the change requires an initial deploy or redeploy of Firebase or Python functions.
- Use the exact make command needed for each deploy target.
- List one checklist item per deploy target.
- Example:

```markdown
- [ ] `make deploy-only FILTER=get_trending`
```

- If no deployment action is required, state that explicitly.

## Writing Rules

- Do not invent impacted systems, components, endpoints, tests, or deployment actions.
- Do not claim validation that did not happen.
- Do not turn the PR body into a file inventory.
- Keep the summary high-signal and reviewer-friendly.
- Make the test plan executable by someone who did not implement the change.
- When frontend work exists, mention the primary UI surfaces changed.
- When backend work exists, mention the functional entrypoints affected.
- Omit sections that do not apply instead of filling them with placeholder text.
- If both frontend and backend work exist, include both conditional sections.
- If deployment is required for only part of the backend work, list only the affected deploy targets.
- If there are no deploy actions, the deployment section must say `No deployment actions required.`

## Default PR Body Template

Use this template and omit conditional sections that do not apply:

```markdown
## Summary
- [Concise outcome-oriented bullet]
- [Concise outcome-oriented bullet]

## Scope
- [Mobile]
- [Firebase Function]

## Primary Frontend Changes
- [Screen, component, or flow]
- [Screen, component, or flow]

## Backend Endpoints Affected
- [Endpoint or callable name]
- [Endpoint or callable name]

## Test Plan
- [ ] From [starting state], do [action], and verify [expected result].
- [ ] From [starting state], do [action], and verify [expected result].

## Deployment Plan
- [ ] `make deploy-only FILTER=example_function`
```

If no deployment is required, use:

```markdown
## Deployment Plan
No deployment actions required.
```

## Final Check

Before presenting or creating the PR, verify:

- the title matches the actual body of work
- the scope only lists touched systems
- frontend and backend sections appear only when relevant
- the test plan is actionable and specific
- deployment commands are included only when required
