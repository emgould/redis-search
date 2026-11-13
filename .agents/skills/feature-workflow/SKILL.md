---
name: feature-workflow
description: >-
  Runs a MediaCircle feature or fix from request through pull request:
  plan, plan review, build, senior review, deploy-file setup, and PR.
  Prepares bin/batch-deploy.sh and never deploys. Use only when the user
  invokes /feature-workflow.
disable-model-invocation: true
---

# Feature workflow

The parent orchestrates. It does not plan, review, edit product code, or open the PR. Each stage is one local `generalPurpose` subagent. Wait for it to finish before starting the next. Never run stages in parallel. Never use a cloud subagent.

## Never deploy

No stage deploys. The parent and every subagent are forbidden to run a deploy, release, or publish command. That includes `bin/batch-deploy.sh`, `firebase deploy`, `make deploy-only`, `make deploy-rules`, `gcloud`, EAS, Expo, hosting, and store submission. Editing `bin/batch-deploy.sh` and `firebase/functions/thin-entrypoints.json` is setup for the user. Only the user deploys, later, outside this workflow. Do not tell the user that functions need to be deployed now. The PR may record the command the user can run later. Recording that command is not permission to run it.

The user explicitly requested these models by invoking this skill:

| Stage | Model slug |
| --- | --- |
| Plan, build, pr | `grok-4.7-medium-fast` |
| Plan review, senior review, deployment review | `gpt-5.6-luna-low` |

Pass that slug as the subagent `model`. Do not use `inherit`. Do not substitute another model. If the slug is rejected, stop and report the allowed list.

## Invoke

```text
/feature-workflow Add a Watch List filter for episodes airing this week.
```

The text after the command is the request. If it is missing, ask for it and stop. A later chat can run the same command with a different request.

## Branch confirmation

Do this before any stage. A dirty tree is not a stop. Being off `main` is not a stop. Do not stash, reset, restore, or discard files.

- On `main`: create a branch from `HEAD`. A fix uses `fix/<kebab-name>`. A feature uses `feature/<kebab-name>`. If the request is not clearly one or the other, ask once, then create the branch. Uncommitted files stay uncommitted and move with the new branch.
- Not on `main`, with uncommitted files: ask whether the user will commit them or the agent should. Wait. If the user commits, continue only after the tree is clean. If the agent commits, commit those existing files with a message from that diff. Do not commit secrets.
- Not on `main`, once the tree is clean: ask whether to keep this branch or create a new one from current `HEAD`, using the same `fix/` or `feature/` prefix rule.

Tell later stages the confirmed branch. They must not create another branch.

## Stage order

Pass the original request, the Never deploy section, and every prior stage output into the next prompt. Tell each subagent to read the named skill or rule and follow it. Require the final line `VERDICT: <token>` so the parent can route.

1. **Plan.** Model `grok-4.7-medium-fast`. Read-only. Follow the Planning Standards in `.cursor/skills/build/SKILL.md`. Return the plan. Do not edit files, create a branch, or implement. If the request is too thin to plan, `VERDICT: NEEDS_USER`. Otherwise `VERDICT: PLAN`.
2. **Plan review.** Model `gpt-5.6-luna-low`. Read `~/.cursor/skills/plan-review/SKILL.md`. Return the plan build should use. `VERDICT: APPROVE`, `DISAPPROVE`, or `NEEDS_USER`.
3. **Build.** Model `grok-4.7-medium-fast`. Read `.cursor/skills/build/SKILL.md`. Implement the reviewed plan on the confirmed branch. Skip that skill's branch-creation steps. Do not open a PR. `VERDICT: COMPLETE` or `BLOCKED`.
4. **Senior review.** Model `gpt-5.6-luna-low`. Read `~/.cursor/skills/sr-review/SKILL.md`. Identify blocking objections and fix the ones that stay inside the approved plan, then re-check. Do not make a fix that would drift from or shift that plan. `VERDICT: APPROVE`, `DISAPPROVE`, or `NEEDS_USER`. `NEEDS_USER` includes the objection and the proposed change.
5. **Deployment review.** Model `gpt-5.6-luna-low`. Run only after senior review is `APPROVE`. Read `.cursor/rules/cursor-pr-deployment-rule.mdc`. Edit `bin/batch-deploy.sh` so the file is the complete deploy definition for this PR. This stage writes the file. It never executes it.
6. **PR.** Model `grok-4.7-medium-fast`. Read `.cursor/skills/pr/SKILL.md` and `.cursor/skills/pr-message-creation/SKILL.md`. Copy the reviewed deploy targets into the Deployment section. Do not add or remove deploy targets. Set `deploy_mobile_ota` in `bin/batch-deploy.sh` from the OTA answer the parent passes in. That edit is file setup. Commit, push, and open the PR. Do not run the deploy script. Return the PR URL. `VERDICT: PR` or `BLOCKED`.

If `~/.cursor/skills/plan-review/SKILL.md` or `~/.cursor/skills/sr-review/SKILL.md` is missing, stop before that stage.

## Deployment review contract

This stage prepares the deploy file. It does not deploy. Pass that prohibition in the subagent prompt.
If firebase project:
- Every changed Firebase export is in `functions=()` or `python_functions=()`. Every name in those lists is a change from this branch. Remove unrelated names.
- If in project that uses firebase functions then register each TypeScript name in `firebase/functions/thin-entrypoints.json` before listing it. The value is the module that already exports that name. Do not call `setGlobalOptions` in that module. A `functions=()` batch is entirely registered or entirely unregistered. Do not commit `lib/thin/`.
- Set rules, indexes, hosting, backfills, cache rolls, and other flags only when this diff requires them. Leave `deploy_mobile_ota` unchanged.

- An application-only diff leaves the script unmodified. The verdict says why in one sentence tied to the diff.
- A cache-version bump uses the shared version registry. If the version key is not obvious, return `NEEDS_USER`. Do not change a cache prefix.
- A script change that would deploy something the approved plan did not include, or drop a deploy the plan required, is `NEEDS_USER`.
- Otherwise `VERDICT: APPROVE` and list the exact targets the PR section must copy. `VERDICT: DISAPPROVE` when the script cannot be made to match the diff.

## Stop gates

- Plan `NEEDS_USER`: stop and ask. Do not review or build.
- Plan review is not `APPROVE`: stop and show the verdict. `NEEDS_USER` is the question for the user. Do not build. Plan `PLAN` continues to plan review.
- Build is `BLOCKED`: stop. Do not review or open a PR.
- Senior review `NEEDS_USER`: stop and ask. Do not commit or open a PR until the user answers. A fix inside the approved plan is not a stop.
- Senior review `DISAPPROVE`: stop. Do not commit or open a PR.
- Senior review `APPROVE`: run deployment review. Do not ask the OTA question yet.
- Deployment review `NEEDS_USER` or `DISAPPROVE`: stop and show the verdict. Do not commit or open a PR until the user answers.
- Deployment review `APPROVE`: ask whether to roll the OTA version. Pass that answer into the PR stage so it does not ask again. If the answer is yes, the PR stage follows `.cursor/skills/roll-ota/SKILL.md` before the PR commit. Invoking this workflow authorizes the commit and PR only after this approval.

## Parent report

After each stage, tell the user the stage name, model, and verdict. After the PR stage, return the PR URL. Tell the user the deploy file is prepared and that this workflow did not deploy.
