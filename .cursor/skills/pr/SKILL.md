---
name: pr
description: Create a PR for the current branch
disable-model-invocation: true
---

# PR Creation

## Purpose

Work has been accepted by the user and wants the code finalized, any untracked changes or modified files to be committed to the branch and then to create a pr for review.

## Files
- All files contained in the branch

## Workflow
1. Confirm the repo is not on `main`.
2. Ask the user if they would like to roll the ota version. if so then follow the /roll-ota skill @.cursor/skills/roll-ota/SKILL.md
3. Follow and run the /Users/ericgould/dev/mediacircle/.cursor/rules/cursor-pr-deployment-rule.mdc

