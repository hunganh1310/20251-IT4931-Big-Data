# Fixing Contributor Attribution for Hieu1607

## Problem

User Hieu1607's commits are not showing up in the GitHub contributor graph despite being merged into the main branch via PR #3. This is because the commits were authored with the email address `hieu@example.com`, which is not associated with the Hieu1607 GitHub account.

## Understanding the Issue

GitHub determines commit attribution based on the email address used in the commits. When a commit is authored with an email that is not verified in any GitHub account, GitHub cannot link it to a user profile.

### Current Situation
- **Commits in main branch**: 7 commits by Hieu1607
- **Email used**: `hieu@example.com` (placeholder/unverified)
- **GitHub account**: Hieu1607 (user ID: 164389040)
- **Result**: Commits are in the repository but not attributed to the GitHub account

## Solutions

### Solution 1: Add Email to GitHub Account (RECOMMENDED)

**For Hieu1607 to do:**

1. Go to [GitHub Email Settings](https://github.com/settings/emails)
2. Click "Add email address"
3. Add the email: `hieu@example.com`
4. Verify the email by clicking the confirmation link sent to that email
5. Wait 24 hours for GitHub to reindex the commits

**Pros:**
- No changes to repository needed
- Preserves commit history exactly as-is
- Automatic fix once email is verified

**Cons:**
- Requires access to the email address
- If it's a fake/placeholder email, this won't work

### Solution 2: Mailmap File (IMPLEMENTED)

A `.mailmap` file has been added to map the old email to the correct GitHub email:

```
Hieu1607 <164389040+Hieu1607@users.noreply.github.com> <hieu@example.com>
```

**Pros:**
- Works for local git commands (`git log`, `git shortlog`, etc.)
- No history rewriting needed
- Good for documentation

**Cons:**
- Does NOT fix GitHub's contributor graph
- GitHub's web interface may not respect `.mailmap` for contributor attribution

### Solution 3: History Rewriting (NOT RECOMMENDED)

Rewrite all commits to use the correct email. This is **not recommended** because:
- Forces everyone to re-clone or rebase
- Can break existing PRs and forks
- Requires force-push which is disruptive
- Only use as last resort with team agreement

## Current Status

✅ `.mailmap` file added for local attribution
✅ `CONTRIBUTING.md` created to prevent future issues
✅ Documentation updated

**Next Steps for Hieu1607:**

### IMMEDIATE ACTION REQUIRED:

**Option A: Add the email to your GitHub account (If you have access to hieu@example.com)**
1. Go to https://github.com/settings/emails
2. Add `hieu@example.com`
3. Verify it via the confirmation email
4. Wait 24-48 hours for GitHub to reindex

**Option B: If hieu@example.com is not a real email:**
1. Contact the repository maintainer
2. Discuss rewriting commit history as a team
3. All team members must agree and coordinate the rebase
4. This will require force-pushing to main (disruptive but necessary)

**For Future Commits (IMPORTANT):**
- Configure git BEFORE making commits:
  ```bash
  git config user.name "Hieu1607"
  git config user.email "164389040+Hieu1607@users.noreply.github.com"
  ```
- See CONTRIBUTING.md for detailed setup instructions

## Verification

After implementing a solution, verify with:

```bash
# Local verification (with .mailmap)
git shortlog -sn main

# GitHub verification
# Check the repository's contributor graph on GitHub
```

## Prevention

All contributors should configure git properly before committing:

```bash
# Use GitHub noreply email
git config user.email "YOUR-USER-ID+YourUsername@users.noreply.github.com"

# Or use your verified email
git config user.email "your-verified-email@domain.com"
```

See `CONTRIBUTING.md` for detailed instructions.
