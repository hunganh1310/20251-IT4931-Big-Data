# Contributing to Air Quality Monitoring System

## Git Configuration

### Setting Up Your Git Identity

**IMPORTANT**: To ensure your contributions are properly attributed to your GitHub account, you must configure Git with a verified email address.

#### Option 1: Use your GitHub-verified email (Recommended)

```bash
git config user.name "Your Name"
git config user.email "your-verified-email@example.com"
```

Make sure the email address is added and verified in your [GitHub Email Settings](https://github.com/settings/emails).

#### Option 2: Use GitHub's noreply email

To keep your email private, you can use GitHub's noreply email:

```bash
git config user.name "YourGitHubUsername"
git config user.email "YOUR-USER-ID+YourGitHubUsername@users.noreply.github.com"
```

You can find your GitHub user ID in your [GitHub Email Settings](https://github.com/settings/emails).

### Why This Matters

GitHub uses the email address in your commits to link them to your GitHub account. If you use an unverified or placeholder email (like `name@example.com`), your contributions will not appear in:
- The repository's contributor graph
- Your GitHub profile's contribution activity
- The commit history linked to your account

### Checking Your Current Configuration

```bash
git config user.name
git config user.email
```

### Global vs Local Configuration

- **Global** (applies to all repositories on your machine):
  ```bash
  git config --global user.name "Your Name"
  git config --global user.email "your-email@example.com"
  ```

- **Local** (applies only to this repository):
  ```bash
  git config user.name "Your Name"
  git config user.email "your-email@example.com"
  ```

## Making Contributions

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Configure your git identity (see above)
4. Make your changes
5. Commit with descriptive messages (`git commit -m 'Add amazing feature'`)
6. Push to your branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Questions?

If you have any questions about setting up Git or contributing to this project, please open an issue or contact the maintainers.
