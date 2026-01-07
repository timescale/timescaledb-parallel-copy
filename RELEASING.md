# Release Process

This project uses [release-please](https://github.com/googleapis/release-please) to automate releases.

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/) format:

- `fix:` → patch release (0.12.0 → 0.12.1)
- `feat:` → minor release (0.12.0 → 0.13.0)
- `feat!:` or `BREAKING CHANGE:` → major release (0.12.0 → 1.0.0)
- `chore:`, `docs:`, `test:`, `ci:` → no release

```bash
git commit -m "fix: handle null values in CSV"
git commit -m "feat: add JSON output support"
git commit -m "feat!: change CLI flags

BREAKING CHANGE: renamed --file to --input"
```

## How Releases Work

1. Push commits to `main` using conventional commit messages
2. Release-please opens a PR with version bump and changelog
3. Review and merge the release PR
4. GitHub release and git tag are created automatically
