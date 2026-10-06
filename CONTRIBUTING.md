# Contributing

Thanks for your interest in StreamForge. Small, focused changes are easiest to review.

## Build and test

Requirements: Go 1.24+, `golangci-lint`; Docker for integration tests.

```bash
go mod download
```

Run the checks before opening a pull request:

```bash
gofmt -l .
go vet ./...
golangci-lint run
go test -race ./... -count=1
```

## Pull requests

1. Fork the repository and create a branch from `main`.
2. Keep each pull request to one logical change and add or update tests with it.
3. Use conventional commit messages (`feat:`, `fix:`, `docs:`, `test:`, `chore:`).
4. Make sure CI passes and describe what changed and why in the pull request.

Report security issues as described in [SECURITY.md](SECURITY.md), not in public issues.
