# Repository Bootstrap Guide (Greenfield)

Dieses Dokument bootstrapt ein **neues, separates Repository** für die moderne MonteCarlo-App.

## 1) Neues GitHub-Repository anlegen

Empfohlener Name:
- `montecarlo-risk-platform`

Empfohlene Einstellungen:
- Visibility: private
- Initialize with README: **no** (wir pushen lokal)
- Default branch: `main`

## 2) Lokales Zielverzeichnis erstellen

Beispiel:

```powershell
mkdir C:\GitHub\montecarlo-risk-platform
cd C:\GitHub\montecarlo-risk-platform
```

## 3) Projektstruktur (Monorepo) übernehmen

Empfohlene Struktur:
- `apps/web` (Next.js Frontend)
- `apps/api` (FastAPI Backend)
- `packages/contracts` (OpenAPI/DTOs, Client)
- `packages/domain` (pure Simulationslogik)
- `infra` (Docker, Compose, IaC)
- `.github/workflows` (CI/CD)

## 4) Git-Initialisierung

```powershell
git init
git checkout -b main
git add .
git commit -m "chore: bootstrap monorepo"
```

Remote verbinden und pushen:

```powershell
git remote add origin <YOUR_GITHUB_URL>
git push -u origin main
```

## 5) Branch- und Repo-Schutz (GitHub)

Für `main`:
- Require pull request before merging
- Require approvals: 1-2
- Require status checks to pass
- Require up-to-date branch before merge
- Dismiss stale approvals
- Include administrators

Zusätzlich:
- Disable force push
- Disable delete branch

## 6) Environments und Secrets

GitHub Environments:
- `dev`
- `staging`
- `prod`

Empfohlene Secrets/Vars:
- `DATABASE_URL`
- `REDIS_URL`
- `JWT_SECRET`
- `SENTRY_DSN` (optional)
- `NEXT_PUBLIC_API_BASE_URL`

## 7) CI Mindeststandard

Checks pro Pull Request:
- Lint
- Tests
- Build

Optional (empfohlen):
- Security scan (`pip-audit`, `npm audit`, `trivy`)
- SBOM erzeugen

## 8) Release-Strategie

Empfehlung:
- Trunk-based development
- SemVer Tags (`v0.1.0`, `v0.2.0`)
- Changelog via Conventional Commits

## 9) Definition of Done (Bootstrap)

Bootstrap gilt als fertig, wenn:
- Repo erstellt und initial gepusht
- Branch protection aktiv
- CI auf PR aktiv und grün
- `dev` Environment vorhanden
- Basisstruktur (`apps`, `packages`, `infra`) im Repo

## 10) Nächster praktischer Schritt

Nach dem Bootstrap direkt umsetzen:
1. `apps/api` minimal FastAPI (`/health`)
2. `apps/web` minimal Next.js (`/health` page)
3. `packages/contracts` mit erstem API-Client
4. CI so erweitern, dass beide Apps gebaut/getestet werden
