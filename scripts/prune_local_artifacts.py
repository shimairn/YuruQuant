from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_KEEP_DIRS = (
    'reports/liquid_top10_dual_core',
    'reports/liquid_top20_dual_core_20260309',
    'reports/minimal_stable_top10_v2',
    'reports/dual_branch_effectiveness_v3',
    'reports/grid_protected_top10_3m',
    'research/cost_profiles',
)


@dataclass(frozen=True)
class PrunePlan:
    keep_dirs: tuple[Path, ...]
    missing_canonical_dirs: tuple[Path, ...]
    prune_dirs: tuple[Path, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Prune local report clutter while preserving canonical YuruQuant baselines.')
    parser.add_argument('--repo-root', default=str(REPO_ROOT))
    parser.add_argument('--dry-run', action='store_true', help='Preview only. This is the default mode.')
    parser.add_argument('--execute', action='store_true', help='Delete prunable directories. Refuses to run when canonical roots are missing.')
    parser.add_argument('--include-gmcache', action='store_true', help='Also delete the local gmcache directory.')
    return parser.parse_args()


def canonical_keep_dirs(repo_root: Path) -> tuple[Path, ...]:
    return tuple((repo_root / relative).resolve() for relative in CANONICAL_KEEP_DIRS)


def build_prune_plan(repo_root: Path, include_gmcache: bool = False) -> PrunePlan:
    keep_dirs = canonical_keep_dirs(repo_root)
    missing_canonical_dirs = tuple(path for path in keep_dirs if path.parts[-2] == 'reports' and not path.exists())
    keep_report_dirs = {path for path in keep_dirs if path.parts[-2] == 'reports'}
    prune_dirs: list[Path] = []

    reports_root = (repo_root / 'reports').resolve()
    if reports_root.exists():
        for child in sorted(reports_root.iterdir()):
            if not child.is_dir():
                continue
            if child.resolve() not in keep_report_dirs:
                prune_dirs.append(child.resolve())

    if include_gmcache:
        gmcache_root = (repo_root / 'gmcache').resolve()
        if gmcache_root.exists():
            prune_dirs.append(gmcache_root)

    return PrunePlan(
        keep_dirs=keep_dirs,
        missing_canonical_dirs=missing_canonical_dirs,
        prune_dirs=tuple(prune_dirs),
    )


def apply_prune_plan(plan: PrunePlan, dry_run: bool = True) -> list[Path]:
    if not dry_run and plan.missing_canonical_dirs:
        missing = ', '.join(path.as_posix() for path in plan.missing_canonical_dirs)
        raise RuntimeError(f'cannot prune while canonical report roots are missing: {missing}')

    deleted: list[Path] = []
    for path in plan.prune_dirs:
        if dry_run:
            continue
        if path.exists():
            shutil.rmtree(path)
            deleted.append(path)
    return deleted


def render_plan(plan: PrunePlan, dry_run: bool) -> str:
    lines = [
        'mode=' + ('dry-run' if dry_run else 'execute'),
        'keep_dirs:',
    ]
    lines.extend(f'  - {path.as_posix()}' for path in plan.keep_dirs)
    lines.append('missing_canonical_dirs:')
    if plan.missing_canonical_dirs:
        lines.extend(f'  - {path.as_posix()}' for path in plan.missing_canonical_dirs)
    else:
        lines.append('  - none')
    lines.append('prune_dirs:')
    if plan.prune_dirs:
        lines.extend(f'  - {path.as_posix()}' for path in plan.prune_dirs)
    else:
        lines.append('  - none')
    return '\n'.join(lines)


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    dry_run = not bool(args.execute)
    plan = build_prune_plan(repo_root, include_gmcache=bool(args.include_gmcache))
    print(render_plan(plan, dry_run))
    try:
        deleted = apply_prune_plan(plan, dry_run=dry_run)
    except RuntimeError as exc:
        print(str(exc))
        return 2
    if not dry_run:
        print('deleted_dirs:')
        if deleted:
            for path in deleted:
                print(f'  - {path.as_posix()}')
        else:
            print('  - none')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
