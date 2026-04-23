from __future__ import annotations

import ast
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = REPO_ROOT / "CODEBASE_UML.md"
EXCLUDED_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "tests",
}


@dataclass
class CallableInfo:
    name: str
    signature: str
    returns: str
    line: int
    async_: bool = False


@dataclass
class ClassInfo:
    name: str
    bases: list[str]
    line: int
    methods: list[CallableInfo] = field(default_factory=list)


@dataclass
class ModuleInfo:
    path: Path
    imports: list[str]
    functions: list[CallableInfo]
    classes: list[ClassInfo]


def _is_included_python_file(path: Path) -> bool:
    if path.suffix != ".py":
        return False
    rel_parts = path.relative_to(REPO_ROOT).parts
    return not any(part in EXCLUDED_DIRS or part.startswith(".") for part in rel_parts)


def _unparse(node: ast.AST | None) -> str:
    if node is None:
        return "Any"
    try:
        return ast.unparse(node)
    except Exception:
        return "Any"


def _format_arg(arg: ast.arg, default: ast.AST | None = None) -> str:
    annotation = _unparse(arg.annotation)
    text = arg.arg if annotation == "Any" else f"{arg.arg}: {annotation}"
    if default is not None:
        text += f" = {_unparse(default)}"
    return text


def _callable_info(node: ast.FunctionDef | ast.AsyncFunctionDef) -> CallableInfo:
    args = node.args
    defaults = [None] * (len(args.args) - len(args.defaults)) + list(args.defaults)
    parts: list[str] = []
    parts.extend(
        _format_arg(arg, default) for arg, default in zip(args.args, defaults, strict=True)
    )
    if args.vararg is not None:
        parts.append("*" + _format_arg(args.vararg))
    elif args.kwonlyargs:
        parts.append("*")
    parts.extend(
        _format_arg(arg, default)
        for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True)
    )
    if args.kwarg is not None:
        parts.append("**" + _format_arg(args.kwarg))
    return CallableInfo(
        name=node.name,
        signature=f"{node.name}({', '.join(parts)})",
        returns=_unparse(node.returns),
        line=node.lineno,
        async_=isinstance(node, ast.AsyncFunctionDef),
    )


def _imports(tree: ast.Module) -> list[str]:
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".", maxsplit=1)[0])
    return sorted(names)


def _module_info(path: Path) -> ModuleInfo:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    functions: list[CallableInfo] = []
    classes: list[ClassInfo] = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            functions.append(_callable_info(node))
        elif isinstance(node, ast.ClassDef):
            class_info = ClassInfo(
                name=node.name,
                bases=[_unparse(base) for base in node.bases],
                line=node.lineno,
            )
            for child in node.body:
                if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                    class_info.methods.append(_callable_info(child))
            classes.append(class_info)
    return ModuleInfo(
        path=path.relative_to(REPO_ROOT),
        imports=_imports(tree),
        functions=functions,
        classes=classes,
    )


def _mermaid_overview() -> str:
    return """```mermaid
flowchart TD
    Main[main.py / runner scripts] --> Experiment[ReplayExperiment or ParameterSearchExperiment]
    Experiment --> Backtest[PredictionMarketBacktest]
    Backtest --> Registry[data_sources.registry]
    Registry --> Adapter[HistoricalReplayAdapter]
    Adapter --> Loader[Vendor loader: Kalshi / Polymarket / PMXT / Telonex]
    Loader --> Records[LoadedReplay records + instrument]
    Records --> Engine[Nautilus BacktestEngine]
    Engine --> Strategy[Strategy configs / LongOnlyPredictionMarketStrategy]
    Engine --> Artifacts[Artifacts, reports, summary series]
    Artifacts --> Optimizer[Optimizer score and leaderboard]
```"""


def _render_module(module: ModuleInfo) -> list[str]:
    lines = [f"### `{module.path.as_posix()}`"]
    if module.imports:
        lines.append(f"- Imports: `{', '.join(module.imports)}`")
    else:
        lines.append("- Imports: none")
    for func in module.functions:
        prefix = "async " if func.async_ else ""
        lines.append(f"- Function L{func.line}: `{prefix}{func.signature} -> {func.returns}`")
    for cls in module.classes:
        bases = f"({', '.join(cls.bases)})" if cls.bases else ""
        lines.append(f"- Class L{cls.line}: `{cls.name}{bases}`")
        for method in cls.methods:
            prefix = "async " if method.async_ else ""
            lines.append(
                f"  - Method L{method.line}: `{prefix}{method.signature} -> {method.returns}`"
            )
    lines.append("")
    return lines


def build_document() -> str:
    modules = [
        _module_info(path)
        for path in sorted(REPO_ROOT.rglob("*.py"))
        if _is_included_python_file(path)
    ]
    callable_count = sum(
        len(module.functions) + sum(len(cls.methods) for cls in module.classes)
        for module in modules
    )
    class_count = sum(len(module.classes) for module in modules)
    lines = [
        "# Codebase UML Inventory",
        "",
        "This file is generated from Python AST metadata and excludes `tests/`.",
        f"Generated: {datetime.now(UTC).isoformat(timespec='seconds')}",
        f"Modules: {len(modules)} | Classes: {class_count} | Functions/methods: {callable_count}",
        "",
        "## Backtesting Data Flow",
        "",
        _mermaid_overview(),
        "",
        "## Module Inventory",
        "",
    ]
    for module in modules:
        lines.extend(_render_module(module))
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    OUTPUT_PATH.write_text(build_document(), encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
