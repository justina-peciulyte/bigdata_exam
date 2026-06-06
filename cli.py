from pathlib import Path
import os
from typing import Optional

# Some utility functions for resolving CLI arguments and defining environment variable defaults.

def env_path(name: str, default: Path) -> Path:
    """Returns a Path from an environment variable or a default."""
    v = os.getenv(name)
    return Path(v) if v else default


def env_float(name: str, default: float) -> float:
    """Returns a float from an environment variable or a default."""
    v = os.getenv(name)
    return float(v) if v is not None and v != "" else default


def resolve_path_arg(arg_value: Optional[Path], env_name: str, default: Path, base_dir: Path) -> Path:
    """Resolves a path argument by first checking the CLI argument, then environment variables, and then using defaults."""
    if arg_value:
        candidate = Path(arg_value)
    else:
        ev = os.getenv(env_name)
        candidate = Path(ev) if ev else default

    if not candidate.is_absolute():
        return base_dir / candidate
    return candidate
