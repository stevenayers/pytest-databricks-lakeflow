import importlib.metadata
import sys


def main() -> None:
    greeting = sys.argv[1] if len(sys.argv) > 1 else ""
    if greeting.startswith("version "):
        # Reports an installed distribution, so tests can check that extra_dependencies reached the environment.
        name = greeting.removeprefix("version ")
        print(f"{name} {importlib.metadata.version(name)}", flush=True)
        return
    if greeting.startswith("--"):
        # Named parameters arrive as --name=value arguments. Print them all, so tests can see which reached the wheel.
        print(f"Named: {' '.join(sorted(sys.argv[1:]))}", flush=True)
        return
    print(f"Success! {greeting}", flush=True)
