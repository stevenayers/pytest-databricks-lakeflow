import importlib.metadata
import sys


def main() -> None:
    greeting = sys.argv[1] if len(sys.argv) > 1 else ""
    if greeting.startswith("version "):
        # Reports an installed distribution, so tests can check that extra_dependencies reached the environment.
        name = greeting.removeprefix("version ")
        print(f"{name} {importlib.metadata.version(name)}", flush=True)
        return
    print(f"Success! {greeting}", flush=True)
