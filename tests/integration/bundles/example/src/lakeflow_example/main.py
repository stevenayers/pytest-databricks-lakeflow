import sys


def main() -> None:
    greeting = sys.argv[1] if len(sys.argv) > 1 else ""
    print(f"Success! {greeting}", flush=True)
