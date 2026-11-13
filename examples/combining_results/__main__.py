from caravel.cli import make_cli

from .pipeline import build_combining_pipeline


def main() -> int:
    return make_cli(build_combining_pipeline())()


if __name__ == "__main__":
    raise SystemExit(main())
