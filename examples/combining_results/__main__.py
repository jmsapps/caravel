from caravel.cli import make_cli

from .pipeline import build_combining_pipeline

from ..support import example_output


def main() -> int:
    return make_cli(build_combining_pipeline(), default_output=example_output(__file__))()


if __name__ == "__main__":
    raise SystemExit(main())
