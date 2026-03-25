from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from ppi import create_app  # noqa: E402
from ppi.migrations import run_migrations  # noqa: E402


def main() -> None:
    app = create_app()
    with app.app_context():
        ran = run_migrations()
    if ran:
        print("Applied migrations:", ", ".join(ran))
    else:
        print("No pending migrations.")


if __name__ == "__main__":
    main()
