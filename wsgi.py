from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

from ppi import create_app  # noqa: E402

application = create_app()
