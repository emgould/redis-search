
import os

from dotenv import load_dotenv


def load_env():
    """Load environment variables from env file.

    Defaults to config/local.env for local development.
    Set ENV_FILE environment variable to override.
    """
    env = os.getenv("ENV_FILE", "config/local.env")
    load_dotenv(env)
