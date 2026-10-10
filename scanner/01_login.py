import json
from pathlib import Path
from kiteconnect import KiteConnect

CONFIG_FILE = Path("config.json")
SESSION_FILE = Path("session.json")


def load_config():
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            "config.json not found. Create it with api_key and api_secret."
        )

    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        config = json.load(f)

    for key in ("api_key", "api_secret"):
        if not config.get(key) or config[key].startswith("YOUR_"):
            raise ValueError(f"Set a real {key} in config.json")

    return config


def serialize_value(value):
    """Convert datetime/date values returned by Kite into JSON-safe strings."""
    if hasattr(value, "isoformat"):
        return value.isoformat()

    return value


def main():
    config = load_config()

    kite = KiteConnect(
        api_key=config["api_key"]
    )

    print("Open this URL in your browser and complete Zerodha login:")
    print()
    print(kite.login_url())
    print()

    request_token = input(
        "Paste the request_token from the redirect URL: "
    ).strip()

    if not request_token:
        raise ValueError("request_token cannot be empty")

    session = kite.generate_session(
        request_token,
        api_secret=config["api_secret"]
    )

    access_token = session["access_token"]

    kite.set_access_token(access_token)

    session_to_save = {
        "access_token": serialize_value(access_token),
        "login_time": serialize_value(session.get("login_time")),
        "user_id": serialize_value(session.get("user_id"))
    }

    SESSION_FILE.write_text(
        json.dumps(
            session_to_save,
            indent=2
        ),
        encoding="utf-8"
    )

    profile = kite.profile()

    print()
    print("====================================")
    print("KITE LOGIN SUCCESSFUL")
    print("====================================")
    print(f"User: {profile.get('user_name')}")
    print(f"User ID: {profile.get('user_id')}")
    print(f"Session saved: {SESSION_FILE.resolve()}")


if __name__ == "__main__":
    main()