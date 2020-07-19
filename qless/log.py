from datetime import datetime


def log(text: str) -> None:
    date = datetime.now()
    print(f"[{date}] {text}")