import json
import math
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

import pandas as pd
import requests

WEBEX_MESSAGES_URL = "https://webexapis.com/v1/messages"


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def get_run_date() -> date:
    override = os.getenv("RUN_DATE", "").strip()
    if not override:
        return date.today()
    try:
        return datetime.strptime(override, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"Invalid RUN_DATE '{override}'. Use YYYY-MM-DD, for example 2026-03-05."
        ) from exc


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def load_source_frame() -> pd.DataFrame:
    excel_file = os.getenv("EXCEL_FILE", "subscriptions.xlsx")
    sheet_name = os.getenv("SHEET_NAME", "Subscription Details-Line Level")
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    return normalize_columns(df)


def require_columns(df: pd.DataFrame, required: List[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {', '.join(missing)}")


def filter_frame(df: pd.DataFrame) -> pd.DataFrame:
    required = ["End Customer", "Subscription ID", "Renewal Date", "Status"]
    require_columns(df, required)

    work = df[required].copy()
    work["Status"] = work["Status"].astype(str).str.strip().str.upper()
    status_filter = os.getenv("STATUS_FILTER", "ACTIVE").strip().upper()
    if status_filter:
        work = work[work["Status"] == status_filter]

    work["Renewal Date Parsed"] = pd.to_datetime(work["Renewal Date"], errors="coerce")

    allow_duplicates = env_bool("ALLOW_DUPLICATES", False)
    if not allow_duplicates:
        work = work.drop_duplicates(subset=["End Customer", "Subscription ID", "Renewal Date"])

    return work


def select_due_rows(df: pd.DataFrame, today: date) -> pd.DataFrame:
    notify_days = int(os.getenv("NOTIFY_DAYS", "60"))
    target_date = today + timedelta(days=notify_days)

    due = df[df["Renewal Date Parsed"].dt.date == target_date].copy()
    due = due.sort_values(by=["Renewal Date Parsed", "End Customer", "Subscription ID"], na_position="last")
    due["Renewal Date"] = due["Renewal Date Parsed"].dt.strftime("%Y-%m-%d")
    due["End Customer"] = due["End Customer"].fillna("").astype(str).str.strip()
    due["Subscription ID"] = due["Subscription ID"].fillna("").astype(str).str.strip()
    return due[["End Customer", "Subscription ID", "Renewal Date"]]


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"sent_keys": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"sent_keys": []}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def row_key(row: pd.Series) -> str:
    return "|".join([
        str(row["End Customer"]).strip(),
        str(row["Subscription ID"]).strip(),
        str(row["Renewal Date"]).strip(),
    ])


def filter_unsent_rows(df: pd.DataFrame, state_file: Path) -> pd.DataFrame:
    state = load_state(state_file)
    sent_keys = set(state.get("sent_keys", []))
    if not sent_keys:
        return df
    keys = df.apply(row_key, axis=1)
    return df[~keys.isin(sent_keys)].copy()


def mark_rows_sent(df: pd.DataFrame, state_file: Path) -> None:
    state = load_state(state_file)
    sent_keys = set(state.get("sent_keys", []))
    for _, row in df.iterrows():
        sent_keys.add(row_key(row))
    state["sent_keys"] = sorted(sent_keys)
    save_state(state_file, state)


def build_messages(df: pd.DataFrame, run_date: date) -> List[str]:
    notify_days = int(os.getenv("NOTIFY_DAYS", "60"))
    per_message = max(1, int(os.getenv("PER_MESSAGE", "15")))
    include_header = env_bool("INCLUDE_HEADER", True)
    target_date = run_date + timedelta(days=notify_days)

    messages: List[str] = []
    total = len(df)
    pages = max(1, math.ceil(total / per_message))

    for page in range(pages):
        chunk = df.iloc[page * per_message : (page + 1) * per_message]
        lines = []
        if include_header:
            header = (
                f"**ACTIVE subscriptions renewing in {notify_days} days**  \n"
                f"Target renewal date: **{target_date.isoformat()}**  \n"
                f"Batch {page + 1} of {pages}"
            )
            lines.append(header)
            lines.append("")

        for _, row in chunk.iterrows():
            lines.append(
                "- **End Customer:** {end_customer}  \n"
                "  **Subscription ID:** {subscription_id}  \n"
                "  **Renewal Date:** {renewal_date}".format(
                    end_customer=row["End Customer"],
                    subscription_id=row["Subscription ID"],
                    renewal_date=row["Renewal Date"],
                )
            )
        messages.append("\n".join(lines))

    return messages


def send_to_webex(messages: List[str]) -> None:
    token = os.getenv("WEBEX_ACCESS_TOKEN", "").strip()
    room_id = os.getenv("WEBEX_ROOM_ID", "").strip()
    if not token:
        raise EnvironmentError("WEBEX_ACCESS_TOKEN is not set.")
    if not room_id:
        raise EnvironmentError("WEBEX_ROOM_ID is not set.")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    for message in messages:
        response = requests.post(
            WEBEX_MESSAGES_URL,
            headers=headers,
            json={"roomId": room_id, "markdown": message},
            timeout=30,
        )
        response.raise_for_status()


def main() -> None:
    run_date = get_run_date()
    state_file = Path(os.getenv("STATE_FILE", "sent_notifications.json"))

    source_df = load_source_frame()
    filtered_df = filter_frame(source_df)
    due_df = select_due_rows(filtered_df, run_date)
    due_df = filter_unsent_rows(due_df, state_file)

    print(f"Run date: {run_date.isoformat()}")
    print(f"Matching unsent rows: {len(due_df)}")

    if due_df.empty:
        print("No matching subscriptions found.")
        return

    dry_run = env_bool("DRY_RUN", False)
    messages = build_messages(due_df, run_date)

    if dry_run:
        print("DRY_RUN is enabled. No Webex messages were sent.")
        for idx, msg in enumerate(messages, start=1):
            print(f"--- Message {idx} ---")
            print(msg)
        return

    send_to_webex(messages)
    mark_rows_sent(due_df, state_file)
    print("Messages sent successfully.")


if __name__ == "__main__":
    main()
