import pandas as pd
import numpy as np
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

STEAM_FILE = DATA_DIR / "steam.csv"
PLAYSTATION_FILE = DATA_DIR / "playstation.csv"
NINTENDO_FILE = DATA_DIR / "nintendo.csv"
OUTPUT_FILE = DATA_DIR / "games_unified.csv"


def safe_value(value):
    """Convert NaN-like values to None."""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "" or value.lower() in {"nan", "n/a", "none", "null"}:
            return None
    return value


def safe_int(value):
    value = safe_value(value)
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def safe_float(value):
    value = safe_value(value)
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace(",", ".")
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def split_to_list(value, separators=(";", ",")):
    value = safe_value(value)
    if value is None:
        return []
    items = [str(value)]
    for sep in separators:
        new_items = []
        for item in items:
            new_items.extend(item.split(sep))
        items = new_items
    return [item.strip() for item in items if item.strip()]


def extract_year_from_date(value):
    value = safe_value(value)
    if value is None:
        return None

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.notna(parsed):
        return int(parsed.year)

    import re
    match = re.search(r"(19|20)\d{2}", str(value))
    if match:
        return int(match.group(0))

    return None


def prepare_steam():
    df = pd.read_csv(STEAM_FILE)

    records = []
    for _, row in df.iterrows():
        record = {
            "source_platform": "steam",
            "source_id": safe_int(row.get("appid")),
            "title": safe_value(row.get("name")),
            "publisher": safe_value(row.get("publisher")),
            "developer": safe_value(row.get("developer")),
            "release_date": safe_value(row.get("release_date")),
            "release_year": extract_year_from_date(row.get("release_date")),
            "genre": split_to_list(row.get("genres")),
            "price": safe_float(row.get("price")),
            "critic_score": None,
            "user_score": None,
            "positive_ratings": safe_int(row.get("positive_ratings")),
            "negative_ratings": safe_int(row.get("negative_ratings")),
            "average_playtime": safe_int(row.get("average_playtime")),
            "owners": safe_value(row.get("owners")),
            "features": split_to_list(row.get("categories")),
            "raw_source": {
                "platforms": safe_value(row.get("platforms")),
                "steamspy_tags": safe_value(row.get("steamspy_tags")),
                "required_age": safe_int(row.get("required_age")),
                "achievements": safe_int(row.get("achievements")),
                "median_playtime": safe_int(row.get("median_playtime")),
                "english": safe_int(row.get("english")),
            },
        }
        records.append(record)

    return pd.DataFrame(records)


def prepare_playstation():
    df = pd.read_csv(PLAYSTATION_FILE)

    records = []
    for _, row in df.iterrows():
        record = {
            "source_platform": "playstation",
            "source_id": safe_int(row.get("ID")),
            "title": safe_value(row.get("GameName")),
            "publisher": safe_value(row.get("Publisher")),
            "developer": safe_value(row.get("Developer")),
            "release_date": safe_value(row.get("ReleaseDate")),
            "release_year": safe_int(row.get("ReleaseYear")) or extract_year_from_date(row.get("ReleaseDate")),
            "genre": split_to_list(row.get("Genre")),
            "price": None,
            "critic_score": None,
"user_score": None,
            "positive_ratings": None,
            "negative_ratings": None,
            "average_playtime": safe_float(row.get("CompletionTime(Hours)")),
            "owners": None,
            "features": split_to_list(row.get("Features")),
            "raw_source": {
                "game_ps_id": safe_int(row.get("GamePSID")),
                "size": safe_int(row.get("Size")),
                "medium": safe_value(row.get("Medium")),
                "hardware": safe_value(row.get("Hardware")),
                "is_digital_game": safe_int(row.get("is_Digital_game")),
                "is_physical_game": safe_int(row.get("is_Physical_game")),
                "official_website": safe_value(row.get("OfficialWebsite")),
            },
        }
        records.append(record)

    return pd.DataFrame(records)


def prepare_nintendo():
    df = pd.read_csv(NINTENDO_FILE, encoding="latin1")

    records = []
    for _, row in df.iterrows():
        record = {
            "source_platform": "nintendo",
            "source_id": safe_int(row.get("Position")),
            "title": safe_value(row.get("Game")),
            "publisher": safe_value(row.get("Publisher")),
            "developer": safe_value(row.get("Developer")),
            "release_date": safe_value(row.get("Release Date")),
            "release_year": extract_year_from_date(row.get("Release Date")),
            "genre": [],
            "price": None,
            "critic_score": safe_float(row.get("Critic Score")),
            "user_score": safe_float(row.get("User Score")),
            "positive_ratings": None,
            "negative_ratings": None,
            "average_playtime": None,
            "owners": None,
            "features": [],
            "raw_source": {
                "vgchartz_score": safe_float(row.get("VGChartz Score")),
                "total_shipped": safe_value(row.get("Total Shipped")),
                "last_update": safe_value(row.get("Last Update")),
            },
        }
        records.append(record)

    return pd.DataFrame(records)


def main():
    print("Loading source datasets...")

    steam_df = prepare_steam()
    print(f"Steam prepared: {len(steam_df)} rows")

    playstation_df = prepare_playstation()
    print(f"PlayStation prepared: {len(playstation_df)} rows")

    nintendo_df = prepare_nintendo()
    print(f"Nintendo prepared: {len(nintendo_df)} rows")

    unified_df = pd.concat(
        [steam_df, playstation_df, nintendo_df],
        ignore_index=True
    )

    output_csv = DATA_DIR / "games_unified.csv"
    output_json = DATA_DIR / "games_unified.json"

    unified_df.to_csv(output_csv, index=False)
    print(f"Unified CSV saved to: {output_csv}")

    unified_df.to_json(output_json, orient="records", lines=True, force_ascii=False)
    print(f"Unified JSON saved to: {output_json}")

    print(f"Total rows: {len(unified_df)}")


if __name__ == "__main__":
    main()
