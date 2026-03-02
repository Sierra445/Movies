from pathlib import Path
import sys

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "data" / "processed"


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"File is empty: {path}")
    return pd.read_csv(path)


def run_etl() -> int:
    movies_path = DATA_DIR / "movies.csv"
    ratings_path = DATA_DIR / "ratings.csv"

    print("Starting ETL...")
    print(f"Movies source: {movies_path}")
    print(f"Ratings source: {ratings_path}")

    try:
        movies = load_csv(movies_path)
        ratings = load_csv(ratings_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ETL failed: {exc}")
        return 1

    if "movieId" not in movies.columns or "movieId" not in ratings.columns:
        print("ETL failed: both CSV files must include a 'movieId' column.")
        return 1

    avg_ratings = (
        ratings.groupby("movieId", as_index=False)["rating"]
        .mean()
        .rename(columns={"rating": "avg_rating"})
    )

    result = movies.merge(avg_ratings, on="movieId", how="left")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "movies_enriched.csv"
    result.to_csv(output_path, index=False)

    print(f"Rows processed: {len(result)}")
    print(f"Output written to: {output_path}")
    print("ETL complete.")
    return 0


if __name__ == "__main__":
    sys.exit(run_etl())
