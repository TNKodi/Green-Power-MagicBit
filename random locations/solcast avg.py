"""Compute daily averages for solcast hourly CSVs ignoring zero values."""

from pathlib import Path

import pandas as pd


def daily_average_nonzero(df: pd.DataFrame) -> pd.DataFrame:
	"""Return daily mean per column, ignoring zero entries for each metric."""

	work = df.copy()
	work["date"] = pd.to_datetime(work["period_end"]).dt.date

	value_cols = [c for c in work.columns if c not in {"period_end", "period", "date"}]
	work[value_cols] = work[value_cols].replace(0, pd.NA)

	daily = work.groupby("date", as_index=False)[value_cols].mean()
	return daily.fillna(0)


def process_file(csv_path: Path) -> tuple[Path, int]:
	df = pd.read_csv(csv_path)
	daily = daily_average_nonzero(df)

	out_path = csv_path.with_name(csv_path.stem.replace("_hourly", "_daily_avg_nonzero") + csv_path.suffix)
	daily.to_csv(out_path, index=False)
	return out_path, len(daily)


def main() -> None:
	folder = Path(__file__).resolve().parent
	files = sorted(folder.glob("solcast_nov_2025_*_hourly.csv"))

	if not files:
		print("No solcast hourly CSV files found.")
		return

	for csv_path in files:
		out_path, n_days = process_file(csv_path)
		print(f"Processed {csv_path.name} -> {out_path.name} ({n_days} days)")


if __name__ == "__main__":
	main()
