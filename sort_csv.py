import csv

# Read the CSV file
input_file = "./src/main/resources/nyc_taxi_pickup_sorted.csv"
output_file = "./src/main/resources/nyc_taxi_medallion_sorted.csv"

print(f"Reading {input_file}...")

with open(input_file, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)  # Skip header
    rows = list(reader)

print(f"Read {len(rows)} rows")
print("Sorting by medallion (col 0) and dropoff_datetime (col 6)...")

# Sort by column 0 (medallion) then column 6 (dropoff_datetime)
sorted_rows = sorted(rows, key=lambda row: (row[0], row[6]))

print(f"Writing sorted data to {output_file}...")

with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(sorted_rows)

print(f"Done! Sorted {len(sorted_rows)} rows.")
print("File saved as: nyc_taxi_medallion_sorted.csv")
