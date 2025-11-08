import pandas as pd

def print_participation_tables(
    csv_path="results/throughput-2-classes.csv",
    queries=None,
    case_sensitive=False
):
    """
    Loads throughput CSV and prints tables showing participating vs non-participating
    case distributions for each relevant Path.

    Expected columns:
    ['Length','Frequency','Path',
     'Part&under5','Part&over10',
     'NonPart&under5','NonPart&over10','p']
    """

    if queries is None:
        queries = []

    # --- Load CSV ---
    df = pd.read_csv(csv_path, quotechar='"', skipinitialspace=True)
    df.columns = df.columns.str.strip()

    required_cols = [
        'Path', 'Part&under5', 'Part&over10', 'NonPart&under5', 'NonPart&over10'
    ]
    for col in required_cols:
        if col not in df.columns:
            print(f" Missing required column: {col}")
            return

    # --- Loop through queries ---
    for query in queries:
        mask = df['Path'].str.contains(query, case=case_sensitive, regex=False)
        filtered_df = df[mask]

        print(f"\n=== Query: '{query}' ===")
        if filtered_df.empty:
            print("No matching path found.")
            continue

        # Group by path (in case multiple match same)
        for path, group in filtered_df.groupby('Path'):
            # Sum up the counts
            part_under5 = group['Part&under5'].sum()
            part_over10 = group['Part&over10'].sum()
            nonpart_under5 = group['NonPart&under5'].sum()
            nonpart_over10 = group['NonPart&over10'].sum()

            total_part = part_under5 + part_over10
            total_nonpart = nonpart_under5 + nonpart_over10
            grand_total = total_part + total_nonpart

            # Format function
            def fmt(count, total):
                pct = count / total if total > 0 else 0
                return f"{int(count)} ({pct:.2%})"

            # Build the table
            table = pd.DataFrame({
                'Participant': [
                    fmt(part_under5, total_part),
                    fmt(part_over10, total_part)
                ],
                'Non-Participant': [
                    fmt(nonpart_under5, total_nonpart),
                    fmt(nonpart_over10, total_nonpart)
                ]
            }, index=['Below (under 250 days)', 'Above (over 250 days)'])

            # Add total row
            total_row = pd.DataFrame({
                'Participant': [fmt(total_part, grand_total)],
                'Non-Participant': [fmt(total_nonpart, grand_total)]
            }, index=['Total'])

            table = pd.concat([table, total_row])

            # Print result
            print(f"\nPath: {path}")
            print(table)

if __name__ == "__main__":
    print_participation_tables(
    csv_path=r"results\throughput-3-classes.csv",
    queries=["enter", "batch"]
)