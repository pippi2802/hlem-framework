import pandas as pd

# Example: filter paths containing a keyword
query_1 = "(('exit', ('A_Complete', 'W_Call after offers|suspend')), ('enter', ('W_Call after offers|suspend', 'W_Call after offers|resume')))"
query_2 = "W_Validate application|suspend"


def print_outcome_tables(
    csv_path="outcome_results.csv",
    queries=None,
    case_sensitive=False
):
    """
    Loads outcome_results.csv and prints success/failure tables 
    for paths matching any of the provided query strings.

    :param csv_path: Path to the CSV file (default: 'outcome_results.csv')
    :param queries: List of substrings to match within the 'Path' column
    :param case_sensitive: Whether path matching should be case sensitive
    """

    if queries is None:
        queries = []

    # Load CSV
    df = pd.read_csv(csv_path, quotechar='"', skipinitialspace=True)
    df.columns = df.columns.str.strip()  # clean whitespace

    if 'Path' not in df.columns:
        print("❌ 'Path' column not found in CSV.")
        return

    # Ensure the columns we expect are present
    required_cols = [
        'Part&Success', 'Part&Unsuccess',
        'NonPart&Success', 'NonPart&Unsuccess'
    ]
    for col in required_cols:
        if col not in df.columns:
            print(f"❌ Missing column: {col}")
            return

    # For each query, filter and print matching paths
    for query in queries:
        mask = df['Path'].str.contains(query, case=case_sensitive, regex=False)
        filtered_df = df[mask]

        print(f"\n=== Query: '{query}' ===")
        if filtered_df.empty:
            print("No matching path found.")
            continue

        for path, group in filtered_df.groupby('Path'):
            success_part = group['Part&Success'].sum()
            failure_part = group['Part&Unsuccess'].sum()
            success_nonpart = group['NonPart&Success'].sum()
            failure_nonpart = group['NonPart&Unsuccess'].sum()

            total_success = success_part + success_nonpart
            total_failure = failure_part + failure_nonpart
            total_part = success_part + failure_part
            total_nonpart = success_nonpart + failure_nonpart
            grand_total = total_part + total_nonpart

            def format_count(count, success, total):
                rate = success / total if total > 0 else 0
                return f"{count} ({rate:.2%})"

            table = pd.DataFrame({
                'Participant': [
                    format_count(success_part, success_part, total_part),
                    format_count(failure_part, success_part, total_part)
                ],
                'Non-Participant': [
                    format_count(success_nonpart, success_nonpart, total_nonpart),
                    format_count(failure_nonpart, success_nonpart, total_nonpart)
                ]
            }, index=['Success', 'Failure'])

            table['Total'] = [
                format_count(total_success, total_success, grand_total),
                format_count(total_failure, total_success, grand_total)
            ]

            total_row = pd.DataFrame({
                'Participant': [format_count(total_part, success_part, total_part)],
                'Non-Participant': [format_count(total_nonpart, success_nonpart, total_nonpart)],
                'Total': [format_count(grand_total, total_success, grand_total)]
            }, index=['Total'])

            table = pd.concat([table, total_row])

            print(f"\nPath: {path}")
            print(table)


def print_throughput_tables(
    csv_path="results/throughput-3-classes.csv",
    queries=None,
    case_sensitive=False
):
    """
    Loads throughput-3-classes.csv and prints throughput distribution tables 
    for paths matching any of the provided query strings.

    CSV expected columns:
    ['Length', 'Frequency', 'Path', 
     'Part&under10', 'Part&10to30', 'Part&over30',
     'NonPart&under10', 'NonPart&10to30', 'NonPart&over30', 'p']

    :param csv_path: Path to the CSV file (default: 'results/throughput-3-classes.csv')
    :param queries: List of substrings to match in 'Path' column
    :param case_sensitive: Whether to perform case-sensitive matching
    """

    if queries is None:
        queries = []

    # Load CSV
    df = pd.read_csv(csv_path, quotechar='"', skipinitialspace=True)
    df.columns = df.columns.str.strip()

    required_cols = [
        'Path',
        'Part&under10', 'Part&10to30', 'Part&over30',
        'NonPart&under10', 'NonPart&10to30', 'NonPart&over30'
    ]
    for col in required_cols:
        if col not in df.columns:
            print(f" Missing required column: {col}")
            return

    for query in queries:
        mask = df['Path'].str.contains(query, case=case_sensitive, regex=False)
        filtered_df = df[mask]

        print(f"\n=== Query: '{query}' ===")
        if filtered_df.empty:
            print("No matching path found.")
            continue

        for path, group in filtered_df.groupby('Path'):
            # Sum counts per throughput class
            part_under10 = group['Part&under10'].sum()
            part_10to30 = group['Part&10to30'].sum()
            part_over30 = group['Part&over30'].sum()

            nonpart_under10 = group['NonPart&under10'].sum()
            nonpart_10to30 = group['NonPart&10to30'].sum()
            nonpart_over30 = group['NonPart&over30'].sum()

            total_part = part_under10 + part_10to30 + part_over30
            total_nonpart = nonpart_under10 + nonpart_10to30 + nonpart_over30
            grand_total = total_part + total_nonpart

            def format_count(count, total):
                rate = count / total if total > 0 else 0
                return f"{count} ({rate:.2%})"

            # Build a table: Throughput class × (Participant, Non-Participant, Total)
            table = pd.DataFrame({
                'Participant': [
                    format_count(part_under10, total_part),
                    format_count(part_10to30, total_part),
                    format_count(part_over30, total_part)
                ],
                'Non-Participant': [
                    format_count(nonpart_under10, total_nonpart),
                    format_count(nonpart_10to30, total_nonpart),
                    format_count(nonpart_over30, total_nonpart)
                ]
            }, index=['under10', '10to30', 'over30'])

            # Add total row
            total_row = pd.DataFrame({
                'Participant': [format_count(total_part, grand_total)],
                'Non-Participant': [format_count(total_nonpart, grand_total)],
            }, index=['Total'])

            table = pd.concat([table, total_row])

            print(f"\nPath: {path}")
            print(table)
