import pandas as pd
import os


def participation_rate(part, non_part):
    total = part + non_part
    return 100 * part / total if total > 0 else 0


def print_path_statistics(row, path_idx=None):
    """Print stats for a single path across departments."""
    print("\n" + "=" * 80)
    print(f"Length: {row['Length']}")
    print(f"Frequency: {row['Frequency']}")
    print(f"p-value: {row['p_value']:.2f}")
    print(f"Path: {row['Path']}")
    print()
    print(f"{'Department':<12} {'Participating':<20} {'Non-Participating':<20} {'Total':<10}")
    print("-" * 80)
    
    departments = ['4e', 'e7', '6b', 'd4']
    for dept in departments:
        part = row[f'Part&{dept}']
        non_part = row[f'NonPart&{dept}']
        total = part + non_part
        part_rate = participation_rate(part, non_part)
        non_part_rate = 100 - part_rate
        
        print(f"{dept:<12} {part} ({part_rate:.1f}%){'':<12} {non_part} ({non_part_rate:.1f}%){'':<12} {total:<10}")
    
    print()


def query_specific_paths(csv_file='results/department-all-combined.csv', path_queries=None):
    """
    Query and print specific paths by exact or partial match.
    
    :param csv_file: Path to the combined CSV file
    :param path_queries: List of path strings or substrings to search for
    """
    if not os.path.exists(csv_file):
        print(f"Error: CSV file not found: {csv_file}")
        print("Please run the main analysis first to generate the results.")
        return
    
    df = pd.read_csv(csv_file)
    
    if len(df) == 0:
        print("No significant paths found in the CSV file.")
        return
    
    if not path_queries:
        print("No path queries provided.")
        return
    
    for query in path_queries:
        mask = df['Path'].str.contains(query, case=False, regex=False, na=False)
        matched = df[mask]
        
        if len(matched) == 0:
            print(f"\nNo match found for: '{query}'\n")
            continue
        
        print(f"\n=== Query: '{query}' ===")
        print(f"Found {len(matched)} matching path(s)\n")
        
        for idx, (_, row) in enumerate(matched.iterrows(), 1):
            print_path_statistics(row, path_idx=idx)
        
        print("\n" + "-" * 80 + "\n")
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\n{'Department':<12} {'Avg Participation Rate':<20}")
    print("-" * 40)
    
    departments = ['4e', 'e7', '6b', 'd4']
    for dept in departments:
        avg_rate = df.apply(lambda row: participation_rate(row[f'Part&{dept}'], row[f'NonPart&{dept}']), axis=1).mean()
        print(f"{dept:<12} {avg_rate:.1f}%")
    
    print("=" * 80)


if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Manually selected paths that were showing interesting differences between departments
    selected_paths = [
        "(('delay', ('finish editing', 'calculate')),)",
        "(('delay', ('finish editing', 'initialize')),)",
        "(('batch', ('mail income', 'initialize')),)",
        "(('exit', ('begin editing', 'calculate')),)",
        "(('workload', ('save', 'calculate')),)",
    ]
    
    print("\n" + "=" * 80)
    print("SELECTED PATHS WITH DEPARTMENT DIFFERENCES")
    print("=" * 80)
    query_specific_paths(path_queries=selected_paths)
