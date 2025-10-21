import pandas as pd

# Load CSV
df = pd.read_csv(
    "success-2-classes.csv",
    quotechar='"',
    skipinitialspace=True
)

# Clean column names
df.columns = df.columns.str.strip()

# Example: filter paths containing a keyword
query_1 = "(('exit', ('A_Complete', 'W_Call after offers|suspend')), ('enter', ('W_Call after offers|suspend', 'W_Call after offers|resume')))"  # replace with any substring in the path

query_2 = "W_Validate application|suspend"

filtered_df = df[df['Path'].str.contains(query_1, regex=False)]

# Check if we found anything
if filtered_df.empty:
    print("No matching path found.")
else:
    # Iterate over each matched path
    for path, group in filtered_df.groupby('Path'):
        success_part = group['Part&Succ'].sum()
        failure_part = group['Part&NotSucc'].sum()
        success_nonpart = group['NonPart&Succ'].sum()
        failure_nonpart = group['NonPart&NotSucc'].sum()
        
        total_success = success_part + success_nonpart
        total_failure = failure_part + failure_nonpart
        total_part = success_part + failure_part
        total_nonpart = success_nonpart + failure_nonpart
        grand_total = total_part + total_nonpart

        def format_count(count, success, total):
            rate = success / total if total > 0 else 0
            return f"{count} ({rate:.2%})"
        
        table = pd.DataFrame({
            'Participant': [format_count(success_part, success_part, total_part),
                            format_count(failure_part, success_part, total_part)],
            'Non-Participant': [format_count(success_nonpart, success_nonpart, total_nonpart),
                                format_count(failure_nonpart, success_nonpart, total_nonpart)]
        }, index=['Success', 'Failure'])
        
        table['Total'] = [format_count(total_success, total_success, grand_total),
                          format_count(total_failure, total_success, grand_total)]
        
        total_row = pd.DataFrame({
            'Participant': [format_count(total_part, success_part, total_part)],
            'Non-Participant': [format_count(total_nonpart, success_nonpart, total_nonpart)],
            'Total': [format_count(grand_total, total_success, grand_total)]
        }, index=['Total'])
        
        table = pd.concat([table, total_row])
        
        print(f"\nPath: {path}")
        print(table)
