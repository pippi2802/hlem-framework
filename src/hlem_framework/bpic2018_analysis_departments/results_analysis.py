import os.path
import pandas as pd
import sys
import logging
from collections import Counter
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
# Add the parent folder (where hlem_with_log.py lives) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from hl_paths import significance


def results_outcome(df_paths, successful_cases, unsuccessful_cases, output_file='results/outcome_results.csv'):
    """
    Tests which high-level paths are statistically correlated with case success or failure.
    
    For each path:
    - Counts participating cases that succeeded vs. failed
    - Counts non-participating cases that succeeded vs. failed
    - Runs chi-square test to determine if path significantly affects success rate
    - Saves only statistically significant paths (p ≤ 0.05) to CSV
    
    :param df_paths: DataFrame with path statistics (from gather_statistics)
    :param successful_cases: List of case IDs that succeeded
    :param unsuccessful_cases: List of case IDs that failed
    :param output_file: Name of the output CSV file
    :return: DataFrame with statistically significant paths
    """    
    # Convert to sets for intersection operations
    successful_set = set(successful_cases)
    unsuccessful_set = set(unsuccessful_cases)

    outcome_partition = [successful_set, unsuccessful_set]
    
    results = []
    
    for _, row in df_paths.iterrows():
        path = row['path']
        path_freq = row['frequency']
        participating = row['participating']
        non_participating = row['non-participating']
        
        # Count participating cases by outcome
        part_success = len(participating.intersection(successful_set))
        part_unsuccess = len(participating.intersection(unsuccessful_set))
        
        # Count non-participating cases by outcome
        non_part_success = len(non_participating.intersection(successful_set))
        non_part_unsuccess = len(non_participating.intersection(unsuccessful_set))
        
        participation_partition = [participating, non_participating]
        
        # Run significant correlation test 
        p_value, is_significant = significance.significance(participation_partition, outcome_partition, method='chi square')
        
        # Only include statistically significant paths (p ≤ 0.05)
        if is_significant:
            results.append({
                'Length': len(path),
                'Frequency': path_freq,
                'Path': path,
                'Part&Success': part_success,
                'Part&Unsuccess': part_unsuccess,
                'NonPart&Success': non_part_success,
                'NonPart&Unsuccess': non_part_unsuccess,
                'p_value': p_value
            })
    
    # Create DataFrame
    results_df = pd.DataFrame(results)
    
    if len(results_df) > 0:
        # Save to CSV
        results_df.to_csv(output_file, index=False)
        logging.info(f"Found {len(results_df)} statistically significant paths (p ≤ 0.05)")
        logging.info(f"Results saved to {output_file}")
    else:
        logging.info("No statistically significant paths found")
    
    return results_df

def throughput_tables(result_df, outcome_throughput):
    """
    Tests which high-level paths are statistically correlated with throughput time.
    Partitions cases into two categories: <= 250 days and > 250 days.
    """
    file_name = 'results/throughput-2-classes.csv'
    header = ['Length', 'Frequency', 'Path', 'Part&<=250days', 'Part&>250days', 
              'NonPart&<=250days', 'NonPart&>250days', 'p_value']
    rows = []
    
    under_250_set = set(outcome_throughput[0]) if not isinstance(outcome_throughput[0], set) else outcome_throughput[0]
    over_250_set = set(outcome_throughput[1]) if not isinstance(outcome_throughput[1], set) else outcome_throughput[1]
    
    for i in range(len(result_df)):
        path_i = result_df.iloc[i]
        path = path_i['path']
        path_freq = path_i['frequency']
        participating = path_i['participating']
        non_participating = path_i['non-participating']
        participation = [participating, non_participating]

        # Count participating cases by throughput time
        part_under_250 = len(participating.intersection(under_250_set))
        part_over_250 = len(participating.intersection(over_250_set))
        
        # Count non-participating cases by throughput time
        non_part_under_250 = len(non_participating.intersection(under_250_set))
        non_part_over_250 = len(non_participating.intersection(over_250_set))

        # Run chi-square test
        p_value, is_significant = significance.significance(participation, 
                                                           [under_250_set, over_250_set], 
                                                           method='chi square')
        
        # Only include statistically significant paths (p ≤ 0.05)
        if is_significant:
            row = [len(path), path_freq, path, part_under_250, part_over_250, 
                   non_part_under_250, non_part_over_250, p_value]
            rows.append(row)

    eval_df = pd.DataFrame(rows, columns=header)
    
    if len(eval_df) > 0:
        eval_df = eval_df.sort_values('p_value')
        eval_df.to_csv(file_name, index=False)
        logging.info(f"Found {len(eval_df)} statistically significant paths for throughput time (p ≤ 0.05)")
        logging.info(f"Results saved to {file_name}")
    else:
        logging.info("No statistically significant paths found for throughput time")
    
    return eval_df

def department_tables(result_df, department_partition):
    """
    Tests which high-level paths are statistically correlated with department membership.
    Creates separate CSV files for each department to make analysis easier.
    Uses chi-square test to determine if path participation differs significantly.
    
    :param result_df: DataFrame with path statistics
    :param department_partition: List of 4 sets/lists of case IDs for each department [dept_4e, dept_e7, dept_6b, dept_d4]
    :return: Dictionary mapping department names to their significant path DataFrames
    """
    logging.info("Analyzing paths by department correlation")
    
    # Convert to sets if not already
    dept_sets = [set(dept) if not isinstance(dept, set) else dept for dept in department_partition]
    dept_names = ['4e', 'e7', '6b', 'd4']
    
    results = {}
    
    # Create a separate table for each department
    for dept_idx, dept_name in enumerate(dept_names):
        dept_set = dept_sets[dept_idx]
        other_depts = set().union(*[dept_sets[i] for i in range(len(dept_sets)) if i != dept_idx])
        
        rows = []
        for i in range(len(result_df)):
            path_i = result_df.iloc[i]
            path = path_i['path']
            path_freq = path_i['frequency']
            participating = path_i['participating']
            non_participating = path_i['non-participating']
            
            # Count for this specific department
            part_in_dept = len(participating.intersection(dept_set))
            nonpart_in_dept = len(non_participating.intersection(dept_set))
            total_in_dept = len(dept_set)
            
            # Count for other departments combined
            part_in_others = len(participating.intersection(other_depts))
            nonpart_in_others = len(non_participating.intersection(other_depts))
            total_in_others = len(other_depts)
            
            # Calculate participation rates
            participation_rate_dept = (part_in_dept / total_in_dept * 100) if total_in_dept > 0 else 0
            participation_rate_others = (part_in_others / total_in_others * 100) if total_in_others > 0 else 0
            
            # Run chi-square test
            partition_this_dept = [participating.intersection(dept_set), non_participating.intersection(dept_set)]
            partition_others = [participating.intersection(other_depts), non_participating.intersection(other_depts)]
            p_value, is_significant = significance.significance([participating, non_participating], 
                                                               [dept_set, other_depts], 
                                                               method='chi square')
            
            # Only include statistically significant paths (p ≤ 0.05)
            if is_significant:
                rows.append({
                    'Length': len(path),
                    'Frequency': path_freq,
                    'Path': path,
                    f'Part_in_{dept_name}': part_in_dept,
                    f'NonPart_in_{dept_name}': nonpart_in_dept,
                    f'Total_in_{dept_name}': total_in_dept,
                    f'Participation_Rate_{dept_name}_%': round(participation_rate_dept, 2),
                    'Part_in_Others': part_in_others,
                    'NonPart_in_Others': nonpart_in_others,
                    'Total_in_Others': total_in_others,
                    'Participation_Rate_Others_%': round(participation_rate_others, 2),
                    'p_value': p_value
                })
        
        # Create DataFrame and sort by p-value
        dept_df = pd.DataFrame(rows)
        if len(dept_df) > 0:
            dept_df = dept_df.sort_values('p_value')
        
        # Save to separate CSV file
        output_file = f'results/department-{dept_name}.csv'
        dept_df.to_csv(output_file, index=False)
        
        results[dept_name] = dept_df
        logging.info(f"Department {dept_name}: {len(dept_df)} significant paths (saved to {output_file})")
    
    # Combined overview table with all departments
    combined_header = ['Length', 'Frequency', 'Path', 
                      'Part&4e', 'Part&e7', 'Part&6b', 'Part&d4',
                      'NonPart&4e', 'NonPart&e7', 'NonPart&6b', 'NonPart&d4', 
                      'p_value']
    
    combined_rows = []
    for i in range(len(result_df)):
        path_i = result_df.iloc[i]
        path = path_i['path']
        path_freq = path_i['frequency']
        participating = path_i['participating']
        non_participating = path_i['non-participating']
        participation = [participating, non_participating]
        
        # Count participating cases by department
        part_dept_counts = [len(participating.intersection(dept_set)) for dept_set in dept_sets]
        
        # Count non-participating cases by department
        non_part_dept_counts = [len(non_participating.intersection(dept_set)) for dept_set in dept_sets]
        
        # Run chi-square test across all 4 departments
        p_value, is_significant = significance.significance(participation, dept_sets, method='chi square')
        
        # Only include statistically significant paths (p ≤ 0.05)
        if is_significant:
            row = [len(path), path_freq, path] + part_dept_counts + non_part_dept_counts + [p_value]
            combined_rows.append(row)
    
    # Create combined DataFrame
    combined_df = pd.DataFrame(combined_rows, columns=combined_header)
    
    if len(combined_df) > 0:
        combined_df = combined_df.sort_values('p_value')
        combined_df.to_csv('results/department-all-combined.csv', index=False)
        logging.info(f"Combined view: {len(combined_df)} significant paths (saved to results/department-all-combined.csv)")
    
    return results

def print_hle_statistics(hle_all_dic, save_to_file=True, output_file='results/hle_statistics.txt'):
    """
    Prints and optionally saves a table showing statistics for high-level events by feature type.
    For each feature type shows total count (%), number of distinct segments, and most frequent segment.
    
    :param hle_all_dic: Dictionary of all high-level events
    :param save_to_file: Whether to save the table to a file
    :param output_file: Name of the output file (default: 'results/hle_statistics.txt')
    """
    
    # Group HLEs by feature type
    feature_stats = {}
    
    for hle_id, hle_info in hle_all_dic.items():
        feature_type = hle_info['f-type']
        entity = hle_info['entity']
        
        if feature_type not in feature_stats:
            feature_stats[feature_type] = {'count': 0, 'segments': []}
        
        feature_stats[feature_type]['count'] += 1
        feature_stats[feature_type]['segments'].append(entity)
    
    # Calculate totals
    total_hles = sum(stats['count'] for stats in feature_stats.values())
    
    # Output lines
    lines = []
    lines.append("=" * 120)
    lines.append("HIGH-LEVEL EVENT STATISTICS")
    lines.append("=" * 120)
    lines.append(f"{'Feature Type':<20} {'Hle Count (%)':<20} {'Distinct Segments':<20} {'Most Frequent Segment'}")
    lines.append("-" * 120)
    
    for feature_type in sorted(feature_stats.keys()):
        stats = feature_stats[feature_type]
        count = stats['count']
        percentage = (count / total_hles * 100) if total_hles > 0 else 0
        
        # Count distinct segments and find most frequent
        segment_counter = Counter(stats['segments'])
        distinct_segments = len(segment_counter)
        most_common_segment, most_common_count = segment_counter.most_common(1)[0]
        
        # Format output
        count_pct = f"{count} ({percentage:.2f}%)"
        segment_str = f"{most_common_segment} (n={most_common_count})"
        
        lines.append(f"{feature_type:<20} {count_pct:<20} {distinct_segments:<20} {segment_str}")
    
    lines.append("-" * 120)
    lines.append(f"{'TOTAL':<20} {total_hles:<10}")
    lines.append("=" * 120)
    
    # Print to console
    print("\n" + "\n".join(lines) + "\n")
    
    # Save to file if requested
    if save_to_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines) + "\n")
        logging.info(f"Statistics saved to {output_file}")