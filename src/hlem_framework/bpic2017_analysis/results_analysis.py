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
    file_name='results/throughput-3-classes.csv'
    header = ['Length', 'Frequency', 'Path', 'Part&under10', 'Part&10to30', 'Part&over30', 'NonPart&under10',
              'NonPart&10to30', 'NonPart&over30', 'p']
    rows = []
    for i in range(len(result_df)):
        path_i = result_df.iloc[i]
        path = path_i['path']
        path_freq = path_i['frequency']
        participating = path_i['participating']
        non_participating = path_i['non-participating']
        participation = [participating, non_participating]

        part_and_class1 = len(participation[0].intersection(outcome_throughput[0]))
        part_and_class2 = len(participation[0].intersection(outcome_throughput[1]))
        part_and_class3 = len(participation[0].intersection(outcome_throughput[2]))
        non_part_and_class1 = len(participation[1].intersection(outcome_throughput[0]))
        non_part_and_class2 = len(participation[1].intersection(outcome_throughput[1]))
        non_part_and_class3 = len(participation[1].intersection(outcome_throughput[2]))

        p_value, special_throughput = significance.significance(participation, outcome_throughput)
        if special_throughput:
            row = [len(path), path_freq, path, part_and_class1, part_and_class2, part_and_class3, non_part_and_class1,
                   non_part_and_class2, non_part_and_class3, p_value]
            rows.append(row)

    eval_df = pd.DataFrame(rows, columns=header)
    eval_df.to_csv(file_name, index=True, header=True)

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