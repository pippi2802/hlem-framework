import os.path
import pickle
import pm4py
import pandas as pd
from collections import Counter
import sys
# Add the parent folder (where hlem_with_log.py lives) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import hlem_with_paths
from hl_paths import postprocess, significance, case_participation
import logging
import preprocessing 
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

# Debug: verify module location
logging.info(f"hlem_with_paths module location: {hlem_with_paths.__file__}")


TRAFFIC_TYPE = 'High'
FRAME = 'days'
SELECTED_F_LIST = ['exit', 'enter', 'handover', 'workload', 'batch', 'delay']
P = 0.9
CO_THRESH = 0.5
CO_PATH_THRESH = 0.5
RES_INFO = True
FREQ = 10 # initially this was 0
ONLY_MAXIMAL_PATHS = True
PATH_FREQUENCY = 10
ACT_SELECTION = 'all'
TO_EXCLUDE = ['User_1']
SEG_METHOD = 'df'
TYPE_BASED = False  # Keep as False - original uses False despite confusing comment
SEG_PERCENTILE = 0.9


def load_event_log(xes_path):
    """
    Load an event log from XES file with pickle caching.
    
    :param xes_path: Path to the .xes file
    :return: Event log object
    """
    cache_path = xes_path.replace('.xes', '.pickle')
    
    if os.path.isfile(cache_path):
        logging.info(f'Loading log from cache: {cache_path}')
        with open(cache_path, 'rb') as f:
            return pickle.load(f)
    else:
        logging.info(f'Reading XES file: {xes_path}')
        log = pm4py.read_xes(xes_path)
        logging.info(f'Creating cache: {cache_path}')
        with open(cache_path, 'wb') as f:
            pickle.dump(log, f)
        return log

def print_hle_statistics(hle_all_dic, save_to_file=True, output_file='hle_statistics.txt'):
    """
    Prints and optionally saves a table showing statistics for high-level events by feature type.
    For each feature type shows total count (%), number of distinct segments, and most frequent segment.
    
    :param hle_all_dic: Dictionary of all high-level events
    :param save_to_file: Whether to save the table to a file
    :param output_file: Name of the output file (default: 'hle_statistics.txt')
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

def main(log, frame, traffic_type, selected_f_list, p, co_thresh, co_path_thresh, res_info, only_maximal_paths, path_frequency,
         act_selection, res_selection, seg_method, type_based, seg_percentile):
    """
    :param path: the local path to the log data

    :param frame: the time frame for partitioning the event log into time windows, can be 'days', 'weeks', 'months',

    :param traffic_type: can be 'High', 'Low' or ['High', 'Low']

    :param selected_f_list: the list of selected high-level features, can be any non-empty list of elements from
    ['exec', 'todo', 'wl', 'enter', 'exit', 'progress', 'wt']

    :param p: the percentile to determine what is considered high (or low), a number 50 < p < 100

    :param co_thresh: the lambda value in [0,1], determines whether any two hle are correlated or not (0.5)

    :param res_info: must be set to False if the event log has no resource information, otherwise can be set to True

    :param freq: the threshold for selecting the most frequent high-level activities

    :param only_comp: if True, the high-level activity only shows the component involved (e.g. 'Jane' instead of
    'wl-Jane')

    :param type_based:

    :param act_selection: if 'all', then all activities (and segments) in the initial log will be analyzed in
    combination with the chosen measures (e.g. 'exec', 'enter'). Otherwise, it must be a list of the activities of
    interest, then only the activities of interest and the segments comprised of them will be analyzed with the chosen
    measures

    :param res_selection: if 'all', then all resources in the initial log will be analyzed in combination with the
    chosen measures (e.g. 'wl'). Otherwise, it must be a list of the resources of interest. If no resource information
    is present in the initial log, then assign 'all'.

    :param seg_method: currently, only 'df' (directly-follows) possible for determining the steps set

    :param flatten: If False, the high-level traces in the output log may contain high-level events with identical
    timestamps. If True, an artificial total order is introduced to flatten the log (here: a lexicographical order on
    the set of the high-level activity labels)

    :return: a dictionary of all detected high-level events and a dictionary of all detected high-level activity paths
    
    
    """
#     # Debug: Print parameters
#     logging.info(f"Parameters: frame={frame}, traffic_type={traffic_type}, selected_f_list={selected_f_list}")
#     logging.info(f"Parameters: p={p}, type_based={type_based}, seg_percentile={seg_percentile}")
#     logging.info(f"Log has {len(log)} traces, {sum(len(t) for t in log)} events")
    
    # We use this method: it implements the overlap connection rules that the paper uses to create episodes and hl-paths.
    hle_all_dic, hla_paths_dict = hlem_with_paths.paths_and_cases_with_overlap(log, frame, traffic_type, selected_f_list, p, co_thresh, co_path_thresh, res_info, only_maximal_paths, path_frequency,
         act_selection, res_selection, seg_method, type_based, seg_percentile)

    return hle_all_dic, hla_paths_dict


def results_outcome(df_paths, successful_cases, unsuccessful_cases, output_file='outcome_results.csv'):
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
        # Sort by p-value in descending order
        #results_df = results_df.sort_values('p_value')
        
        # Save to CSV
        results_df.to_csv(output_file, index=False)
        logging.info(f"Found {len(results_df)} statistically significant paths (p ≤ 0.05)")
        logging.info(f"Results saved to {output_file}")
    else:
        logging.info("No statistically significant paths found")
    
    return results_df


if __name__ == '__main__':
    current_dir = os.path.abspath(os.curdir)
    bpi2017_path = os.path.join(current_dir, "event_logs/BPI2017.xes")
    #print("Event log path:", bpi2017_path)
    
    # Load log
    log = load_event_log(bpi2017_path)
    logging.info('The log has ' + str(len(log)) + ' traces.')

    no_events = sum([len(trace) for trace in log])
    logging.info('The log has ' + str(no_events) + ' events.')
    
    # Filter out incomplete cases
    #log = preprocessing.filter_incomplete_cases(log)
    
    # Remove User_1 from the log
    logging.info("Getting resource selection")
    res_selection = preprocessing.get_resources(log, TO_EXCLUDE)

    # Rename workflow activities
    log = preprocessing.rename_workflow_activities(log)
    
    successful_cases, unsuccessful_cases = preprocessing.partition_outcome(log)

    print("Running main...")
    hle_all_dic, hla_paths_dict = main(log, FRAME, TRAFFIC_TYPE, SELECTED_F_LIST, P, CO_THRESH, CO_PATH_THRESH, 
                                        RES_INFO, ONLY_MAXIMAL_PATHS, PATH_FREQUENCY, ACT_SELECTION, res_selection, 
                                        SEG_METHOD, TYPE_BASED, SEG_PERCENTILE)
    
#     # Debug: Check structure of hle_all_dic
#     if len(hle_all_dic) > 0:
#         first_key = list(hle_all_dic.keys())[0]
#         first_value = hle_all_dic[first_key]
#         print(f"\nDEBUG: First HLE key: {first_key}, type: {type(first_key)}")
#         print(f"DEBUG: First HLE value type: {type(first_value)}")
#         print(f"DEBUG: First HLE value keys: {first_value.keys() if isinstance(first_value, dict) else 'Not a dict'}")
#         print(f"DEBUG: First HLE value sample: {first_value}\n")
    
    # Print HLE table with statistics
    # print_hle_statistics(hle_all_dic)
    
#     # Debug: Print first HLA path
#     if len(hla_paths_dict) > 0:
#         first_key = list(hla_paths_dict.keys())[0]
#         first_value = hla_paths_dict[first_key]
#         print(f"\n=== HLA PATHS DICT STRUCTURE ===")
#         print(f"Total number of HLA paths: {len(hla_paths_dict)}")
#         print(f"\nFirst HLA path key: {first_key}")
#         print(f"Key type: {type(first_key)}")
#         print(f"\nFirst HLA path value: {first_value}")
#         print(f"Value type: {type(first_value)}")
#         if isinstance(first_value, tuple) and len(first_value) == 2:
#             print(f"\nValue structure: (frequency={first_value[0]}, cases={first_value[1]})")
#             print(f"Number of cases participating: {len(first_value[1])}")
#         print("="*50 + "\n")

    # Get control-flow dictionary: maps each case ID to its sequence of activity names
    control_flow_dict = case_participation.get_cf_dict(log)   
    
    # Generate DataFrame with statistics for each HLA path (frequency, participating/non-participating cases, etc.)
    df_paths = postprocess.gather_statistics(hle_all_dic, hla_paths_dict, control_flow_dict, P, CO_THRESH)
    
    # Test correlation between paths and case outcomes (success/failure) using chi-square test
    results_outcome(df_paths, successful_cases, unsuccessful_cases)
    
    

    
