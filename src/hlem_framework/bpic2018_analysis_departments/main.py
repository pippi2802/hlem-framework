import os.path
import pickle
import pm4py
import sys
# Add the parent folder (where hlem_with_log.py lives) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import hlem_with_paths
from hl_paths import postprocess, case_participation
import logging
import preprocessing 
import results_analysis
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

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
TO_EXCLUDE = None
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
            cache = pickle.load(f)
            return pm4py.convert_to_event_log(cache)
    else:
        logging.info(f'Reading XES file: {xes_path}')
        log = pm4py.read_xes(xes_path, return_legacy_log_object=True)
        logging.info(f'Creating cache: {cache_path}')
        with open(cache_path, 'wb') as f:
            pickle.dump(log, f)
        return log


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


if __name__ == '__main__':
    current_dir = os.path.abspath(os.curdir)
    bpi2018_path = os.path.join(current_dir, "event_logs/BPIC2018.xes")

    # Load log
    log = load_event_log(bpi2018_path)
    logging.info('The log has ' + str(len(log)) + ' traces.')

    no_events = sum([len(trace) for trace in log])
    logging.info('The log has ' + str(no_events) + ' events.')
    
    # Filter for 2016 cases only
    log = preprocessing.filter_cases_by_year(log, 2016)
    
    # Use random sampling to reduce the number of cases and thus the HLEs
    log = preprocessing.sample_cases(log, sample_fraction=0.30)
    
    # Filter out incomplete cases (keep only cases with 'finish payment')
    log = preprocessing.filter_incomplete_cases(log)
    logging.info(f'After filtering incomplete cases: {len(log)} traces.')
    
    # Get resource selection
    logging.info("Getting resource selection")
    res_selection = preprocessing.get_resources(log, TO_EXCLUDE)
    
    # Partition by department (4 main departments)
    logging.info("Partitioning cases by department")
    dept_4e, dept_e7, dept_6b, dept_d4 = preprocessing.partition_on_departments(log)
    logging.info(f"Department distribution - 4e: {len(dept_4e)}, e7: {len(dept_e7)}, 6b: {len(dept_6b)}, d4: {len(dept_d4)}")

    print("Running main...")
    hle_all_dic, hla_paths_dict = main(log, FRAME, TRAFFIC_TYPE, SELECTED_F_LIST, P, CO_THRESH, CO_PATH_THRESH, 
                                        RES_INFO, ONLY_MAXIMAL_PATHS, PATH_FREQUENCY, ACT_SELECTION, res_selection, 
                                        SEG_METHOD, TYPE_BASED, SEG_PERCENTILE)
    
    # Get control-flow dictionary: maps each case ID to its sequence of activity names
    control_flow_dict = case_participation.get_cf_dict(log)   
    
    # Generate DataFrame with statistics for each HLA path (frequency, participating/non-participating cases, etc.)
    logging.info("Gather statistics on High-Level Attributes")
    df_paths = postprocess.gather_statistics(hle_all_dic, hla_paths_dict, control_flow_dict, P, CO_THRESH)
    
    # Analyze paths by department correlation
    logging.info("Produce result tables for department analysis")
    dept_results = results_analysis.department_tables(df_paths, [dept_4e, dept_e7, dept_6b, dept_d4])
    
    logging.info("Department Analysis Summary:")
    for dept_name, dept_df in dept_results.items():
        logging.info(f"Department {dept_name}: {len(dept_df)} significant paths")
    
