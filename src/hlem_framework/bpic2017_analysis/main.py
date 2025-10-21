import os.path
import pm4py
import math
import sys
# Add the parent folder (where hlem_with_log.py lives) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import hlem_with_paths
import logging
from preprocessing import get_resources, filter_complete_traces
logging.basicConfig(level=logging.INFO)


TRAFFIC_TYPE = 'High'
FRAME = 'days'
SELECTED_F_LIST = ['exit', 'enter', 'handover', 'workload', 'batch', 'delay']
P = 0.9
CO_THRESH = 0.5
CO_PATH_THRESH = 0.5
RES_INFO = True
FREQ = 10 # initially this was 0
ONLY_MAXIMAL_PATHS = True
PATH_FREQUENCY = 10 # initially this was 0
ACT_SELECTION = 'all'
TO_EXCLUDE = ['User_1']
SEG_METHOD = 'df'
TYPE_BASED = True
SEG_PERCENTILE = 0.75
END_TRACE_EVENTS = ['A_Cancelled', 'A_Pending', 'A_Denied']

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

    :return: a (high-level) event log
    """

    # We use this method: it implements the overlap connection rules that the paper uses to create episodes and hl-paths.
    hl_log = hlem_with_paths.paths_and_cases_with_overlap(log, frame, traffic_type, selected_f_list, p, co_thresh, co_path_thresh, res_info, only_maximal_paths, path_frequency,
         act_selection, res_selection, seg_method, type_based, seg_percentile)

    return hl_log


if __name__ == '__main__':
    current_dir = os.path.dirname(__file__)
    print("current_dir:", current_dir)
    
    current_dir = os.path.abspath(os.curdir)

    bpi2017_path = os.path.join(current_dir, "event_logs/BPI2017.xes")
    print("my_path:", bpi2017_path)
    
    #log = pm4py.read_xes(bpi2017_path, return_legacy_log_object=True)

    log = pm4py.read_xes(bpi2017_path)
    logging.info("Log type is " + str(type(log)))
    logging.info('The log has ' + str(len(log)) + ' traces.')

    no_events = sum([len(trace) for trace in log])
    logging.info('The log has ' + str(no_events) + ' events.')

    # filter event log for copmlete traces only
    log = filter_complete_traces(log, END_TRACE_EVENTS)
    logging.info('The filtered log has ' + str(len(log)) + ' traces after filtering incomplete traces.')

    # remove User_1 from the log (project log)
    logging.info(f"Get all human resouces from log with type {str(type(log))}")
    res_selection = get_resources(log, TO_EXCLUDE)

    print("Running main...")
    main(log, FRAME, TRAFFIC_TYPE, SELECTED_F_LIST, P, CO_THRESH, CO_PATH_THRESH, RES_INFO, ONLY_MAXIMAL_PATHS,
          PATH_FREQUENCY, ACT_SELECTION, res_selection, SEG_METHOD, TYPE_BASED, SEG_PERCENTILE)