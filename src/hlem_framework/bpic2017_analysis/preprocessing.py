import pm4py
import logging
logging.basicConfig(level=logging.INFO)

def get_resources(log, to_exclude=[]):
    """
    Returns a SET of resources excluding those specified.
 
    :param log: The input event log.
    :param to_exclude: List of resources to exclude.
    :return: Set of resources after exclusion.
    """
    logging.info("Filtering resources")
    resources_set = set()
   
    if to_exclude is None:
        to_exclude = []
   
    exclude_set = set(to_exclude)
   
    for trace in log:
        for event in trace:
            res = event['org:resource']
            if res not in exclude_set:
                resources_set.add(res)
 
    return resources_set
 

def filter_complete_traces(log, end_trace_events=[]):
    """
    Remove all incomplete traces from the event log.

    :param log: The input event log.
    :return: The modified event log with incomplete traces removed.
    """
    logging.info("Filter incomplete traces")
    if not end_trace_events is None:
        complete_log = pm4py.filter_event_attribute_values(log, "concept:name", end_trace_events, level="case", retain=True)
        return complete_log
    else:
        return log