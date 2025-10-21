import logging
import pandas as pd
from pm4py.objects.log.obj import EventLog 
from pm4py.algo.filtering.log.attributes import attributes_filter

logging.basicConfig(level=logging.INFO)

def get_resources(log, to_exclude=None):
    """
    Returns a SET of resources excluding those specified.

    :param log: The input event log (either a pm4py EventLog or a pandas DataFrame).
    :param to_exclude: List of resources to exclude.
    :return: Set of resources after exclusion.
    """
    logging.info("Filtering resources")
    if to_exclude is None:
        to_exclude = []

    exclude_set = set(to_exclude)
    resources_set = set()

    print(f"Log is of type {type(log)} in get_resources()")

    # Case 1: Pandas DataFrame
    if isinstance(log, pd.DataFrame):
        if 'org:resource' not in log.columns:
            raise KeyError("DataFrame must contain 'org:resource' column.")
        print(log.columns)
        print(log.head(10))

        resources_set = set(log['org:resource'].unique()) - exclude_set

    # Case 2: PM4Py EventLog
    elif isinstance(log, EventLog):
        for trace in log:
            for event in trace:
                res = event.get('org:resource')
                if res and res not in exclude_set:
                    resources_set.add(res)

    else:
        raise TypeError(f"Unsupported log type: {type(log)}")

    return resources_set


def rename_workflow_activities(log):
    """
    Renames workflow activities by appending the lifecycle transition to the activity name.
    
    :param log: The input event log.
    :return: The modified event log with renamed activities.
    """
    logging.info("Renaming workflow activities")
    for trace in log:
        for event in trace:
            act_name = event.get('concept:name', '')
            if act_name.startswith('W'):
                lifecycle = event.get('lifecycle:transition', 'Unknown')
                event['concept:name'] = f"{act_name}|{lifecycle}"
    
    return log 

def partition_outcome(log):
    """
    Partitions the event log into successful and unsuccessful cases.
    A successful case contains the activity 'A_Pending'.
    
    :param log: The input event log.
    :return: successful_case_ids, unsuccessful_case_ids (lists of case indices)
    """
    logging.info("Partitioning cases by outcome")
    successful_case_ids = []
    unsuccessful_case_ids = []
    
    for i, trace in enumerate(log):
        if any(event.get('concept:name') == 'A_Pending' for event in trace):
            successful_case_ids.append(i)
        else: 
            unsuccessful_case_ids.append(i)
    
    total_cases = len(log)
    num_successful = len(successful_case_ids)
    num_unsuccessful = len(unsuccessful_case_ids)
    
    success_rate = (num_successful / total_cases * 100) if total_cases > 0 else 0
    unsuccessful_rate = (num_unsuccessful / total_cases * 100) if total_cases > 0 else 0
    
    logging.info(f"Total cases: {total_cases}")
    logging.info(f"Successful cases: {num_successful} ({success_rate:.2f}%)")
    logging.info(f"Unsuccessful cases: {num_unsuccessful} ({unsuccessful_rate:.2f}%)")

    return successful_case_ids, unsuccessful_case_ids

def partition_on_throughput(log):
    """
    Partitions the event log into three throughput time categoeries:
    - <= 10 days;
    - 10 > t >= 30 days;
    - > 30 days.

    :param log: The input event log.
    :return: class_under_10, class_10_to_30, class_over_30
    """
    class_under_10 = []
    class_10_to_30 = []
    class_over_30 = []
    for i, trace in enumerate(log):
        ts_first = trace[0]['time:timestamp']
        ts_last = trace[len(trace)-1]['time:timestamp']
        throughput = (ts_last - ts_first).days
        if throughput <= 10:
            class_under_10.append(i)
        elif throughput < 30:
            class_10_to_30.append(i)
        else:
            class_over_30.append(i)

    logging.info(f"Total cases under 10: {len(class_under_10)}")
    logging.info(f"Total cases between 10 and 30: {len(class_10_to_30)}")
    logging.info(f"Total cases over 10: {len(class_over_30)}")
    return class_under_10, class_10_to_30, class_over_30

def filter_incomplete_cases(log):
    """
    Filters out incomplete cases from the event log.
    Keep only cases that contain at least one completion activity.
    """
    completion_activities = ['A_Cancelled', 'A_Pending', 'A_Denied']
    logging.info(f"Filtering incomplete cases")
    
    original_count = len(log)
    
    filtered_log = attributes_filter.apply(log, completion_activities, 
                                           parameters={attributes_filter.Parameters.ATTRIBUTE_KEY: "concept:name",
                                                      attributes_filter.Parameters.POSITIVE: True})
    
    filtered_count = len(filtered_log)
    removed_count = original_count - filtered_count
    
    logging.info(f"Filtered {removed_count} incomplete cases out of {original_count} total")
    logging.info(f"Remaining: {filtered_count} cases ({filtered_count/original_count*100:.2f}%)")
    
    return filtered_log


