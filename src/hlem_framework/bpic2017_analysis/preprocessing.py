import logging
from pm4py.algo.filtering.log.attributes import attributes_filter

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


