import logging
import pandas as pd
from pm4py.objects.log.obj import EventLog 
from pm4py.algo.filtering.log.attributes import attributes_filter
from datetime import datetime
import random

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

def partition_outcome(log):
    """
    Partitions the event log into successful and unsuccessful cases.
    
    :param log: The input event log.
    :return: successful_case_ids, unsuccessful_case_ids (lists of case indices)
    """
    logging.info("Partitioning cases by outcome")
    successful_case_ids = []
    unsuccessful_case_ids = []
    
    for i, trace in enumerate(log):
        if any(event.get('concept:name') == 'finish payment' for event in trace):
            successful_case_ids.append(i)
        else: 
            unsuccessful_case_ids.append(i)
    
    total_cases = len(log)
    num_successful = len(successful_case_ids)
    num_unsuccessful = len(unsuccessful_case_ids)
    
    success_rate = (num_successful / total_cases * 100) if total_cases > 0 else 0
    unsuccessful_rate = (num_unsuccessful / total_cases * 100) if total_cases > 0 else 0
    
    logging.info(f"Total cases: {total_cases}")
    logging.info(f"Successful cases (with 'finish payment'): {num_successful} ({success_rate:.2f}%)")
    logging.info(f"Unsuccessful cases (without 'finish payment'): {num_unsuccessful} ({unsuccessful_rate:.2f}%)")

    return successful_case_ids, unsuccessful_case_ids

def partition_on_throughput(log):
    """
    Partitions the event log into two throughput time categories:
    - <= 250 days;
    - > 250 days.

    :param log: The input event log.
    :return: class_under_250, class_over_250
    """
    class_under_250 = []
    class_over_250 = []
    for i, trace in enumerate(log):
        ts_first = trace[0]['time:timestamp']
        ts_last = trace[len(trace)-1]['time:timestamp']
        throughput = (ts_last - ts_first).days
        if throughput <= 250:
            class_under_250.append(i)
        else:
            class_over_250.append(i)

    logging.info(f"Total cases under or equal to 250 days: {len(class_under_250)}")
    logging.info(f"Total cases over 250 days: {len(class_over_250)}")
    return class_under_250, class_over_250

def partition_on_departments(log, department_attribute='department'):
    """
    Partitions the BPIC2018 event log based on the department case attribute.
    Returns lists of case indices for departments: 4e, e7, 6b, d4
    
    :param log: The input event log.
    :param department_attribute: The case attribute containing department info
    :return: dept_4e, dept_e7, dept_6b, dept_d4
    """
    logging.info(f"Partitioning cases by department using case attribute '{department_attribute}'")
    
    dept_4e = []
    dept_e7 = []
    dept_6b = []
    dept_d4 = []
    other_depts = []
    
    for i, trace in enumerate(log):
        # Get department from case attributes
        if hasattr(trace, 'attributes') and department_attribute in trace.attributes:
            dept = trace.attributes[department_attribute]
            
            if dept == '4e':
                dept_4e.append(i)
            elif dept == 'e7':
                dept_e7.append(i)
            elif dept == '6b':
                dept_6b.append(i)
            elif dept == 'd4':
                dept_d4.append(i)
            else:
                other_depts.append(i)
        else:
            other_depts.append(i)
    
    # Log statistics
    total_cases = len(log)
    logging.info(f"Total cases: {total_cases}")
    logging.info(f"  Department '4e': {len(dept_4e)} cases ({len(dept_4e)/total_cases*100:.2f}%)")
    logging.info(f"  Department 'e7': {len(dept_e7)} cases ({len(dept_e7)/total_cases*100:.2f}%)")
    logging.info(f"  Department '6b': {len(dept_6b)} cases ({len(dept_6b)/total_cases*100:.2f}%)")
    logging.info(f"  Department 'd4': {len(dept_d4)} cases ({len(dept_d4)/total_cases*100:.2f}%)")
    
    if other_depts:
        logging.info(f"  Other/Unassigned:    {len(other_depts)} cases ({len(other_depts)/total_cases*100:.2f}%)")
    
    return dept_4e, dept_e7, dept_6b, dept_d4

def filter_incomplete_cases(log):
    """
    Filters out incomplete cases from the event log.
    Keep only cases that contain the 'finish payment' activity.
    """
    completion_activity = ['finish payment']
    logging.info(f"Filtering incomplete cases - keeping only traces with '{completion_activity}'")
    
    original_count = len(log)
    
    filtered_log = attributes_filter.apply(log, completion_activity, 
                                           parameters={attributes_filter.Parameters.ATTRIBUTE_KEY: "concept:name",
                                                      attributes_filter.Parameters.POSITIVE: True})
    
    filtered_count = len(filtered_log)
    removed_count = original_count - filtered_count
    
    logging.info(f"Filtered {removed_count} incomplete cases out of {original_count} total")
    logging.info(f"Remaining: {filtered_count} cases ({filtered_count/original_count*100:.2f}%)")
    
    return filtered_log

def filter_cases_by_year(log, year):
    """
    Filters the event log to keep only cases that started in a specific year.
    
    :param log: The input event log.
    :param year: The year to filter for
    :return: Filtered event log with only cases from the specified year
    """
    logging.info(f"Filtering cases to keep only those from year {year}")
    
    original_count = len(log)
    filtered_log = EventLog()
    
    for trace in log:
        if len(trace) > 0:
            # Get the timestamp of the first event in the trace
            first_event = trace[0]
            timestamp = first_event.get('time:timestamp')
            
            if timestamp and timestamp.year == year:
                filtered_log.append(trace)
    
    filtered_count = len(filtered_log)
    removed_count = original_count - filtered_count
    
    logging.info(f"Kept {filtered_count} cases from year {year} (removed {removed_count} cases)")
    logging.info(f"Remaining: {filtered_count} cases ({filtered_count/original_count*100:.2f}%)")
    
    return filtered_log

def filter_cases_by_date_range(log, start_year, start_month, end_year, end_month):
    """
    Filters the event log to keep only cases that started within a specific date range.
    
    :param log: The input event log.
    :param start_year: Starting year (2016)
    :param start_month: Starting month (1-12)
    :param end_year: Ending year (2016)
    :param end_month: Ending month (1-12)
    :return: Filtered event log
    """
    
    logging.info(f"Filtering cases from {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
    
    original_count = len(log)
    filtered_log = EventLog()
    
    for trace in log:
        if len(trace) > 0:
            first_event = trace[0]
            timestamp = first_event.get('time:timestamp')
            
            if timestamp:
                # Check if within range
                if ((timestamp.year > start_year) or 
                    (timestamp.year == start_year and timestamp.month >= start_month)):
                    if ((timestamp.year < end_year) or 
                        (timestamp.year == end_year and timestamp.month <= end_month)):
                        filtered_log.append(trace)
    
    filtered_count = len(filtered_log)
    removed_count = original_count - filtered_count
    
    logging.info(f"Kept {filtered_count} cases (removed {removed_count} cases)")
    logging.info(f"Remaining: {filtered_count} cases ({filtered_count/original_count*100:.2f}%)")
    
    return filtered_log

def sample_cases(log, sample_size=None, sample_fraction=0.5, random_seed=42):
    """
    Takes a random sample of cases from the log.
    
    :param log: The input event log.
    :param sample_size: Exact number of cases to sample (if specified, overrides sample_fraction)
    :param sample_fraction: Fraction of cases to sample (default: 0.5 = 50%)
    :param random_seed: Random seed for reproducibility
    :return: Sampled event log
    """   
    original_count = len(log)
    
    if sample_size is not None:
        n = min(sample_size, original_count)
    else:
        n = int(original_count * sample_fraction)
    
    logging.info(f"Sampling {n} cases from {original_count} total cases")
    
    # Set random seed for reproducibility
    random.seed(random_seed)
    
    # Randomly sample indices
    sampled_indices = random.sample(range(original_count), n)
    
    # Create new log with sampled cases
    filtered_log = EventLog()
    for idx in sorted(sampled_indices):
        filtered_log.append(log[idx])
    
    logging.info(f"Sampled {len(filtered_log)} cases ({len(filtered_log)/original_count*100:.2f}%)")
    
    return filtered_log





