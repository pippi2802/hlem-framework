import pm4py
from pm4py.objects.log.obj import EventLog, Trace
import os
import pickle
import logging
logging.basicConfig(level=logging.INFO)
from pm4py.statistics.end_activities.log import get as end_activities_get

import numpy as np
import matplotlib.pyplot as plt

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

def reduce_log_to_recent(input_xes_path, output_xes_path, max_events=120000, max_cases=30000):
    """
    Reduce the event log to the most recent max_events and max_cases.
    
    :param input_xes_path: Path to the input .xes file  
    :param output_xes_path: Path to save the reduced .xes file
    :param max_events: Maximum number of events to keep
    :param max_cases: Maximum number of unique cases to keep
    """

    log = load_event_log(input_xes_path)
    print(f"Loaded log with {len(log)} cases and {sum(len(t) for t in log)} events.")

    #  Flatten all events into a single list 
    flat_events = []
    for trace in log:
        case_id = trace.attributes.get("concept:name", "UNKNOWN_CASE")
        for event in trace:
            timestamp = event.get("time:timestamp")
            if timestamp is not None:
                flat_events.append((case_id, timestamp))

    # Sort by timestamp descending (most recent first) 
    flat_events.sort(key=lambda x: x[1], reverse=True)

    #  Keep the most recent 120k events 
    flat_events = flat_events[:max_events]

    # Extract unique case IDs (up to 30k cases) 
    selected_cases = []
    seen = set()
    for case_id, _ in flat_events:
        if case_id not in seen:
            seen.add(case_id)
            selected_cases.append(case_id)
        if len(selected_cases) >= max_cases:
            break

    # Filter original log to keep only selected cases 
    reduced_log = EventLog()
    for trace in log:
        cid = trace.attributes.get("concept:name", "UNKNOWN_CASE")
        if cid in seen:
            reduced_log.append(trace)

    print(f"Reduced to {len(reduced_log)} cases and {sum(len(t) for t in reduced_log)} events.")

    pm4py.write_xes(reduced_log, output_xes_path)
    print(f"✅ Saved reduced log to {output_xes_path}")

reduce_log_to_recent(
    input_xes_path=r"event_logs\BPI2018.xes",
    output_xes_path=r"event_logs\BPI2018_short.xes",
    max_events=120000,
    max_cases=30000
)

log = load_event_log("event_logs/BPI2018_short.xes")

# event_attributes = pm4py.get_trace_attributes(log)
# for attr in event_attributes:
#     print(f"Trace attribute: {attr}")

case_durations = []
case_ids = []

for trace in log:
    # extract timestamps from each event in the trace
    timestamps = [event["time:timestamp"] for event in trace if "time:timestamp" in event]
    
    if len(timestamps) >= 2:
        start_time = min(timestamps)
        end_time = max(timestamps)
        duration_sec = (end_time - start_time).total_seconds()
        case_durations.append(np.array(duration_sec) / (60 * 60 * 24))
        case_ids.append(trace.attributes.get("concept:name", None))

print(f"Number of cases: {len(case_durations)}")
print(f"Average duration (seconds): {np.mean(case_durations):.2f}")
print(f"Median: {np.median(case_durations):.2f}")
print(f"Min: {np.min(case_durations):.2f}")
print(f"Max: {np.max(case_durations):.2f}")

