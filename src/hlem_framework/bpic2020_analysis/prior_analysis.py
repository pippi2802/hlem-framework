import pm4py
from pm4py.statistics.end_activities.log import get as end_activities_get
import pickle
import os
import logging
logging.basicConfig(level=logging.INFO)

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


# Load your event log (example path)
log = load_event_log(r"event_logs\BPI2012.xes")

# Get end activities and their frequencies
end_activities_freq = end_activities_get.get_end_activities(log)

# Print results
print("End activities and their frequencies:")
for activity, freq in end_activities_freq.items():
    print(f"{activity}: {freq}")
