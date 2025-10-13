import pm4py
import logging
logging.basicConfig(level=logging.INFO)

def get_resources(log, to_exclude=[]):
    """
    Remove all events involving 'User_1' from the event log.

    :param log: The input event log.
    :return: The modified event log with 'User_1' events removed.
    """
    logging.info("Filter resources")
    resouces_list = []
    if to_exclude is None:
        to_exclude = []
    else:
        for trace in log:
            for event in trace:
                if event['org:resource'] not in to_exclude:
                    resouces_list.append(event['org:resource'])

    return resouces_list

def filter_uncomplete_traces(log):
    """
    Remove all incomplete traces from the event log.

    :param log: The input event log.
    :return: The modified event log with incomplete traces removed.
    """
    logging.info("Filter incomplete traces")
    complete_log = pm4py.filter_traces(lambda t: all(event['lifecycle:transition'] == 'complete' for event in t), log)
    return complete_log