import os
import sys
import pickle
import pm4py
import pandas as pd
from collections import Counter, defaultdict
from datetime import timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def load_log():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(current_dir, "event_logs", "BPIC2018.xes")
    cache_path = log_path.replace('.xes', '.pickle')
    
    if os.path.isfile(cache_path):
        print(f"Loading from cache: {cache_path}")
        with open(cache_path, 'rb') as f:
            log = pickle.load(f)
            log = pm4py.convert_to_event_log(log)
        print(f"Done\n")
    else:
        print(f"Loading XES file: {log_path}")
        log = pm4py.read_xes(log_path, return_legacy_log_object=True)
        print(f"Loaded")
        
        print(f"Saving cache: {cache_path}")
        with open(cache_path, 'wb') as f:
            pickle.dump(log, f)
        print(f"Done\n")
    
    return log


def basic_statistics(log):
    print("=" * 80)
    print("BASIC STATISTICS")
    print("=" * 80)
    
    num_cases = len(log)
    num_events = sum(len(trace) for trace in log)
    avg_events_per_case = num_events / num_cases if num_cases > 0 else 0
    
    print(f"Cases:                    {num_cases:,}")
    print(f"Events:                   {num_events:,}")
    print(f"Avg events per case:      {avg_events_per_case:.2f}")
    print()


def discover_attributes(log):
    print("=" * 80)
    print("ATTRIBUTES")
    print("=" * 80)
    
    trace_attributes = set()
    for trace in log:
        if hasattr(trace, 'attributes'):
            trace_attributes.update(trace.attributes.keys())
    
    event_attributes = set()
    for trace in log:
        for event in trace:
            event_attributes.update(event.keys())
    
    print("CASE-LEVEL ATTRIBUTES:")
    if trace_attributes:
        for attr in sorted(trace_attributes):
            sample_value = None
            for trace in log:
                if hasattr(trace, 'attributes') and attr in trace.attributes:
                    sample_value = trace.attributes[attr]
                    break
            print(f"  {attr:<40} (example: {sample_value})")
    else:
        print("  none")
    
    print()
    print("EVENT-LEVEL ATTRIBUTES:")
    if event_attributes:
        for attr in sorted(event_attributes):
            sample_value = None
            for trace in log:
                for event in trace:
                    if attr in event:
                        sample_value = event[attr]
                        break
                if sample_value is not None:
                    break
            
            sample_str = str(sample_value)
            if len(sample_str) > 50:
                sample_str = sample_str[:47] + "..."
            
            print(f"  {attr:<40} (example: {sample_str})")
    else:
        print("  none")
    
    print()
    print("DEPARTMENT/ROLE RELATED:")
    dept_attrs = [attr for attr in event_attributes 
                  if any(kw in attr.lower() 
                         for kw in ['role', 'department', 'dept', 'group', 'org', 'team'])]
    
    if dept_attrs:
        for attr in sorted(dept_attrs):
            unique_vals = set()
            for trace in log:
                for event in trace:
                    val = event.get(attr)
                    if val:
                        unique_vals.add(val)
            
            print(f"  {attr:<40} ({len(unique_vals)} unique)")
            if len(unique_vals) <= 10:
                print(f"    → {', '.join(sorted(unique_vals))}")
            else:
                print(f"    → {', '.join(list(sorted(unique_vals))[:5])}...")
    else:
        print("  none obvious")
    
    print()


def analyze_activities(log):
    print("=" * 80)
    print("ACTIVITIES")
    print("=" * 80)
    
    activity_counter = Counter()
    for trace in log:
        for event in trace:
            activity = event.get('concept:name', 'Unknown')
            activity_counter[activity] += 1
    
    print(f"Unique activities: {len(activity_counter)}")
    print()
    
    print("Top 15 most frequent:")
    print(f"{'Activity':<50} {'Count':>10} {'%':>8}")
    print("-" * 80)
    
    total = sum(activity_counter.values())
    for activity, count in activity_counter.most_common(15):
        pct = (count / total) * 100
        print(f"{activity:<50} {count:>10,} {pct:>7.2f}%")
    print()


def analyze_resources(log):
    print("=" * 80)
    print("RESOURCES")
    print("=" * 80)
    
    resource_counter = Counter()
    events_with_resource = 0
    
    for trace in log:
        for event in trace:
            resource = event.get('org:resource')
            if resource:
                resource_counter[resource] += 1
                events_with_resource += 1
    
    if resource_counter:
        print(f"Unique resources: {len(resource_counter)}")
        print(f"Events with resource: {events_with_resource:,}")
        print()
        
        print("Top 15 most active:")
        print(f"{'Resource':<40} {'Events':>15} {'%':>8}")
        print("-" * 80)
        
        for resource, count in resource_counter.most_common(15):
            pct = (count / events_with_resource) * 100
            print(f"{resource:<40} {count:>15,} {pct:>7.2f}%")
    else:
        print("No resource info found.")
    print()


def analyze_lifecycle(log):
    print("=" * 80)
    print("LIFECYCLE TRANSITIONS")
    print("=" * 80)
    
    lifecycle_counter = Counter()
    
    for trace in log:
        for event in trace:
            lifecycle = event.get('lifecycle:transition', 'Unknown')
            lifecycle_counter[lifecycle] += 1
    
    print(f"{'Transition':<30} {'Count':>10} {'%':>8}")
    print("-" * 80)
    
    total = sum(lifecycle_counter.values())
    for transition, count in lifecycle_counter.most_common():
        pct = (count / total) * 100
        print(f"{transition:<30} {count:>10,} {pct:>7.2f}%")
    print()


def analyze_departments_per_case(log, case_dept_attribute='department', event_dept_attribute='org:resource'):
    """
    Department distribution.
    """
    print("=" * 80)
    print("DEPARTMENTS")
    print("=" * 80)
    
    print(f"Looking at case attribute: '{case_dept_attribute}'")
    department_distribution = Counter()
    no_dept_cases = 0
    
    for trace in log:
        if hasattr(trace, 'attributes') and case_dept_attribute in trace.attributes:
            dept = trace.attributes[case_dept_attribute]
            department_distribution[dept] += 1
        else:
            no_dept_cases += 1
    
    total_cases = len(log)
    
    if department_distribution:
        print(f"Unique departments: {len(department_distribution)}")
        print()
        
        print("Distribution:")
        print(f"{'Department':<40} {'Cases':>10} {'%':>8}")
        print("-" * 80)
        
        for dept, count in department_distribution.most_common():
            pct = (count / total_cases) * 100
            print(f"{dept:<40} {count:>10,} {pct:>7.2f}%")
        
        if no_dept_cases > 0:
            print(f"{'(no dept info)':<40} {no_dept_cases:>10,} {no_dept_cases/total_cases*100:>7.2f}%")
    else:
        print(f"No department info found.")
    
    print()
    
    # Check if multiple resources work on cases
    print(f"Resources per case (checking '{event_dept_attribute}'):")
    print("-" * 80)
    
    multi_resource_cases = 0
    single_resource_cases = 0
    
    for trace in log:
        resource_counter = Counter()
        for event in trace:
            resource = event.get(event_dept_attribute)
            if resource and resource != '0;n/a':
                resource_counter[resource] += 1
        
        if len(resource_counter) > 1:
            multi_resource_cases += 1
        elif len(resource_counter) == 1:
            single_resource_cases += 1
    
    print(f"Single resource:    {single_resource_cases:>6,} ({single_resource_cases/total_cases*100:>5.1f}%)")
    print(f"Multiple resources: {multi_resource_cases:>6,} ({multi_resource_cases/total_cases*100:>5.1f}%)")
    print()

def analyze_throughput(log):
    print("=" * 80)
    print("THROUGHPUT TIME")
    print("=" * 80)
    
    throughput_times = []
    
    for trace in log:
        if len(trace) > 0:
            start = trace[0]['time:timestamp']
            end = trace[-1]['time:timestamp']
            days = (end - start).total_seconds() / (24 * 3600)
            throughput_times.append(days)
    
    if throughput_times:
        throughput_times.sort()
        
        min_time = min(throughput_times)
        max_time = max(throughput_times)
        avg_time = sum(throughput_times) / len(throughput_times)
        median_time = throughput_times[len(throughput_times) // 2]
        
        print(f"Min:     {min_time:>10.2f} days")
        print(f"Max:     {max_time:>10.2f} days")
        print(f"Average: {avg_time:>10.2f} days")
        print(f"Median:  {median_time:>10.2f} days")
    print()


def analyze_case_variants(log):
    print("=" * 80)
    print("CASE VARIANTS")
    print("=" * 80)
    
    variants = {}
    for trace in log:
        variant = tuple(event.get('concept:name', 'Unknown') for event in trace)
        if variant in variants:
            variants[variant] += 1
        else:
            variants[variant] = 1
    
    num_variants = len(variants)
    num_cases = len(log)
    
    print(f"Unique variants: {num_variants:,}")
    print(f"Avg cases per variant: {num_cases/num_variants:.2f}")
    print()
    
    sorted_variants = sorted(variants.items(), key=lambda x: x[1], reverse=True)
    
    print("Top 10:")
    print(f"{'#':<6} {'Freq':>10} {'%':>8} {'Len':>8} Preview")
    print("-" * 80)
    
    for i, (variant, freq) in enumerate(sorted_variants[:10], 1):
        pct = (freq / num_cases) * 100
        preview = ' → '.join(variant[:3]) + (' → ...' if len(variant) > 3 else '')
        print(f"{i:<6} {freq:>10,} {pct:>7.2f}% {len(variant):>8} {preview[:60]}")
    print()


def main():
    print("\n" + "=" * 80)
    print("BPIC2018 LOG EXPLORATION")
    print("=" * 80 + "\n")
    
    log = load_log()
    
    discover_attributes(log)
    basic_statistics(log)
    analyze_activities(log)
    analyze_resources(log)
    analyze_lifecycle(log)
    analyze_departments_per_case(log)
    analyze_throughput(log)
    analyze_case_variants(log)


if __name__ == '__main__':
    main()
