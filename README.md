# High-Level Event Mining: A Framework (Replication of ICPM 2022)

This repository contains the replication study for the paper:  
> **“The interplay between high-level problems and the process instances that give rise to them”**  
> *Bakullari et al., ICPM 2022*

The goal of this project is to replicate the original analysis on the **BPIC 2017** event log using the **High-Level Event Mining (HLEM)** framework, and to extend it to additional datasets such as BPIC 2018.  
All replication details and results are documented in the accompanying report.

---

## Project Structure
```
├── src/
│ └── hlem_framework/
│ ├── bpic2017_analysis/
│ │ ├── main.py
│ │ ├── preprocessing.py
│ │ ├── results_analysis.py
│ │ ├── statistics_csv_experiment.py
│ │ ├── results/ 
│ │ └── event_logs/ # Place dataset here
│ ├── bpic2018_analysis/
│ │ ├── main.py
│ │ ├── preprocessing.py
│ │ ├── results_analysis.py
│ │ ├── utils.py
│ │ ├── analysis.py
│ │ ├── results/ 
│ │ └── event_logs/ # Place dataset here
│ ├── bpic2018_analysis/
│ │ ├── main.py
│ │ ├── preprocessing.py
│ │ ├── results_analysis.py
│ │ ├── prior_analysis.py
│ │ ├── results/ 
│ │ └── event_logs/ # Place dataset here
│ └── ...
├── requirements.txt
├── .venv1
├── event_logs
├── Jane.cpn
├── pyproject.toml
└── README.md

```
---

## Installation

We recommend running the project in a **virtual environment** to ensure reproducibility.

### 1. Create a virtual environment
```bash
python -m venv venv
```

### 2. Activate the environment
Windows:

```bash
.\venv\Scripts\activate
```
macOS / Linux:

```bash
source venv/bin/activate
```
### 3. Install dependencies
```bash
pip install -r requirements.txt
```

## Running the Framework
To execute the analysis for a given BPIC dataset (e.g., BPIC 2017):
### 1. Navigate to the corresponding analysis directory:

```bash
cd src/hlem_framework/bpic2017_analysis
```

### 2. Run the main script:

```bash
python main.py
```
This script performs the full high-level event mining workflow, including:
- Loading and preprocessing the event log
- Detecting high-level events, episodes, and paths
- Performing statistical analyses on success rate and throughput time

## Dataset Setup
**Important**: The event logs are not included in this repository due to file size limitations.

To reproduce the experiments:

Download the BPIC 2017 event log from the official 4TU Research Data repository:
> https://doi.org/10.4121/uuid:5f3067df-f10b-45da-b98b-86ae4c7a310b

Place the downloaded .xes file in:
```
src/hlem_framework/bpic2017_analysis/event_logs/
```
Rename the file as follows (to match the default path in main.py):
```
BPIC2017.xes
```

(If you use a different name, update the file path in main.py accordingly.)

## References

Bakullari, B. et al. (2022). The interplay between high-level problems and the process instances that give rise to them. In International Conference on Business Process Management (ICPM 2022).

BPI Challenge 2017 dataset: https://doi.org/10.4121/uuid:5f3067df-f10b-45da-b98b-86ae4c7a310b

## Authors

Denitsa Gincheva & Silvia Brighi

Eindhoven University of Technology

## Acknowledgment

This work was conducted as part of the Seminar Process Analytics course at Eindhoven University of Technology.