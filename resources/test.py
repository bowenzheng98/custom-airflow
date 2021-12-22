import pandas as pd
import os

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
file = pd.read_csv(AIRFLOW_HOME+ '/resources/test.csv')
print("CSV File contents " + file)