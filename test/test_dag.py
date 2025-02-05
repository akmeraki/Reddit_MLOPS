from airflow.models import DagBag

dagbag = DagBag("/Users/ajithkumarg/Library/CloudStorage/GoogleDrive-ak1616244@gmail.com/My Drive/Ajith's Work/Reddit_MLOps/dags")
print(f"Found {len(dagbag.dags)} DAGs")
print("\nDAGs found:")
for dag_id in dagbag.dags:
    print(dag_id)
print("\nImport Errors:")
print(dagbag.import_errors)
