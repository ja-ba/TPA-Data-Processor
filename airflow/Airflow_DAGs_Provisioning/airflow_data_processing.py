from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.models import Variable
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

# Retrieve the value of an environment variable
package_version = Variable.get("PACKAGE_VERSION")
tpa_config = Variable.get("TPA_CONFIG")
oci_key = Variable.get("OCI_STORAGE_KEY")

sshHook1 = SSHHook(ssh_conn_id="oci_data_processing", conn_timeout=3600)

# Run DAG every day at 5am, Cron expression from https://crontab.guru/#10_4_*_*_*
with DAG(
    dag_id="DataProcessing",
    start_date=datetime(2022, 1, 1),
    schedule_interval="11 4 * * *",
    catchup=False,
) as dag:

    # Task for deleting and creating directory
    makeDir = SSHOperator(
        task_id="makeDir",
        ssh_hook=sshHook1,
        command="""rm -r -f  prod
mkdir prod -p
cd prod""",
        dag=dag,
    )

    # Task for creating a virtual environment
    createVirtualEnv = SSHOperator(
        task_id="createVirtualEnv",
        ssh_hook=sshHook1,
        command=f"""python3.9 -m pip install --user --upgrade --quiet pip
python3.9 -m venv myvirtualenv
source myvirtualenv/bin/activate
python3.9 -m pip install --upgrade pip
python -m pip install git+https://github.com/ja-ba/TAPA-Data-Processor.git@{package_version}
which python""",
        dag=dag,
        environment={"TPA_CONFIG": tpa_config},
    )

    write_config = SSHOperator(
        task_id="write_config",
        ssh_hook=sshHook1,
        command=f"""
        cat << EOF > tpa_config.yaml
        {tpa_config}
        """,
        dag=dag,
        environment={"TPA_CONFIG": tpa_config},
    )

    # Task for running python script Daily_Delta_Load.py
    DailyDeltaLoad = SSHOperator(
        task_id="DailyDeltaLoad",
        ssh_conn_id="oci_data_processing",
        command=f"""source ~/myvirtualenv/bin/activate
export OCI_KEY='{oci_key}'
python -c 'from tpa_data_processor.Loader import DailyLoader; \
currentDL = DailyLoader(config_path="tpa_config.yaml", full_load=False, verbose=True); \
currentDL.process(); \
currentDL.save_dfs();'""",
        dag=dag,
        cmd_timeout=3600,
    )

    # Task for running python script Daily_Wide_Load.py
    DailyWideLoad = SSHOperator(
        task_id="DailyWideLoad",
        ssh_conn_id="oci_data_processing",
        command=f"""source ~/myvirtualenv/bin/activate
    export OCI_KEY='{oci_key}'
    python -c 'from tpa_data_processor.Loader import DailyWideLoader; \
    currentDWL = DailyWideLoader(config_path="tpa_config.yaml", verbose=True); \
    currentDWL.process(); \
    currentDWL.save_dfs();'""",
        dag=dag,
        cmd_timeout=3600,
    )

    # Determine if the last dag has to be processed, 0=Monday, 6=Sunday
    command_weekly = (
        f"""source ~/myvirtualenv/bin/activate
    export OCI_KEY='{oci_key}'
    python -c 'from tpa_data_processor.Loader import WeeklyLoader; \
    currentDWL = WeeklyLoader(config_path="tpa_config.yaml", fullLoad=False, verbose=True); \
    currentDWL.process(); \
    currentDWL.save_dfs(); \
    currentDWL.create_upload_htmls();'
    rm tpa_config.yaml"""
        if datetime.today().weekday() == 6
        else """echo Nothing to do for WeeklyLoader
            rm tpa_config.yaml"""
    )

    # Task for running python script Weekly_Map_Load.py
    WeeklyMapLoad = SSHOperator(
        task_id="WeeklyMapLoad",
        ssh_conn_id="oci_data_processing",
        command=command_weekly,
        dag=dag,
        cmd_timeout=3600,
    )

    # makeDir >> createVirtualEnv >> DailyDeltaLoad >> DailyWideLoad >> WeeklyMapLoad
    (
        makeDir
        >> createVirtualEnv
        >> write_config
        >> DailyDeltaLoad
        >> DailyWideLoad
        >> WeeklyMapLoad
    )
