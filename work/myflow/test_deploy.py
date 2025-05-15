import subprocess

def deploy_flow(flow_file, flow_name, deployment_name, work_queue, work_pool, cron_schedule):
    cmd = [
        "prefect", "deploy",
        f"{flow_file}:{flow_name}",
        "--name", deployment_name,
        "--work-queue", work_queue,
        "--pool", work_pool,
        "--cron", cron_schedule,
    ]
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Deployment '{deployment_name}' succeeded.")
    else:
        print(f"Deployment '{deployment_name}' failed.")
        print("stdout:", result.stdout)
        print("stderr:", result.stderr)

if __name__ == "__main__":
    deploy_flow(
        flow_file="test_flow.py",
        flow_name="data_ingestion_flow",
        deployment_name="data-ingestion",
        work_queue="default-agent-pool-2", 
        work_pool="default-agent-pool-2",
        cron_schedule="25 * * * *",
    )
    deploy_flow(
        flow_file="test_ml.py",
        flow_name="forecasting_flow",
        deployment_name="forecasting",
        work_queue="default-agent-pool",
        work_pool="default-agent-pool",
        cron_schedule="35 * * * *",
    )
