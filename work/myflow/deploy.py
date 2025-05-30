from prefect import flow
from pathlib import Path

source = str(Path.cwd())

entrypoint = f"flow.py:main_flow"

print(f'entrypoint:{entrypoint}, source:{source}')

if __name__ == "__main__":
    flow.from_source(
        source=source,
        entrypoint=entrypoint,
    ).deploy(
        name="dust_deployment",
        parameters={},
        work_pool_name="default-agent-pool",
        cron="25 * * * *", 
    )