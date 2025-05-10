# from prefect import flow
# from pathlib import Path
# from flow import main_flow

# if __name__ == "__main__":
#     main_flow.deploy(
#         name="dust_deployment",
#         parameters={},
#         work_pool_name="default-agent-pool",
#         schedule={"cron": "0 * * * *"},  # Run every hour
#     )

from prefect import flow
from pathlib import Path

source = str(Path.cwd())
entrypoint = f"flow.py:main_flow"  # python file: function
print(f'entrypoint:{entrypoint}, source:{source}')

if __name__ == "__main__":
    flow.from_source(
        source=source,
        entrypoint=entrypoint,
    ).deploy(
        name="dust_deployment",
        parameters={},
        work_pool_name="default-agent-pool",
        cron="0 * * * *",  # Run every 5 minutes
    )
