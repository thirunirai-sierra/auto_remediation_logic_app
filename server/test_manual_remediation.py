import asyncio
from services.agents.orchestrator import Orchestrator
from config import get_settings

async def main():
    settings = get_settings()
    orch = Orchestrator(settings)
    # Use a run ID that exists in Log Analytics (choose one from your logs)
    result = await orch.remediate(
        workflow_name="ERROR-FLOW",
        run_id="08584229591530384809255076978CU04",
        subscription_id=settings.AZURE_SUBSCRIPTION_ID,
        resource_group=settings.AZURE_RESOURCE_GROUP,
    )
    print(result)

asyncio.run(main())