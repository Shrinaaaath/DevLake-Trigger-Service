import httpx


class DevLakeClient:

    async def trigger_collection(self, repositories: list[str]):

        print("\nCalling DevLake")

        for repo in repositories:
            print(f"Collecting {repo}")

        print("Collection Triggered\n")

        #
        # Later
        #
        # await httpx.post(...)
        #