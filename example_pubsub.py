import asyncio
import os

from custom_clients import CustomPubSubClient


async def main() -> None:
    subscription = os.environ.get("SUBSCRIPTION")
    async with CustomPubSubClient() as pb_client:
        batch = await pb_client.pull_messages(
            subscription=subscription, batch_size=10
        )
        print("\n\nReceived batch of messages:", batch)

        ids = [message["ackId"] for message in batch]
        if len(ids):
            print("\n\nAcknowledging ids:", ids)
            await pb_client.acknowledge_messages(
                subscription=subscription, acknowledge_ids=ids
            )
            print("Acknowledged")


if __name__ == "__main__":
    asyncio.run(main())
