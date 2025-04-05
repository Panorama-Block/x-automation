import asyncio
from app import job

async def main():
    try:
        await job()
        print("Job executed successfully")
    except Exception as e:
        print(f"Error executing job: {e}")


if __name__ == "__main__":
    asyncio.run(main())
