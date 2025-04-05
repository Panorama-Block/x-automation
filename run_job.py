import asyncio
import sys
import warnings
from app import job

async def main():
    try:
        await job()
        for warning in warnings.warnings_occurred:
            if isinstance(warning.message, ResourceWarning) and "unclosed" in str(warning.message).lower():
                raise RuntimeError("Detected unclosed client session - forcing program termination")
        print("Job executed successfully")
    except Exception as e:
        print(f"Error executing job: {e}")
        sys.exit(1)

if __name__ == "__main__":
    warnings.simplefilter("always", ResourceWarning)
    warnings.warnings_occurred = []
    warnings.showwarning = lambda message, category, filename, lineno, file=None, line=None: \
        warnings.warnings_occurred.append(warnings.WarningMessage(message, category, filename, lineno, file, line))
    
    asyncio.run(main())
