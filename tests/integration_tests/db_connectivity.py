"""
Simple script to test database connectivity for the NBO Pipeline.
"""
import asyncio
import sys, os
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


# Add project root to path
project_root = Path(__file__).parent.parent  # Adjust as needed
sys.path.append(str(project_root))

# Import the database connection manager
from db.connection import db_manager
from config import logger

async def test_db_connection():
    """Test connection to the database."""
    try:
        logger.info("Initializing database connection...")
        await db_manager.initialize()
        
        logger.info("Testing database connection...")
        connection = await db_manager.get_connection()
        
        # Execute a simple query to verify connection
        result = await connection.fetchval("SELECT 1")
        
        if result == 1:
            logger.info("✅ Database connection successful!")
        else:
            logger.error("❌ Database returned unexpected result")
        
        # Release the connection back to the pool
        await db_manager.release_connection(connection)
        
        # Close the pool when done
        await db_manager.close()
        
        return result == 1
        
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    logger.info("Starting database connection test...")
    success = asyncio.run(test_db_connection())
    logger.info("Test complete.")
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)