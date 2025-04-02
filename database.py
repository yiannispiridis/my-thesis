import asyncpg
import logging
from config import DB_CONFIG

class Database:
    def __init__(self, db_config):
        self.db_config = db_config
        self.pool = None

    async def init_pool(self):
        """Initialize the database connection pool."""
        self.pool = await asyncpg.create_pool(**self.db_config)
        logging.info("Database pool initialized.")

    async def close_pool(self):
        """Close the database connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logging.info("Database pool closed.")

db = Database(DB_CONFIG)
