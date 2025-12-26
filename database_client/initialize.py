
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from database_client.models import Base


class DatabaseClient:
    def __init__(self, db_url: str = None):
        """
        Initialize async database client with SQLAlchemy.
        
        Args:
            db_url: PostgreSQL connection string
        """
        self.db_url = db_url or "postgresql+asyncpg://postgres:password@localhost:5432/chatdb"
        self.engine = None
        self.SessionLocal = None
    
    async def initialize(self):
        """Create engine and session factory"""
        self.engine = create_async_engine(
            self.db_url,
            echo=False,  # Set True for SQL logging
            pool_size=10,
            max_overflow=20,
        )
        
        self.SessionLocal = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        
        print(f"[DB] Connected to {self.db_url}")
    
    async def create_tables(self):
        """Create all tables (development only)"""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        print("[DB] Tables created")
    
    async def drop_tables(self):
        """Drop all tables (development only)"""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        print("[DB] Tables dropped")
    
    async def test_connection(self):
        """Test database connection"""
        async with self.engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            print(f"[DB] Connection test: {result.scalar()}")
            return True
    
    def get_session(self) -> AsyncSession:
        """Get a new database session"""
        return self.SessionLocal()
    
    async def close(self):
        """Close database connection"""
        if self.engine:
            await self.engine.dispose()
            print("[DB] Connection closed")
