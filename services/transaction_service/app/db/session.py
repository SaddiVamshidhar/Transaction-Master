from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

# This line creates the async engine, which will manage connections to the database.
# It uses the DB_URL from our settings file.
engine = create_async_engine(settings.DB_URL, echo=False, future=True)

# This creates a session "factory". We will call this to get a new database session
# whenever we need to interact with the database (e.g., for an API request).
AsyncSessionFactory = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def get_db_session() -> AsyncSession:
    """
    Dependency provider for FastAPI to get a DB session.
    """
    async with AsyncSessionFactory() as session:
        yield session