from sqlalchemy import Column, String, Text, BigInteger, DateTime, ForeignKey, Index
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.sql import func


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Room(Base):
    __tablename__ = "rooms"
    
    room_id = Column(String(36), primary_key=True)
    room_name = Column(String(255), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationship
    messages = relationship("Message", back_populates="room", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Room(room_id={self.room_id}, name={self.room_name})>"


class Message(Base):
    __tablename__ = "messages"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    room_id = Column(String(36), ForeignKey("rooms.room_id", ondelete="CASCADE"), nullable=False)
    user_id = Column(String(100), nullable=False)
    message = Column(Text, nullable=False)
    redis_msg_id = Column(String(50), unique=True, nullable=True)  # Redis Stream msg ID
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationship
    room = relationship("Room", back_populates="messages")
    
    # Index for fast querying
    __table_args__ = (
        Index('idx_room_created', 'room_id', 'created_at'),
    )
    
    def __repr__(self):
        return f"<Message(id={self.id}, room={self.room_id}, user={self.user_id})>"
