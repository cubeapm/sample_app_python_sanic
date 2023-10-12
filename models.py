from sqlalchemy import INTEGER, Column, String
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase


class BaseModel(AsyncAttrs, DeclarativeBase):
    __abstract__ = True
    id = Column(INTEGER(), primary_key=True)


class User(BaseModel):
    __tablename__ = "user"
    name = Column(String())

    def to_dict(self):
        return {"name": self.name}
