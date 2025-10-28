from typing import Generic, TypeVar, Type, List, Optional, Any, Dict
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func, and_
from sqlalchemy.exc import SQLAlchemyError
from app.db import Base

ModelType = TypeVar("ModelType", bound=Base)

class AsyncBaseRepository(Generic[ModelType]):
    def __init__(self, model: Type[ModelType]):
        self.model = model

    def _get_base_query(self, include_deleted: bool = False):
        """
        Get base select statement with soft delete filter.
        """
        stmt = select(self.model)
        if not include_deleted and hasattr(self.model, 'is_deleted'):
            stmt = stmt.where(self.model.is_deleted == False)
        return stmt

    async def create(self, db: AsyncSession, *, obj_in: Dict[str, Any]) -> ModelType:
        """
        Create a new record.
        """
        try:
            db_obj = self.model(**obj_in)
            db.add(db_obj)
            await db.flush()  # Get ID without committing transaction
            await db.refresh(db_obj)
            return db_obj
        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def create_many(self, db: AsyncSession, *, objs_in: List[Dict[str, Any]]) -> List[ModelType]:
        """
        Create multiple new records.
        """
        try:
            db_objs = [self.model(**obj_in) for obj_in in objs_in]
            db.add_all(db_objs)
            await db.flush()
            # Refresh all objects to get their IDs
            for db_obj in db_objs:
                await db.refresh(db_obj)
            return db_objs
        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def get(self, db: AsyncSession, id: Any, include_deleted: bool = False) -> Optional[ModelType]:
        """
        Get a record by id.
        """
        stmt = self._get_base_query(include_deleted).where(self.model.id == id)
        result = await db.execute(stmt)
        return result.scalar_one_or_none()

    async def get_all(self, db: AsyncSession, include_deleted: bool = False) -> List[ModelType]:
        """
        Get all records.
        """
        stmt = self._get_base_query(include_deleted)
        result = await db.execute(stmt)
        return list(result.scalars().all())

    async def get_paginated(
            self,
            db: AsyncSession,
            skip: int = 0,
            limit: int = 100,
            include_deleted: bool = False
    ) -> List[ModelType]:
        """
        Get paginated records.
        """
        stmt = self._get_base_query(include_deleted).offset(skip).limit(limit)
        result = await db.execute(stmt)
        return list(result.scalars().all())

    async def get_by_condition(
            self,
            db: AsyncSession,
            condition: Dict[str, Any],
            include_deleted: bool = False,
            limit: Optional[int] = None
    ) -> List[ModelType]:
        """
        Get records based on conditions.
        """
        stmt = self._get_base_query(include_deleted)

        # Build WHERE conditions
        where_conditions = []
        for attr, value in condition.items():
            if hasattr(self.model, attr):
                if isinstance(value, list):
                    where_conditions.append(getattr(self.model, attr).in_(value))
                else:
                    where_conditions.append(getattr(self.model, attr) == value)

        if where_conditions:
            stmt = stmt.where(and_(*where_conditions))

        if limit:
            stmt = stmt.limit(limit)

        result = await db.execute(stmt)
        return list(result.scalars().all())

    async def get_one_by_condition(
            self,
            db: AsyncSession,
            condition: Dict[str, Any],
            include_deleted: bool = False
    ) -> Optional[ModelType]:
        """
        Get single record based on conditions.
        """
        results = await self.get_by_condition(db, condition, include_deleted, limit=1)
        return results[0] if results else None

    async def update(
            self,
            db: AsyncSession,
            *,
            id: Any,
            obj_in: Dict[str, Any],
            include_deleted: bool = False
    ) -> Optional[ModelType]:
        """
        Update a record by id.
        """
        try:
            db_obj = await self.get(db, id, include_deleted)
            if not db_obj:
                return None

            # Update fields
            for key, value in obj_in.items():
                if hasattr(db_obj, key):
                    setattr(db_obj, key, value)

            # Update timestamp if exists
            if hasattr(db_obj, 'updated_at'):
                setattr(db_obj, 'updated_at', datetime.now(timezone.utc))

            db.add(db_obj)
            await db.flush()
            await db.refresh(db_obj)
            return db_obj
        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def update_many(
            self,
            db: AsyncSession,
            *,
            ids: List[Any],
            obj_in: Dict[str, Any],
            include_deleted: bool = False
    ) -> List[ModelType]:
        """
        Update multiple records by ids.
        """
        try:
            stmt = self._get_base_query(include_deleted).where(self.model.id.in_(ids))
            result = await db.execute(stmt)
            db_objs = list(result.scalars().all())

            for db_obj in db_objs:
                for key, value in obj_in.items():
                    if hasattr(db_obj, key):
                        setattr(db_obj, key, value)

                # Update timestamp if exists
                if hasattr(db_obj, 'updated_at'):
                    setattr(db_obj, 'updated_at', datetime.now(timezone.utc))

                db.add(db_obj)

            await db.flush()
            return db_objs
        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def bulk_update(
            self,
            db: AsyncSession,
            condition: Dict[str, Any],
            values: Dict[str, Any]
    ) -> int:
        """
        Bulk update records matching condition.
        """
        try:
            # Add updated_at if model has it
            if hasattr(self.model, 'updated_at'):
                values['updated_at'] = datetime.now(timezone.utc)

            stmt = update(self.model)

            # Build WHERE conditions
            where_conditions = []
            for attr, value in condition.items():
                if hasattr(self.model, attr):
                    where_conditions.append(getattr(self.model, attr) == value)

            if where_conditions:
                stmt = stmt.where(and_(*where_conditions))

            stmt = stmt.values(**values)
            result = await db.execute(stmt)
            return result.rowcount
        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def delete(self, db: AsyncSession, *, id: Any) -> bool:
        """
        Soft delete a record.
        """
        try:
            db_obj = await self.get(db, id)
            if not db_obj:
                return False

            if hasattr(db_obj, 'soft_delete'):
                db_obj.soft_delete()
                db.add(db_obj)
                await db.flush()
                return True
            return False
        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def delete_many(self, db: AsyncSession, *, ids: List[Any]) -> bool:
        """
        Soft delete multiple records.
        """
        try:
            stmt = self._get_base_query().where(self.model.id.in_(ids))
            result = await db.execute(stmt)
            db_objs = list(result.scalars().all())

            if not db_objs:
                return False

            for db_obj in db_objs:
                if hasattr(db_obj, 'soft_delete'):
                    db_obj.soft_delete()
                    db.add(db_obj)

            await db.flush()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def hard_delete(self, db: AsyncSession, *, id: Any) -> bool:
        """
        Permanently delete a record.
        """
        try:
            db_obj = await self.get(db, id, include_deleted=True)
            if not db_obj:
                return False

            await db.delete(db_obj)
            await db.flush()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def exists(self, db: AsyncSession, *, id: Any, include_deleted: bool = False) -> bool:
        """
        Check if a record exists.
        """
        result = await self.get(db, id, include_deleted)
        return result is not None

    async def count(
            self,
            db: AsyncSession,
            condition: Optional[Dict[str, Any]] = None,
            include_deleted: bool = False
    ) -> int:
        """
        Count records with optional conditions.
        """
        stmt = select(func.count(self.model.id))

        # Add soft delete filter
        if not include_deleted and hasattr(self.model, 'is_deleted'):
            stmt = stmt.where(self.model.is_deleted == False)

        # Add conditions
        if condition:
            where_conditions = []
            for attr, value in condition.items():
                if hasattr(self.model, attr):
                    where_conditions.append(getattr(self.model, attr) == value)
            if where_conditions:
                stmt = stmt.where(and_(*where_conditions))

        result = await db.execute(stmt)
        return result.scalar()

    async def restore(self, db: AsyncSession, *, id: Any) -> Optional[ModelType]:
        """
        Restore a soft-deleted record.
        """
        try:
            db_obj = await self.get(db, id, include_deleted=True)
            if db_obj and hasattr(db_obj, 'restore'):
                db_obj.restore()
                db.add(db_obj)
                await db.flush()
                await db.refresh(db_obj)
                return db_obj
            return None
        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def get_deleted(self, db: AsyncSession) -> List[ModelType]:
        """
        Get only deleted records.
        """
        if not hasattr(self.model, 'is_deleted'):
            return []

        stmt = select(self.model).where(self.model.is_deleted == True)
        result = await db.execute(stmt)
        return list(result.scalars().all())