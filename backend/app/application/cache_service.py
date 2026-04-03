"""Cache service implementation for LAB 05."""

import json
from typing import Any, Optional
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.redis_client import get_redis
from app.infrastructure.cache_keys import catalog_key, order_card_key


class CacheService:
    """
    Сервис кэширования каталога и карточки заказа.

    TODO:
    - реализовать методы через Redis client + БД;
    - добавить TTL и версионирование ключей.
    """

    def __init__(self, db_session: AsyncSession):
        self.db = db_session
        self.redis = get_redis()
        self.ttl_seconds = 300

    async def get_catalog(self, *, use_cache: bool = True) -> list[dict[str, Any]]:
        """
        TODO:
        1) Попытаться вернуть catalog из Redis.
        2) При miss загрузить из БД.
        3) Положить в Redis с TTL.
        """
        cache_key = catalog_key()

        if use_cache:
            cached_data = await self.redis.get(cache_key)
            if cached_data is not None:
                return json.loads(cached_data)

        query = text("""
            SELECT
                oi.product_name,
                count(*) AS order_lines,
                sum(oi.quantity) AS sold_qty,
                round(avg(oi.price)::numeric, 2) AS avg_price
            FROM order_items oi
            GROUP BY oi.product_name
            ORDER BY sold_qty DESC
            LIMIT 100
        """)

        result = await self.db.execute(query)
        rows = result.fetchall()

        catalog_data = []
        for row in rows:
            catalog_data.append({
                "product_name": row[0],
                "order_lines": row[1],
                "sold_qty": row[2],
                "avg_price": float(row[3]) if row[3] is not None else None
            })

        if use_cache:
            await self.redis.setex(
                cache_key,
                self.ttl_seconds,
                json.dumps(catalog_data, default=str)
            )

        return catalog_data

    async def get_order_card(self, order_id: str, *, use_cache: bool = True) -> Optional[dict[str, Any]]:
        """
        TODO:
        1) Попытаться вернуть карточку заказа из Redis.
        2) При miss загрузить из БД.
        3) Положить в Redis с TTL.
        """
        cache_key = order_card_key(order_id)

        if use_cache:
            cached_data = await self.redis.get(cache_key)
            if cached_data is not None:
                return json.loads(cached_data)

        order_query = text("""
            SELECT o.id, o.user_id, o.status, o.total_amount, o.created_at,
                   u.email, u.name
            FROM orders o
            JOIN users u ON o.user_id = u.id
            WHERE o.id = :order_id
        """)

        order_result = await self.db.execute(order_query, {"order_id": order_id})
        order_row = order_result.fetchone()

        if not order_row:
            return None

        items_query = text("""
            SELECT id, product_name, price, quantity
            FROM order_items
            WHERE order_id = :order_id
        """)

        items_result = await self.db.execute(items_query, {"order_id": order_id})
        items = []
        for item_row in items_result:
            items.append({
                "id": str(item_row[0]),
                "product_name": item_row[1],
                "price": float(item_row[2]),
                "quantity": item_row[3],
                "subtotal": float(item_row[2] * item_row[3])
            })

        order_data = {
            "id": str(order_row[0]),
            "user_id": str(order_row[1]),
            "status": order_row[2],
            "total_amount": float(order_row[3]),
            "created_at": order_row[4].isoformat() if order_row[4] else None,
            "user": {
                "email": order_row[5],
                "name": order_row[6]
            },
            "items": items
        }

        if use_cache:
            await self.redis.setex(
                cache_key,
                self.ttl_seconds,
                json.dumps(order_data, default=str)
            )

        return order_data

    async def invalidate_order_card(self, order_id: str) -> None:
        """Удалить ключ карточки заказа из Redis."""
        cache_key = order_card_key(order_id)
        await self.redis.delete(cache_key)

    async def invalidate_catalog(self) -> None:
        """Удалить ключ каталога из Redis."""
        cache_key = catalog_key()
        await self.redis.delete(cache_key)