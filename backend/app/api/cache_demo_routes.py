"""Cache consistency demo endpoints for LAB 05."""

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.infrastructure.db import get_db
from app.application.cache_service import CacheService


router = APIRouter(prefix="/api/cache-demo", tags=["cache-demo"])


class UpdateOrderRequest(BaseModel):
    """Payload для изменения заказа в demo-сценариях."""

    new_total_amount: float


@router.get("/catalog")
async def get_catalog(
    use_cache: bool = Query(True, description="Использовать Redis кэш"),
    db: AsyncSession = Depends(get_db)
) -> Any:
    """
    TODO: Кэш каталога товаров в Redis.

    Требования:
    1) При use_cache=true читать/писать Redis.
    2) При cache miss грузить из БД и класть в кэш.
    3) Добавить TTL.

    Примечание:
    В текущей схеме можно строить \"каталог\" как агрегат по order_items.product_name.
    """
    cache_service = CacheService(db)
    catalog_data = await cache_service.get_catalog(use_cache=use_cache)
    return catalog_data


@router.get("/orders/{order_id}/card")
async def get_order_card(
    order_id: uuid.UUID,
    use_cache: bool = Query(True, description="Использовать Redis кэш"),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    TODO: Кэш карточки заказа в Redis.

    Требования:
    1) Ключ вида order_card:v1:{order_id}.
    2) При use_cache=true возвращать данные из кэша.
    3) При miss грузить из БД и сохранять в кэш.
    """
    cache_service = CacheService(db)
    order_data = await cache_service.get_order_card(str(order_id), use_cache=use_cache)

    if order_data is None:
        raise HTTPException(status_code=404, detail="Order not found")

    return order_data

@router.post("/orders/{order_id}/mutate-without-invalidation")
async def mutate_without_invalidation(
    order_id: uuid.UUID,
    payload: UpdateOrderRequest,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    TODO: Намеренно сломанный сценарий консистентности.

    Нужно:
    1) Изменить заказ в БД.
    2) НЕ инвалидировать кэш.
    3) Показать, что последующий GET /orders/{id}/card может вернуть stale data.
    """
    check_query = text("SELECT id FROM orders WHERE id = :order_id")
    result = await db.execute(check_query, {"order_id": order_id})
    if not result.fetchone():
        raise HTTPException(status_code=404, detail="Order not found")

    update_query = text("""
        UPDATE orders
        SET total_amount = :new_total_amount
        WHERE id = :order_id
    """)
    await db.execute(update_query, {
        "order_id": order_id,
        "new_total_amount": payload.new_total_amount
    })

    await db.commit()

    return {
        "message": "Order updated in DB, but cache was NOT invalidated",
        "order_id": str(order_id),
        "new_total_amount": payload.new_total_amount,
        "warning": "Subsequent GET /orders/{id}/card?use_cache=true will return stale data"
    }

@router.post("/orders/{order_id}/mutate-with-event-invalidation")
async def mutate_with_event_invalidation(
    order_id: uuid.UUID,
    payload: UpdateOrderRequest,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    TODO: Починка через событийную инвалидацию.

    Нужно:
    1) Изменить заказ в БД.
    2) Сгенерировать событие OrderUpdated.
    3) Обработчик события должен инвалидировать связанные cache keys:
       - order_card:v1:{order_id}
       - catalog:v1 (если изменение влияет на каталог/агрегаты)
    """
    check_query = text("SELECT id FROM orders WHERE id = :order_id")
    result = await db.execute(check_query, {"order_id": order_id})
    if not result.fetchone():
        raise HTTPException(status_code=404, detail="Order not found")

    update_query = text("""
        UPDATE orders
        SET total_amount = :new_total_amount
        WHERE id = :order_id
    """)
    await db.execute(update_query, {
        "order_id": order_id,
        "new_total_amount": payload.new_total_amount
    })

    await db.commit()

    cache_service = CacheService(db)
    await cache_service.invalidate_order_card(str(order_id))
    await cache_service.invalidate_catalog()

    return {
        "message": "Order updated in DB and cache was invalidated",
        "order_id": str(order_id),
        "new_total_amount": payload.new_total_amount,
        "cache_invalidated": ["order_card", "catalog"]
    }