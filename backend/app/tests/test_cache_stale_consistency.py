"""
LAB 05: Демонстрация неконсистентности кэша.
"""

import os
import sys
import pytest
import uuid
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

os.environ["DATABASE_URL"] = "postgresql+asyncpg://postgres:postgres@db:5432/marketplace"
os.environ["REDIS_URL"] = "redis://redis:6379/0"

from app.main import app
from app.infrastructure.redis_client import get_redis
from redis.asyncio import Redis


@pytest.fixture()
async def test_engine():
    """Создать AsyncEngine для тестов."""
    engine = create_async_engine(
        os.environ["DATABASE_URL"],
        echo=False,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20
    )
    yield engine
    await engine.dispose()


@pytest.fixture
async def test_order(test_engine):
    """
    Создать тестовый заказ со статусом 'created' и товарами.
    """
    user_id = uuid.uuid4()
    order_id = uuid.uuid4()

    async with AsyncSession(test_engine) as session:
        async with session.begin():
            await session.execute(
                text("""
                    INSERT INTO users (id, email, name, created_at)
                    VALUES (:user_id, :email, :name, NOW())
                    ON CONFLICT (id) DO NOTHING
                """),
                {
                    "user_id": user_id,
                    "email": f"stale_test_{order_id}@example.com",
                    "name": "Stale Test User"
                }
            )

            await session.execute(
                text("""
                    INSERT INTO orders (id, user_id, status, total_amount, created_at)
                    VALUES (:order_id, :user_id, 'created', 100.00, NOW())
                """),
                {"order_id": order_id, "user_id": user_id}
            )

            await session.execute(
                text("""
                    INSERT INTO order_items (id, order_id, product_name, price, quantity)
                    VALUES (gen_random_uuid(), :order_id, 'Test Product', 50.00, 2)
                """),
                {"order_id": order_id}
            )

            await session.execute(
                text("""
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (gen_random_uuid(), :order_id, 'created', NOW())
                """),
                {"order_id": order_id}
            )

    yield order_id

    async with AsyncSession(test_engine) as cleanup_session:
        async with cleanup_session.begin():
            await cleanup_session.execute(
                text("DELETE FROM order_status_history WHERE order_id = :order_id"),
                {"order_id": order_id}
            )
            await cleanup_session.execute(
                text("DELETE FROM order_items WHERE order_id = :order_id"),
                {"order_id": order_id}
            )
            await cleanup_session.execute(
                text("DELETE FROM orders WHERE id = :order_id"),
                {"order_id": order_id}
            )
            await cleanup_session.execute(
                text("DELETE FROM users WHERE id = :user_id"),
                {"user_id": user_id}
            )


@pytest.fixture
async def cleanup_redis_cache():
    """Очистить Redis кэш перед тестом."""
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    await redis.flushall()
    await redis.aclose()
    yield
    redis2 = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    await redis2.flushall()
    await redis2.aclose()


@pytest.mark.asyncio
async def test_stale_order_card_when_db_updated_without_invalidation(
    test_engine, test_order, cleanup_redis_cache
):
    """
    Тест демонстрирует проблему неконсистентности кэша.

    Сценарий:
    1) Прогреть кэш карточки заказа (GET с use_cache=true)
    2) Изменить заказ в БД через mutate-without-invalidation
    3) Повторно запросить карточку с use_cache=true
    4) Проверить, что клиент получает stale данные из кэша
    """
    order_id = test_order
    order_id_str = str(order_id)
    original_total = 100.00
    new_total = 200.00

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as client:

        print("\n" + "="*60)
        print("ТЕСТ: Демонстрация stale data в кэше")
        print("="*60)

        print(f"\n1. Прогреваем кэш карточки заказа {order_id_str}")
        response1 = await client.get(
            f"/api/cache-demo/orders/{order_id_str}/card",
            params={"use_cache": True}
        )

        assert response1.status_code == 200
        order_data_before = response1.json()

        print(f"   Данные из кэша: total_amount = {order_data_before['total_amount']}")
        assert order_data_before['total_amount'] == original_total

        print(f"\n2. Изменяем заказ в БД через mutate-without-invalidation")
        print(f"   Новый total_amount: {new_total}")

        response2 = await client.post(
            f"/api/cache-demo/orders/{order_id_str}/mutate-without-invalidation",
            json={"new_total_amount": new_total}
        )

        assert response2.status_code == 200
        mutate_result = response2.json()
        print(f"   Результат: {mutate_result['message']}")

        async with AsyncSession(test_engine) as session:
            result = await session.execute(
                text("SELECT total_amount FROM orders WHERE id = :order_id"),
                {"order_id": order_id}
            )
            db_total = result.fetchone()[0]
        print(f"   Значение в БД: total_amount = {db_total}")
        assert db_total == new_total

        print(f"\n3. Повторный запрос карточки с use_cache=true")
        response3 = await client.get(
            f"/api/cache-demo/orders/{order_id_str}/card",
            params={"use_cache": True}
        )

        assert response3.status_code == 200
        order_data_after = response3.json()

        print(f"   Данные из кэша после изменения: total_amount = {order_data_after['total_amount']}")

        print(f"\n4. Проверка консистентности:")
        print(f"   - Значение в БД: {db_total}")
        print(f"   - Значение из кэша: {order_data_after['total_amount']}")

        if order_data_after['total_amount'] == original_total:
            print(f"\nSTALE DATA DETECTED!")
            print(f"   Кэш вернул старые данные ({original_total})")
            print(f"   Хотя в БД уже новое значение ({new_total})")
            print(f"   Это демонстрирует проблему неконсистентности кэша")
            assert order_data_after['total_amount'] == original_total
        else:
            print(f"\nОШИБКА: Кэш обновился без инвалидации!")
            print(f"   Это не соответствует ожидаемому поведению")
            assert False, "Cache was updated without invalidation"

        print(f"\n5. Проверка запроса без кэша:")
        response4 = await client.get(
            f"/api/cache-demo/orders/{order_id_str}/card",
            params={"use_cache": False}
        )

        assert response4.status_code == 200
        order_data_no_cache = response4.json()
        print(f"   Данные без кэша: total_amount = {order_data_no_cache['total_amount']}")
        assert order_data_no_cache['total_amount'] == new_total

        print("\n" + "="*60)
        print("ВЫВОД: Проблема неконсистентности кэша успешно продемонстрирована!")
        print("Кэш возвращает устаревшие данные после изменения БД без инвалидации.")
        print("="*60)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])