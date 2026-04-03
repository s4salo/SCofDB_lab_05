"""
LAB 05: Проверка починки через событийную инвалидацию.
"""

import os
import sys
import pytest
import uuid
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text
from redis.asyncio import Redis

os.environ["DATABASE_URL"] = "postgresql+asyncpg://postgres:postgres@db:5432/marketplace"
os.environ["REDIS_URL"] = "redis://redis:6379/0"

from app.main import app


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
                    "email": f"event_test_{order_id}@example.com",
                    "name": "Event Test User"
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
async def test_order_card_is_fresh_after_event_invalidation(
    test_engine, test_order, cleanup_redis_cache
):
    """
    Тест проверяет, что событийная инвалидация работает корректно.

    Сценарий:
    1) Прогреть кэш карточки заказа
    2) Изменить заказ через mutate-with-event-invalidation
    3) Убедиться, что ключ карточки инвалидирован
    4) Повторный GET возвращает свежие данные из БД, а не stale cache
    """
    order_id = test_order
    order_id_str = str(order_id)
    original_total = 100.00
    new_total = 250.00

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as client:

        print("\n" + "="*60)
        print("ТЕСТ: Проверка событийной инвалидации кэша")
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

        redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        cache_key = f"order_card:v1:{order_id_str}"
        exists_before = await redis.exists(cache_key)
        print(f"   Ключ в Redis до изменения: {'существует' if exists_before else 'отсутствует'}")
        assert exists_before == 1

        print(f"\n2. Изменяем заказ с событийной инвалидацией")
        print(f"   Новый total_amount: {new_total}")

        response2 = await client.post(
            f"/api/cache-demo/orders/{order_id_str}/mutate-with-event-invalidation",
            json={"new_total_amount": new_total}
        )

        assert response2.status_code == 200
        mutate_result = response2.json()
        print(f"   Результат: {mutate_result['message']}")
        print(f"   Инвалидированные ключи: {mutate_result.get('cache_invalidated', [])}")

        exists_after = await redis.exists(cache_key)
        print(f"\n3. Проверка инвалидации кэша:")
        print(f"   Ключ в Redis после изменения: {'существует' if exists_after else 'удален'}")
        assert exists_after == 0, "Cache key should be invalidated"
        await redis.aclose()

        print(f"\n4. Повторный запрос карточки с use_cache=true")
        response3 = await client.get(
            f"/api/cache-demo/orders/{order_id_str}/card",
            params={"use_cache": True}
        )

        assert response3.status_code == 200
        order_data_after = response3.json()

        print(f"   Данные из кэша после изменения: total_amount = {order_data_after['total_amount']}")

        print(f"\n5. Проверка консистентности:")
        if order_data_after['total_amount'] == new_total:
            print(f"\nКЭШ КОНСИСТЕНТЕН!")
            print(f"   Кэш вернул актуальные данные ({new_total})")
            print(f"   Событийная инвалидация работает корректно")
            assert order_data_after['total_amount'] == new_total
        else:
            print(f"\nОШИБКА: Кэш вернул неактуальные данные!")
            print(f"   Ожидалось: {new_total}, Получено: {order_data_after['total_amount']}")
            assert False, "Cache returned stale data after event invalidation"

        print("\n" + "="*60)
        print("ВЫВОД: Событийная инвалидация успешно устранила проблему!")
        print("Кэш автоматически обновляется при изменении данных в БД.")
        print("="*60)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])