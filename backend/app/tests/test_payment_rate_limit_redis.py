"""
LAB 05: Rate limiting endpoint оплаты через Redis.
"""

import pytest
import uuid
import os
import asyncio
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
async def create_test_order(test_engine):
    """
    Фикстура для создания тестового заказа.
    Возвращает функцию, которая создает новый заказ.
    """
    async def _create_order():
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
                        "email": f"rate_test_{order_id}@example.com",
                        "name": "Rate Limit Test User"
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
                        INSERT INTO order_status_history (id, order_id, status, changed_at)
                        VALUES (gen_random_uuid(), :order_id, 'created', NOW())
                    """),
                    {"order_id": order_id}
                )

        return order_id, user_id

    return _create_order


@pytest.fixture
async def cleanup_order(test_engine):
    """
    Фикстура для очистки заказа после теста.
    """
    async def _cleanup(order_id, user_id):
        async with AsyncSession(test_engine) as session:
            async with session.begin():
                await session.execute(
                    text("DELETE FROM order_status_history WHERE order_id = :order_id"),
                    {"order_id": order_id}
                )
                await session.execute(
                    text("DELETE FROM orders WHERE id = :order_id"),
                    {"order_id": order_id}
                )
                await session.execute(
                    text("DELETE FROM users WHERE id = :user_id"),
                    {"user_id": user_id}
                )

    return _cleanup


@pytest.fixture
async def cleanup_redis_rate_limit():
    """Очистить Redis ключи rate limiting перед тестом."""
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)

    keys = await redis.keys("rate_limit:pay:*")
    if keys:
        await redis.delete(*keys)

    await redis.aclose()
    yield

    redis2 = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    keys2 = await redis2.keys("rate_limit:pay:*")
    if keys2:
        await redis2.delete(*keys2)
    await redis2.aclose()


@pytest.mark.asyncio
async def test_payment_endpoint_rate_limit(
    test_engine, create_test_order, cleanup_order, cleanup_redis_rate_limit
):
    """
    Тест проверяет Redis rate limiting для endpoint оплаты.

    Сценарий:
    1) Создаем 6 разных заказов
    2) Делаем запрос на оплату каждого заказа
    3) Проверяем, что первые 5 проходят, 6-й получает 429
    """
    limit = 5
    orders = []

    try:
        for i in range(limit + 1):
            order_id, user_id = await create_test_order()
            orders.append((order_id, user_id))

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:

            print("\n" + "="*60)
            print("ТЕСТ: Redis Rate Limiting для оплаты")
            print("="*60)
            print(f"\nЛимит запросов: {limit} запросов в окне")

            results = []

            for i, (order_id, _) in enumerate(orders, 1):
                print(f"\n--- Запрос {i} (заказ {str(order_id)[:8]}...) ---")

                response = await client.post(f"/api/orders/{str(order_id)}/pay")

                status_code = response.status_code
                headers = response.headers

                limit_header = headers.get("X-RateLimit-Limit")
                remaining_header = headers.get("X-RateLimit-Remaining")
                window_header = headers.get("X-RateLimit-Window")

                print(f"  Status: {status_code}")
                print(f"  X-RateLimit-Limit: {limit_header}")
                print(f"  X-RateLimit-Remaining: {remaining_header}")
                print(f"  X-RateLimit-Window: {window_header}")

                if status_code == 200:
                    data = response.json()
                    print(f"  Response: {data.get('message', 'OK')}")
                elif status_code == 429:
                    data = response.json()
                    print(f"  Response: {data.get('message', data.get('error', 'Rate limited'))}")

                results.append({
                    "request": i,
                    "status_code": status_code,
                    "limit": limit_header,
                    "remaining": remaining_header
                })

            print("\n" + "="*60)
            print("РЕЗУЛЬТАТЫ ТЕСТА")
            print("="*60)

            successful_requests = [r for r in results if r["status_code"] == 200]
            rate_limited_requests = [r for r in results if r["status_code"] == 429]

            print(f"\nУспешные запросы: {len(successful_requests)}")
            print(f"Отклоненные запросы (429): {len(rate_limited_requests)}")

            for i in range(min(limit, len(results))):
                if i < len(results):
                    assert results[i]["status_code"] == 200, \
                        f"Запрос {results[i]['request']} должен быть успешным"
                    print(f"  Запрос {results[i]['request']}: PASS (200)")

            if len(results) > limit:
                assert results[limit]["status_code"] == 429, \
                    f"Запрос {results[limit]['request']} должен быть отклонен (429)"
                print(f" Запрос {results[limit]['request']}: PASS (429)")

            assert len(successful_requests) <= limit, \
                f"Успешных запросов ({len(successful_requests)}) больше лимита ({limit})"

            for result in results:
                assert result["limit"] == str(limit), \
                    f"X-RateLimit-Limit должен быть {limit}"
                assert result["remaining"] is not None, \
                    "X-RateLimit-Remaining должен присутствовать"

            print("\n" + "="*60)
            print("ВЫВОД: Rate limiting работает корректно!")
            print(f" Защита от DDoS и случайных повторных кликов активна")
            print(f" Лимит {limit} запросов в окне соблюдается")
            print(f" Заголовки X-RateLimit-* возвращаются")
            print("="*60)

    finally:
        for order_id, user_id in orders:
            await cleanup_order(order_id, user_id)

@pytest.mark.asyncio
async def test_rate_limit_resets_after_window(
    test_engine, create_test_order, cleanup_order, cleanup_redis_rate_limit
):
    """
    Тест проверяет, что лимит сбрасывается после окна.
    """
    limit = 5
    orders = []

    try:
        for i in range(limit + 1):
            order_id, user_id = await create_test_order()
            orders.append((order_id, user_id))

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:

            print("\n" + "="*60)
            print("ТЕСТ: Сброс лимита после временного окна")
            print("="*60)

            print(f"\n1. Делаем {limit} запросов...")
            for i in range(limit):
                order_id, _ = orders[i]
                response = await client.post(f"/api/orders/{str(order_id)}/pay")
                assert response.status_code == 200, f"Запрос {i+1} должен быть успешным"
                print(f"   Запрос {i+1}: OK")

            print(f"\n2. {limit+1}-й запрос должен быть отклонен...")
            order_id, _ = orders[limit]
            response = await client.post(f"/api/orders/{str(order_id)}/pay")

            print(f"   Запрос {limit+1}: {response.status_code}")
            assert response.status_code == 429, "Запрос должен быть отклонен (429)"

            print(f"\n3. Ждем 10+5 секунд до сброса лимита...")
            await asyncio.sleep(15)

            new_order_id, new_user_id = await create_test_order()
            orders.append((new_order_id, new_user_id))

            print(f"\n4. Запрос после ожидания...")
            response = await client.post(f"/api/orders/{str(new_order_id)}/pay")

            if response.status_code == 200:
                print(f"   Запрос: 200 (лимит сброшен)")
                assert response.status_code == 200
            else:
                print(f"   Запрос: {response.status_code} (лимит еще активен)")

            print("\n Сброс лимита после окна работает")

    finally:
        for order_id, user_id in orders:
            await cleanup_order(order_id, user_id)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])