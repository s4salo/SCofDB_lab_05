"""Rate limiting middleware implementation for LAB 05."""

from typing import Callable
import os

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.infrastructure.redis_client import get_redis
from app.infrastructure.cache_keys import payment_rate_limit_key


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Redis-based rate limiting для endpoint оплаты.

    Цель:
    - защита от DDoS/шторма запросов;
    - защита от случайных повторных кликов пользователя.
    """

    def __init__(self, app, limit_per_window: int = 5, window_seconds: int = 10):
        super().__init__(app)
        self.limit_per_window = limit_per_window
        self.window_seconds = window_seconds

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        TODO: Реализовать Redis rate limiting.

        Рекомендуемая логика:
        1) Применять только к endpoint оплаты:
           - /api/orders/{order_id}/pay
           - /api/payments/retry-demo
        2) Сформировать subject:
           - user_id (если есть), иначе client IP.
        3) Использовать Redis INCR + EXPIRE:
           - key = rate_limit:pay:{subject}
           - если counter > limit_per_window -> 429 Too Many Requests.
        4) Для прохождения запроса добавить в ответ headers:
           - X-RateLimit-Limit
           - X-RateLimit-Remaining
        """
        is_payment_endpoint = (
            request.url.path.startswith("/api/orders/") and request.url.path.endswith("/pay")
        ) or request.url.path.startswith("/api/payments/")

        if not is_payment_endpoint:
            return await call_next(request)

        subject = self._get_subject(request)

        redis_key = payment_rate_limit_key(subject)

        redis_client = get_redis()

        current_count = await redis_client.incr(redis_key)

        if current_count == 1:
            await redis_client.expire(redis_key, self.window_seconds)

        headers = {
            "X-RateLimit-Limit": str(self.limit_per_window),
            "X-RateLimit-Remaining": str(max(0, self.limit_per_window - current_count)),
            "X-RateLimit-Window": f"{self.window_seconds}s"
        }

        if current_count > self.limit_per_window:
            return Response(
                content='{"error": "Too Many Requests", "message": "Rate limit exceeded"}',
                status_code=429,
                headers=headers,
                media_type="application/json"
            )

        response = await call_next(request)

        for key, value in headers.items():
            response.headers[key] = value

        return response

    def _get_subject(self, request: Request) -> str:
        """
        Определяет уникальный идентификатор для rate limiting.
        Приоритет: user_id -> client IP.
        """
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            client_ip = forwarded.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else "unknown"

        return f"ip:{client_ip}"