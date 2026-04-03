\timing on

-- ============================================
-- LAB 05: Проверка "истины" в БД для карточки заказа
-- ============================================
--
-- TODO:
-- Замените {{order_id}} на UUID заказа, который тестируете.

SELECT
    o.id,
    o.user_id,
    o.status,
    o.total_amount,
    o.created_at
FROM orders o
WHERE o.id = '{{dff9899c-6cdf-405d-9ccf-ded626c1bc5b}}'::uuid;

SELECT
    oi.order_id,
    oi.product_name,
    oi.price,
    oi.quantity
FROM order_items oi
WHERE oi.order_id = '{{dff9899c-6cdf-405d-9ccf-ded626c1bc5b}}'::uuid
ORDER BY oi.product_name;
