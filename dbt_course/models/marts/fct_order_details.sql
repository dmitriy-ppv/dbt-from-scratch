WITH order_details AS (SELECT product_id, order_id, total_price
                       FROM {{ref('stg_dunder_mifflin__order_details')}}),

     orders AS (SELECT order_id, employee_id
                       FROM {{ref('stg_dunder_mifflin__orders')}}),

     products AS (SELECT product_name, product_id
                       FROM {{ref('stg_dunder_mifflin__products')}}),

     employees AS (SELECT employee_id, first_name, last_name
                       FROM {{ref('stg_dunder_mifflin__employees')}})

SELECT order_details.product_id,
       products.product_name,
       orders.employee_id,
       employees.first_name,
       employees.last_name,
       order_details.total_price as total_orders
FROM order_details
    LEFT JOIN products
        ON order_details.product_id = products.product_id
    LEFT JOIN orders
        ON orders.order_id = order_details.order_id
    LEFT JOIN employees
        ON orders.employee_id = employees.employee_id
