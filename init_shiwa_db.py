#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中益石蛙基地 - 数据库一键初始化
用法：
  1. export DATABASE_SHIWA_URL="postgresql://user:pwd@host:5432/shiwa"
  2. python init_shiwa_db.py
运行多次无害，全部 IF NOT EXISTS。
"""
import os
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_SHIWA_URL")
if not DB_URL:
    raise RuntimeError("请先 export DATABASE_SHIWA_URL=postgresql://...")

url = urlparse(DB_URL)
conn_params = {
    "host": url.hostname,
    "port": url.port or 5432,
    "database": url.path[1:],
    "user": url.username,
    "password": url.password,
}

def get_conn():
    return psycopg2.connect(**conn_params)

SQLS = [
    # 1. 用户表
    """
    CREATE TABLE IF NOT EXISTS user_shiwa (
        id          SERIAL PRIMARY KEY,
        username    VARCHAR(50) UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        department  VARCHAR(20) CHECK (department IN ('管理部','现场部')),
        role        VARCHAR(20) DEFAULT '员工',
        created_at  TIMESTAMP DEFAULT NOW()
    );
    """,

    # 2. 蛙种种类
    """
    CREATE TABLE IF NOT EXISTS frog_type_shiwa (
        id   SERIAL PRIMARY KEY,
        name VARCHAR(50) UNIQUE NOT NULL
    );
    INSERT INTO frog_type_shiwa (name) VALUES
    ('细皮蛙'),('粗皮蛙')
    ON CONFLICT DO NOTHING;
    """,

    # 3. 池塘类型
    """
    CREATE TABLE IF NOT EXISTS pond_type_shiwa (
        id   SERIAL PRIMARY KEY,
        name VARCHAR(50) UNIQUE NOT NULL
    );
    INSERT INTO pond_type_shiwa (name) VALUES
    ('种蛙池'),('孵化池'),('养殖池'),('商品蛙池'),('试验池'),('三年蛙池'),('四年蛙池'),('五年蛙池'),('六年蛙池')
    ON CONFLICT DO NOTHING;
    """,

    # 4. 饲料类型（库存状态）
    """
    CREATE TABLE IF NOT EXISTS feed_type_shiwa (
        id              SERIAL PRIMARY KEY,
        name            VARCHAR(100) UNIQUE NOT NULL,
        unit_price      NUMERIC(10,2) NOT NULL DEFAULT 0,
        stock_kg        NUMERIC(12,2) DEFAULT 0,
        supplier        VARCHAR(100),
        supplier_phone  VARCHAR(50),
        purchased_by    VARCHAR(50),
        created_at      TIMESTAMP DEFAULT NOW()
    );
    """,

    # 5. 蛙苗类型（库存状态）
    """
    CREATE TABLE IF NOT EXISTS frog_purchase_type_shiwa (
        id              SERIAL PRIMARY KEY,
        name            VARCHAR(100) UNIQUE NOT NULL,
        unit_price      NUMERIC(10,2) NOT NULL DEFAULT 0,
        quantity        INTEGER DEFAULT 0,
        supplier        VARCHAR(100),
        supplier_phone  VARCHAR(50),
        purchased_by    VARCHAR(50),
        created_at      TIMESTAMP DEFAULT NOW()
    );
    """,

    # 6. 饲料采购流水记录（每次采购独立一行）
    """
    CREATE TABLE IF NOT EXISTS feed_purchase_record_shiwa (
        id                SERIAL PRIMARY KEY,
        feed_type_name    VARCHAR(100) NOT NULL,
        quantity_kg       NUMERIC(12,2) NOT NULL,
        unit_price        NUMERIC(10,2) NOT NULL,
        total_amount      NUMERIC(14,2) GENERATED ALWAYS AS (quantity_kg * unit_price) STORED,
        supplier          VARCHAR(100),
        supplier_phone    VARCHAR(50),
        purchased_by      VARCHAR(50),
        purchased_at      TIMESTAMP DEFAULT NOW()
    );
    """,

    # 7. 蛙苗采购流水记录（每次采购独立一行）
    """
    CREATE TABLE IF NOT EXISTS frog_purchase_record_shiwa (
        id                SERIAL PRIMARY KEY,
        frog_type_name    VARCHAR(100) NOT NULL,
        quantity          INTEGER NOT NULL,
        unit_price        NUMERIC(10,2) NOT NULL,
        total_amount      NUMERIC(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
        supplier          VARCHAR(100),
        supplier_phone    VARCHAR(50),
        purchased_by      VARCHAR(50),
        purchased_at      TIMESTAMP DEFAULT NOW()
    );
    """,

    # 8. 客户
    """
    CREATE TABLE IF NOT EXISTS customer_shiwa (
        id     SERIAL PRIMARY KEY,
        name   VARCHAR(100) NOT NULL,
        phone  VARCHAR(50),
        type   VARCHAR(20) CHECK (type IN ('零售','批发')),
        created_at TIMESTAMP DEFAULT NOW()
    );
    """,

    # 9. 池塘主表
    """
    CREATE TABLE IF NOT EXISTS pond_shiwa (
        id              SERIAL PRIMARY KEY,
        name            VARCHAR(100) UNIQUE NOT NULL,
        pond_type_id    INTEGER REFERENCES pond_type_shiwa(id),
        frog_type_id    INTEGER REFERENCES frog_type_shiwa(id),
        max_capacity    INTEGER NOT NULL,
        current_count   INTEGER DEFAULT 0,
        created_at      TIMESTAMP DEFAULT NOW(),
        updated_at      TIMESTAMP DEFAULT NOW()
    );
    """,

    # 10. 库存变动（转池/外购/孵化/死亡/销售）
    """
    CREATE TABLE IF NOT EXISTS stock_movement_shiwa (
        id            SERIAL PRIMARY KEY,
        movement_type VARCHAR(20) CHECK (movement_type IN ('transfer','purchase','hatch','sale','death')),
        from_pond_id  INTEGER REFERENCES pond_shiwa(id),
        to_pond_id    INTEGER REFERENCES pond_shiwa(id),
        quantity      INTEGER NOT NULL,
        description   TEXT,
        unit_price    NUMERIC(10,2),
        created_by    VARCHAR(50),
        moved_at      TIMESTAMP DEFAULT NOW()
    );
    """,

    # 11. 喂养记录
    """
    CREATE TABLE IF NOT EXISTS feeding_record_shiwa (
        id                 SERIAL PRIMARY KEY,
        pond_id            INTEGER REFERENCES pond_shiwa(id),
        feed_type_id       INTEGER REFERENCES feed_type_shiwa(id),
        feed_weight_kg     NUMERIC(10,2) NOT NULL,
        unit_price_at_time NUMERIC(10,2) NOT NULL,
        total_cost         NUMERIC(12,2) GENERATED ALWAYS AS (feed_weight_kg * unit_price_at_time) STORED,
        notes              TEXT,
        fed_at             TIMESTAMP DEFAULT NOW(),
        fed_by             VARCHAR(50)
    );
    """,

    # 12. 每日日志
    """
    CREATE TABLE IF NOT EXISTS daily_log_shiwa (
        id            SERIAL PRIMARY KEY,
        pond_id       INTEGER REFERENCES pond_shiwa(id),
        log_date      DATE NOT NULL,
        water_temp    NUMERIC(5,2),
        ph_value      NUMERIC(5,2),
        do_value      NUMERIC(5,2),
        humidity      NUMERIC(5,2),
        weather       VARCHAR(20),
        water_source  VARCHAR(20),
        observation   TEXT,
        recorded_by   VARCHAR(50),
        created_at    TIMESTAMP DEFAULT NOW(),
        updated_at    TIMESTAMP DEFAULT NOW(),
        UNIQUE (pond_id, log_date)
    );
    """,

    # 13. 销售记录
    """
    CREATE TABLE IF NOT EXISTS sale_record_shiwa (
        id                SERIAL PRIMARY KEY,
        pond_id           INTEGER REFERENCES pond_shiwa(id),
        customer_id       INTEGER REFERENCES customer_shiwa(id),
        sale_type         VARCHAR(20) CHECK (sale_type IN ('零售','批发')),
        quantity          INTEGER NOT NULL,
        unit_price        NUMERIC(10,2) NOT NULL,
        total_amount      NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
        weight_jin        NUMERIC(10,2),
        note              TEXT,
        sold_at           TIMESTAMP DEFAULT NOW(),
        sold_by           VARCHAR(50)
    );
    """,

    # 14. 死亡图片
    """
    CREATE TABLE IF NOT EXISTS death_image_shiwa (
        id                  SERIAL PRIMARY KEY,
        death_movement_id   INTEGER REFERENCES stock_movement_shiwa(id) ON DELETE CASCADE,
        image_path          TEXT NOT NULL
    );
    """,

    # 15. 生命周期（可选）
    """
    CREATE TABLE IF NOT EXISTS pond_life_cycle_shiwa (
        id           SERIAL PRIMARY KEY,
        movement_id  INTEGER REFERENCES stock_movement_shiwa(id),
        pond_id      INTEGER REFERENCES pond_shiwa(id),
        frog_type_id INTEGER REFERENCES frog_type_shiwa(id),
        quantity     INTEGER NOT NULL,
        start_at     DATE DEFAULT CURRENT_DATE,
        stage        VARCHAR(20)
    );
    """,

    # 16. 常用索引
    """
    CREATE INDEX IF NOT EXISTS idx_pond_type ON pond_shiwa(pond_type_id);
    CREATE INDEX IF NOT EXISTS idx_pond_frog ON pond_shiwa(frog_type_id);
    CREATE INDEX IF NOT EXISTS idx_movement_from ON stock_movement_shiwa(from_pond_id);
    CREATE INDEX IF NOT EXISTS idx_movement_to   ON stock_movement_shiwa(to_pond_id);
    CREATE INDEX IF NOT EXISTS idx_feed_pond     ON feeding_record_shiwa(pond_id);
    CREATE INDEX IF NOT EXISTS idx_sale_pond     ON sale_record_shiwa(pond_id);
    CREATE INDEX IF NOT EXISTS idx_daily_pond    ON daily_log_shiwa(pond_id);
    CREATE INDEX IF NOT EXISTS idx_feed_purchase_time ON feed_purchase_record_shiwa(purchased_at);
    CREATE INDEX IF NOT EXISTS idx_frog_purchase_time ON frog_purchase_record_shiwa(purchased_at);
    """,

    # 17. 提醒视图
    """
    CREATE OR REPLACE VIEW pond_reminder_v AS
    SELECT
        p.id           AS pond_id,
        p.name         AS pond_name,
        ft.name        AS frog_type,
        p.current_count AS quantity,
        DATE_TRUNC('day', sm.moved_at) AS start_date,
        EXTRACT(DAY FROM (CURRENT_DATE - DATE_TRUNC('day', sm.moved_at)))::int AS days_elapsed,
        GREATEST(0, 90 - EXTRACT(DAY FROM (CURRENT_DATE - DATE_TRUNC('day', sm.moved_at)))::int) AS days_left,
        CASE
            WHEN EXTRACT(DAY FROM (CURRENT_DATE - DATE_TRUNC('day', sm.moved_at)))::int < 30 THEN '幼蛙'
            WHEN EXTRACT(DAY FROM (CURRENT_DATE - DATE_TRUNC('day', sm.moved_at)))::int < 60 THEN '青年蛙'
            ELSE '成蛙'
        END AS next_stage
    FROM pond_shiwa p
    JOIN frog_type_shiwa ft ON p.frog_type_id = ft.id
    JOIN stock_movement_shiwa sm
          ON sm.to_pond_id = p.id
         AND sm.movement_type IN ('purchase','hatch')
    WHERE p.current_count > 0;
    """
]

def main():
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in SQLS:
                cur.execute(sql)
        conn.commit()
    print("✅ 中益石蛙基地数据库已全部就绪！")
    print("📌 请确保 .env 里 DATABASE_SHIWA_URL 指向刚才初始化的库，然后启动 Streamlit。")

if __name__ == "__main__":
    main()