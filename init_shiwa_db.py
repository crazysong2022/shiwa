#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中益石蛙基地 - 智能数据库初始化（自动修复/升级）
- 全新环境：创建完整表
- 已有环境：自动检测缺失字段并 ALTER TABLE 补全
- 多次运行安全无害
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

def column_exists(cur, table_name, column_name):
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s;
    """, (table_name, column_name))
    return cur.fetchone() is not None

def index_exists(cur, index_name):
    cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = %s;", (index_name,))
    return cur.fetchone() is not None

def main():
    with get_conn() as conn:
        with conn.cursor() as cur:

            # ========== 1. 创建基础表（IF NOT EXISTS）==========
            tables_sql = [
                # user_shiwa
                """
                CREATE TABLE IF NOT EXISTS user_shiwa (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    department VARCHAR(20) CHECK (department IN ('管理部','现场部')),
                    role VARCHAR(20) DEFAULT '员工',
                    created_at TIMESTAMP DEFAULT NOW()
                );
                """,
                # frog_type_shiwa
                """
                CREATE TABLE IF NOT EXISTS frog_type_shiwa (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) UNIQUE NOT NULL
                );
                INSERT INTO frog_type_shiwa (name) VALUES ('细皮蛙'),('粗皮蛙') ON CONFLICT DO NOTHING;
                """,
                # pond_type_shiwa
                """
                CREATE TABLE IF NOT EXISTS pond_type_shiwa (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) UNIQUE NOT NULL
                );
                INSERT INTO pond_type_shiwa (name) VALUES
                ('种蛙池'),('孵化池'),('养殖池'),('商品蛙池'),('试验池'),
                ('三年蛙池'),('四年蛙池'),('五年蛙池'),('六年蛙池')
                ON CONFLICT DO NOTHING;
                """,
                # feed_type_shiwa
                """
                CREATE TABLE IF NOT EXISTS feed_type_shiwa (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) UNIQUE NOT NULL,
                    unit_price NUMERIC(10,2) NOT NULL DEFAULT 0,
                    stock_kg NUMERIC(12,2) DEFAULT 0,
                    supplier VARCHAR(100),
                    supplier_phone VARCHAR(50),
                    purchased_by VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW()
                );
                """,
                # frog_purchase_type_shiwa
                """
                CREATE TABLE IF NOT EXISTS frog_purchase_type_shiwa (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) UNIQUE NOT NULL,
                    unit_price NUMERIC(10,2) NOT NULL DEFAULT 0,
                    quantity INTEGER DEFAULT 0,
                    supplier VARCHAR(100),
                    supplier_phone VARCHAR(50),
                    purchased_by VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW()
                );
                """,
                # customer_shiwa
                """
                CREATE TABLE IF NOT EXISTS customer_shiwa (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    phone VARCHAR(50),
                    type VARCHAR(20) CHECK (type IN ('零售','批发')),
                    created_at TIMESTAMP DEFAULT NOW()
                );
                """,
                # pond_shiwa
                """
                CREATE TABLE IF NOT EXISTS pond_shiwa (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) UNIQUE NOT NULL,
                    pond_type_id INTEGER REFERENCES pond_type_shiwa(id),
                    frog_type_id INTEGER REFERENCES frog_type_shiwa(id),
                    max_capacity INTEGER NOT NULL,
                    current_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                """,
                # feeding_record_shiwa
                """
                CREATE TABLE IF NOT EXISTS feeding_record_shiwa (
                    id SERIAL PRIMARY KEY,
                    pond_id INTEGER REFERENCES pond_shiwa(id),
                    feed_type_id INTEGER REFERENCES feed_type_shiwa(id),
                    feed_weight_kg NUMERIC(10,2) NOT NULL,
                    unit_price_at_time NUMERIC(10,2) NOT NULL,
                    total_cost NUMERIC(12,2) GENERATED ALWAYS AS (feed_weight_kg * unit_price_at_time) STORED,
                    notes TEXT,
                    fed_at TIMESTAMP DEFAULT NOW(),
                    fed_by VARCHAR(50)
                );
                """,
                # daily_log_shiwa
                """
                CREATE TABLE IF NOT EXISTS daily_log_shiwa (
                    id SERIAL PRIMARY KEY,
                    pond_id INTEGER REFERENCES pond_shiwa(id),
                    log_date DATE NOT NULL,
                    water_temp NUMERIC(5,2),
                    ph_value NUMERIC(5,2),
                    do_value NUMERIC(5,2),
                    humidity NUMERIC(5,2),
                    weather VARCHAR(20),
                    water_source VARCHAR(20),
                    observation TEXT,
                    recorded_by VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE (pond_id, log_date)
                );
                """,
                # sale_record_shiwa
                """
                CREATE TABLE IF NOT EXISTS sale_record_shiwa (
                    id SERIAL PRIMARY KEY,
                    pond_id INTEGER REFERENCES pond_shiwa(id),
                    customer_id INTEGER REFERENCES customer_shiwa(id),
                    sale_type VARCHAR(20) CHECK (sale_type IN ('零售','批发')),
                    quantity INTEGER NOT NULL,
                    unit_price NUMERIC(10,2) NOT NULL,
                    total_amount NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
                    weight_jin NUMERIC(10,2),
                    note TEXT,
                    sold_at TIMESTAMP DEFAULT NOW(),
                    sold_by VARCHAR(50)
                );
                """,
                # death_image_shiwa
                """
                CREATE TABLE IF NOT EXISTS death_image_shiwa (
                    id SERIAL PRIMARY KEY,
                    death_movement_id INTEGER REFERENCES stock_movement_shiwa(id) ON DELETE CASCADE,
                    image_path TEXT NOT NULL
                );
                """,
                # pond_life_cycle_shiwa
                """
                CREATE TABLE IF NOT EXISTS pond_life_cycle_shiwa (
                    id SERIAL PRIMARY KEY,
                    movement_id INTEGER REFERENCES stock_movement_shiwa(id),
                    pond_id INTEGER REFERENCES pond_shiwa(id),
                    frog_type_id INTEGER REFERENCES frog_type_shiwa(id),
                    quantity INTEGER NOT NULL,
                    start_at DATE DEFAULT CURRENT_DATE,
                    stage VARCHAR(20)
                );
                """,
                # pond_change_log
                """
                CREATE TABLE IF NOT EXISTS pond_change_log (
                    id SERIAL PRIMARY KEY,
                    pond_id INTEGER NOT NULL REFERENCES pond_shiwa(id) ON DELETE CASCADE,
                    change_type VARCHAR(20) NOT NULL CHECK (change_type IN ('修正创建', '变更用途')),
                    old_name TEXT,
                    new_name TEXT,
                    old_pond_type_id INTEGER,
                    new_pond_type_id INTEGER,
                    old_frog_type_id INTEGER,
                    new_frog_type_id INTEGER,
                    old_max_capacity INTEGER,
                    new_max_capacity INTEGER,
                    old_current_count INTEGER,
                    new_current_count INTEGER,
                    change_date DATE NOT NULL,
                    notes TEXT,
                    changed_by VARCHAR(50),
                    changed_at TIMESTAMP DEFAULT NOW()
                );
                """,
            ]

            for sql in tables_sql:
                cur.execute(sql)

            # ========== 2. 重点：升级 feed_purchase_record_shiwa ==========
            cur.execute("""
                CREATE TABLE IF NOT EXISTS feed_purchase_record_shiwa (
                    id SERIAL PRIMARY KEY,
                    feed_type_name VARCHAR(100) NOT NULL,
                    quantity_kg NUMERIC(12,2) NOT NULL,
                    unit_price NUMERIC(10,2) NOT NULL,
                    total_amount NUMERIC(14,2) GENERATED ALWAYS AS (quantity_kg * unit_price) STORED,
                    supplier VARCHAR(100),
                    supplier_phone VARCHAR(50),
                    purchased_by VARCHAR(50),
                    purchased_at TIMESTAMP DEFAULT NOW(),
                    notes TEXT
                );
            """)

            # ========== 3. 重点：升级 frog_purchase_record_shiwa ==========
            cur.execute("""
                CREATE TABLE IF NOT EXISTS frog_purchase_record_shiwa (
                    id SERIAL PRIMARY KEY,
                    frog_type_name VARCHAR(100) NOT NULL,
                    quantity INTEGER NOT NULL,
                    unit_price NUMERIC(10,2) NOT NULL,
                    total_amount NUMERIC(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
                    supplier VARCHAR(100),
                    supplier_phone VARCHAR(50),
                    purchased_by VARCHAR(50),
                    purchased_at TIMESTAMP DEFAULT NOW(),
                    notes TEXT
                );
            """)

            # ========== 4. 重点：创建/升级 stock_movement_shiwa ==========
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_movement_shiwa (
                    id SERIAL PRIMARY KEY,
                    movement_type VARCHAR(20) CHECK (movement_type IN ('transfer','purchase','hatch','sale','death')),
                    from_pond_id INTEGER REFERENCES pond_shiwa(id),
                    to_pond_id INTEGER REFERENCES pond_shiwa(id),
                    quantity INTEGER NOT NULL,
                    description TEXT,
                    unit_price NUMERIC(10,2),
                    created_by VARCHAR(50),
                    moved_at TIMESTAMP DEFAULT NOW(),
                    frog_purchase_type_id INTEGER REFERENCES frog_purchase_type_shiwa(id)
                );
            """)

            # ========== 5. 自动修复：检查缺失字段并添加 ==========
            # 5.1 feed_purchase_record_shiwa.notes
            if not column_exists(cur, 'feed_purchase_record_shiwa', 'notes'):
                cur.execute("ALTER TABLE feed_purchase_record_shiwa ADD COLUMN notes TEXT;")

            # 5.2 frog_purchase_record_shiwa.notes
            if not column_exists(cur, 'frog_purchase_record_shiwa', 'notes'):
                cur.execute("ALTER TABLE frog_purchase_record_shiwa ADD COLUMN notes TEXT;")

            # 5.3 stock_movement_shiwa.frog_purchase_type_id
            if not column_exists(cur, 'stock_movement_shiwa', 'frog_purchase_type_id'):
                cur.execute("""
                    ALTER TABLE stock_movement_shiwa
                    ADD COLUMN frog_purchase_type_id INTEGER
                    REFERENCES frog_purchase_type_shiwa(id);
                """)

            # ========== 6. 创建索引 ==========
            indexes = [
                ("idx_pond_type", "pond_shiwa(pond_type_id)"),
                ("idx_pond_frog", "pond_shiwa(frog_type_id)"),
                ("idx_movement_from", "stock_movement_shiwa(from_pond_id)"),
                ("idx_movement_to", "stock_movement_shiwa(to_pond_id)"),
                ("idx_feed_pond", "feeding_record_shiwa(pond_id)"),
                ("idx_sale_pond", "sale_record_shiwa(pond_id)"),
                ("idx_daily_pond", "daily_log_shiwa(pond_id)"),
                ("idx_feed_purchase_time", "feed_purchase_record_shiwa(purchased_at)"),
                ("idx_frog_purchase_time", "frog_purchase_record_shiwa(purchased_at)"),
                ("idx_movement_frog_purchase", "stock_movement_shiwa(frog_purchase_type_id)"),
            ]
            for idx_name, cols in indexes:
                if not index_exists(cur, idx_name):
                    cur.execute(f"CREATE INDEX {idx_name} ON {cols};")

            # ========== 7. 创建视图 ==========
            cur.execute("""
                CREATE OR REPLACE VIEW pond_reminder_v AS
                SELECT
                    p.id AS pond_id,
                    p.name AS pond_name,
                    ft.name AS frog_type,
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
            """)

        conn.commit()

    print("✅ 中益石蛙基地数据库已初始化或自动修复完成！")
    print("📌 请确保 .env 里 DATABASE_SHIWA_URL 配置正确，然后启动 Streamlit。")

if __name__ == "__main__":
    main()