import streamlit as st
import os
from urllib.parse import urlparse
import psycopg2
from datetime import datetime, time
from PIL import Image
import io
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import uuid

# ================== ① AI 问答新增依赖 ==================
import json, tempfile, pandas as pd
from datetime import datetime
from openai import OpenAI
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
# =======================================================

# -----------------------------
# 加载环境变量
# -----------------------------
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_SHIWA_URL")
if not DATABASE_URL:
    st.error("❌ DATABASE_SHIWA_URL 未在 .env 中设置！")
    st.stop()

# 解析数据库 URL
try:
    url = urlparse(DATABASE_URL)
    DB_CONFIG = {
        "host": url.hostname,
        "port": url.port or 5432,
        "database": url.path[1:],
        "user": url.username,
        "password": url.password,
    }
except Exception as e:
    st.error(f"❌ 数据库 URL 解析失败: {e}")
    st.stop()

# 确保死亡图片目录存在
DEATH_IMAGE_DIR = "death_images"
os.makedirs(DEATH_IMAGE_DIR, exist_ok=True)

# -----------------------------
# 🔐 用户认证依赖（新增）
# -----------------------------
# ----------------------------- 
# 🔐 用户认证依赖（新增）
# ----------------------------- 
from passlib.context import CryptContext
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(password: str) -> str:
    """生成 bcrypt 哈希，自动截断超过 72 字节的密码"""
    # 1. 编码成字节
    pwd_bytes = password.encode('utf-8')
    # 2. 截断为 72 字节（bcrypt 上限）
    pwd_bytes = pwd_bytes[:72]
    # 3. 解码回字符串（忽略不完整的多字节字符）
    pwd_str = pwd_bytes.decode('utf-8', errors='ignore')
    return pwd_context.hash(pwd_str)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """校验密码，同样需要先截断"""
    plain_bytes = plain_password.encode('utf-8')[:72]
    plain_str   = plain_bytes.decode('utf-8', errors='ignore')
    return pwd_context.verify(plain_str, hashed_password)

# -----------------------------
# 数据库工具函数
# -----------------------------
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def table_exists(cursor, table_name):
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (table_name,))
    return cursor.fetchone()[0]

# -----------------------------
# 🔐 用户相关数据库函数（新增）
# -----------------------------
def create_user(username: str, password: str, department: str):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        hashed = hash_password(password)
        cur.execute("""
            INSERT INTO user_shiwa (username, password_hash, department, role)
            VALUES (%s, %s, %s, '员工');
        """, (username.strip(), hashed, department))
        conn.commit()
    except psycopg2.IntegrityError as e:
        conn.rollback()
        if "unique_username" in str(e) or "duplicate key" in str(e):
            raise ValueError("用户名已存在")
        else:
            raise e
    finally:
        cur.close()
        conn.close()

def get_user_by_username(username: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, username, password_hash, department, role FROM user_shiwa WHERE username = %s;", (username,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row  # (id, username, password_hash, department, role) 或 None

# -----------------------------
# 初始化用户表（如果不存在）
# -----------------------------
def init_user_table():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_shiwa (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            department VARCHAR(20) NOT NULL CHECK (department IN ('管理部', '现场部')),
            role VARCHAR(20) DEFAULT '员工',
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

TRANSFER_PATH_RULES = {
    "种蛙池": ["商品蛙池","三年蛙池", "四年蛙池", "五年蛙池", "六年蛙池", "试验池", "种蛙池"],
    "孵化池": ["养殖池", "试验池", "孵化池"],
    "养殖池": ["商品蛙池", "种蛙池", "三年蛙池", "四年蛙池", "五年蛙池", "六年蛙池", "试验池", "养殖池"],
    "商品蛙池": ["三年蛙池", "四年蛙池", "五年蛙池", "六年蛙池", "试验池", "商品蛙池"],
    "试验池": ["三年蛙池", "四年蛙池", "五年蛙池", "六年蛙池", "试验池"],
}
# ============== 常用备注短语字典 ==============
COMMON_REMARKS = {
    "喂养备注": [
        "",
        "正常投喂",
        "加量投喂",
        "减量投喂",
        "蛙群活跃",
        "蛙群食欲一般",
        "剩料较多",
        "今日换水",
        "水温偏高，减料",
        "水温偏低，加料",
        "下雨延迟投喂"
    ],
    "每日观察": [
        "",
        "蛙群活跃，摄食正常",
        "发现个别浮头",
        "水面有泡沫",
        "池底粪便较多",
        "蝌蚪集群正常",
        "卵块增加",
        "发现有死亡个体",
        "活动力下降",
        "皮肤颜色正常",
        "换水后活跃"
    ],
    "操作描述": [
        "",
        "日常转池",
        "密度调整",
        "大小分级",
        "外购新苗",
        "自繁孵化",
        "病害隔离",
        "销售备货",
        "实验观察",
        "清池消毒",
        "暴雨后应急转移"
    ]
}
def execute_safe_select(sql: str) -> pd.DataFrame:
    """只允许 SELECT，返回 DataFrame"""
    # 移除重复的 import pandas as pd，直接使用全局导入的 pd
    sql = sql.strip()
    if not sql.lower().startswith("select"):
        raise ValueError("仅允许 SELECT 查询")
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)
# ==========================================
def get_recent_movements(limit=20):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT sm.id,
               CASE sm.movement_type
                   WHEN 'transfer' THEN '转池'
                   WHEN 'purchase' THEN '外购'
                   WHEN 'hatch'    THEN '孵化'
                   WHEN 'sale'     THEN '销售出库'
                   WHEN 'death'    THEN '死亡'
               END AS movement_type,
               fp.name   AS from_name,
               tp.name   AS to_name,
               sm.quantity,
               sm.description,
               sm.moved_at,
               sm.created_by AS 操作人
        FROM stock_movement_shiwa sm
        LEFT JOIN pond_shiwa fp ON sm.from_pond_id = fp.id
        LEFT JOIN pond_shiwa tp ON sm.to_pond_id = tp.id
        ORDER BY sm.moved_at DESC
        LIMIT %s;
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
# -----------------------------
# 业务功能函数
# -----------------------------
def get_all_ponds():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.id, p.name, pt.name AS pond_type, ft.name AS frog_type, 
               p.max_capacity, p.current_count
        FROM pond_shiwa p
        JOIN pond_type_shiwa pt ON p.pond_type_id = pt.id
        JOIN frog_type_shiwa ft ON p.frog_type_id = ft.id
        ORDER BY p.id;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def add_feeding_record(pond_id, feed_type_id, weight_kg, unit_price, notes,
                       fed_at=None, fed_by=None):
    fed_at = fed_at or datetime.utcnow()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # ① 先扣饲料库存
        cur.execute("SELECT stock_kg FROM feed_type_shiwa WHERE id = %s FOR UPDATE;", (feed_type_id,))
        row = cur.fetchone()
        if not row or row[0] < weight_kg:
            raise ValueError("该饲料库存不足，无法投喂！")
        cur.execute("UPDATE feed_type_shiwa SET stock_kg = stock_kg - %s WHERE id = %s;", (weight_kg, feed_type_id))

        # ② 写入喂养记录（不再给 total_cost）
        cur.execute("""
            INSERT INTO feeding_record_shiwa
            (pond_id, feed_type_id, feed_weight_kg, unit_price_at_time, notes, fed_at, fed_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (pond_id, feed_type_id, weight_kg, unit_price, notes, fed_at, fed_by))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close(); conn.close()


def get_feed_types():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, unit_price FROM feed_type_shiwa ORDER BY name;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_pond_types():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM pond_type_shiwa ORDER BY id;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_frog_types():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM frog_type_shiwa;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def create_pond(name, pond_type_id, frog_type_id, max_capacity, initial_count=0):
    initial_count = max(0, min(initial_count, max_capacity))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 👇 先检查重名
        cur.execute("SELECT 1 FROM pond_shiwa WHERE name = %s;", (name.strip(),))
        if cur.fetchone():
            raise ValueError(f"池塘名称「{name}」已存在，请勿重复创建！")

        cur.execute("""
            INSERT INTO pond_shiwa (name, pond_type_id, frog_type_id, max_capacity, current_count)
            VALUES (%s, %s, %s, %s, %s);
        """, (name.strip(), pond_type_id, frog_type_id, max_capacity, initial_count))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()
def is_pond_unused(pond_id: int) -> bool:
    """
    判断池塘是否从未被使用过（无喂养、无转池/外购/孵化/死亡/销售、无日志）
    注意：允许有初始数量，只要没发生过任何操作即可修改
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1. 检查 feeding_record_shiwa
        cur.execute("SELECT 1 FROM feeding_record_shiwa WHERE pond_id = %s LIMIT 1;", (pond_id,))
        if cur.fetchone():
            return False

        # 2. 检查 stock_movement_shiwa（作为 from 或 to）
        cur.execute("""
            SELECT 1 FROM stock_movement_shiwa 
            WHERE from_pond_id = %s OR to_pond_id = %s 
            LIMIT 1;
        """, (pond_id, pond_id))
        if cur.fetchone():
            return False

        # 3. 检查 daily_log_shiwa
        cur.execute("SELECT 1 FROM daily_log_shiwa WHERE pond_id = %s LIMIT 1;", (pond_id,))
        if cur.fetchone():
            return False

        return True
    finally:
        cur.close()
        conn.close()
def update_pond_identity(pond_id: int,
                        new_name: str,
                        new_pond_type_id: int,
                        new_frog_type_id: int) -> tuple[bool, str]:
    """
    变更池塘身份（名称、类型、蛙种）
    仅要求当前数量为 0，允许曾参与过业务流程（喂养/转池/日志等）
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # ✅ 新逻辑：只检查 current_count 是否为 0
        cur.execute("SELECT current_count FROM pond_shiwa WHERE id = %s;", (pond_id,))
        row = cur.fetchone()
        if not row:
            return False, "池塘不存在"
        if row[0] != 0:
            return False, "池塘当前数量不为 0，无法变更用途"

        cur.execute("""
            UPDATE pond_shiwa
            SET name = %s,
                pond_type_id = %s,
                frog_type_id = %s,
                updated_at = NOW()
            WHERE id = %s;
        """, (new_name, new_pond_type_id, new_frog_type_id, pond_id))
        conn.commit()
        return True, ""
    except psycopg2.IntegrityError as e:
        conn.rollback()
        if "unique_pond_name" in str(e):
            return False, f"新名称「{new_name}」已存在，请更换编号"
        return False, f"数据库约束错误：{e}"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()
def update_pond_full(
    pond_id: int,
    new_name: str,
    new_pond_type_id: int,
    new_frog_type_id: int,
    new_max_capacity: int,
    new_current_count: int
) -> tuple[bool, str]:
    """
    完整修改池塘信息（仅限从未使用过的池塘）
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if not is_pond_unused(pond_id):
            return False, "池塘已参与业务流程（喂养/转池/日志等），无法修正创建信息"

        cur.execute("""
            UPDATE pond_shiwa
            SET name = %s,
                pond_type_id = %s,
                frog_type_id = %s,
                max_capacity = %s,
                current_count = %s,
                updated_at = NOW()
            WHERE id = %s;
        """, (
            new_name,
            new_pond_type_id,
            new_frog_type_id,
            new_max_capacity,
            new_current_count,
            pond_id
        ))
        conn.commit()
        return True, ""
    except psycopg2.IntegrityError as e:
        conn.rollback()
        if "unique_pond_name" in str(e):
            return False, f"新名称「{new_name}」已存在，请更换编号"
        return False, f"数据库约束错误：{e}"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()

def get_pond_by_id(pond_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, frog_type_id, max_capacity, current_count
        FROM pond_shiwa WHERE id = %s;
    """, (pond_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row  # (id, name, frog_type_id, max_capacity, current_count)
def log_pond_change(
    pond_id: int,
    change_type: str,
    old_values: dict,
    new_values: dict,
    change_date: datetime.date,
    notes: str,
    changed_by: str
):
    """记录池塘变更日志到 pond_change_log 表"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO pond_change_log (
                pond_id, change_type,
                old_name, new_name,
                old_pond_type_id, new_pond_type_id,
                old_frog_type_id, new_frog_type_id,
                old_max_capacity, new_max_capacity,
                old_current_count, new_current_count,
                change_date, notes, changed_by
            ) VALUES (
                %(pond_id)s, %(change_type)s,
                %(old_name)s, %(new_name)s,
                %(old_pond_type_id)s, %(new_pond_type_id)s,
                %(old_frog_type_id)s, %(new_frog_type_id)s,
                %(old_max_capacity)s, %(new_max_capacity)s,
                %(old_current_count)s, %(new_current_count)s,
                %(change_date)s, %(notes)s, %(changed_by)s
            );
        """, {
            "pond_id": pond_id,
            "change_type": change_type,
            "old_name": old_values.get("name"),
            "new_name": new_values.get("name"),
            "old_pond_type_id": old_values.get("pond_type_id"),
            "new_pond_type_id": new_values.get("pond_type_id"),
            "old_frog_type_id": old_values.get("frog_type_id"),
            "new_frog_type_id": new_values.get("frog_type_id"),
            "old_max_capacity": old_values.get("max_capacity"),
            "new_max_capacity": new_values.get("max_capacity"),
            "old_current_count": old_values.get("current_count"),
            "new_current_count": new_values.get("current_count"),
            "change_date": change_date,
            "notes": notes or "",
            "changed_by": changed_by
        })
        conn.commit()
    finally:
        cur.close()
        conn.close()
def _log_life_start(conn, movement_id, to_pond_id, quantity, movement_type):
    cur = conn.cursor()
    cur.execute("SELECT frog_type_id FROM pond_shiwa WHERE id=%s", (to_pond_id,))
    frog_type_id = cur.fetchone()[0]
    stage = '卵' if movement_type in ('hatch', 'purchase') else '幼蛙'
    cur.execute("""
        INSERT INTO pond_life_cycle_shiwa
        (movement_id, pond_id, frog_type_id, quantity, start_at, stage)
        VALUES (%s, %s, %s, %s, CURRENT_DATE, %s)
    """, (movement_id, to_pond_id, frog_type_id, quantity, stage))
def add_stock_movement(movement_type, from_pond_id, to_pond_id, quantity,
                       description, unit_price=None, created_by=None, moved_at=None, frog_type_id=None):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        actual_moved_at = moved_at or datetime.utcnow()
        
        # ========== 处理采购分配：扣采购库存 + 加池塘数量 ==========
        if movement_type == 'purchase':
            if frog_type_id is None or to_pond_id is None:
                raise ValueError("外购操作必须提供 frog_type_id 和 to_pond_id")
            # 扣采购库存
            cur.execute("SELECT quantity FROM frog_purchase_type_shiwa WHERE id = %s FOR UPDATE;", (frog_type_id,))
            row = cur.fetchone()
            if not row or row[0] < quantity:
                raise ValueError("采购库存不足")
            cur.execute("UPDATE frog_purchase_type_shiwa SET quantity = quantity - %s WHERE id = %s;", (quantity, frog_type_id))
            # 加目标池数量
            cur.execute("UPDATE pond_shiwa SET current_count = current_count + %s WHERE id = %s;", (quantity, to_pond_id))
            # 插入 movement
            cur.execute("""
                INSERT INTO stock_movement_shiwa
                (movement_type, from_pond_id, to_pond_id, quantity, description, unit_price, created_by, moved_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, ('purchase', None, to_pond_id, quantity, description, unit_price, created_by, actual_moved_at))
            movement_id = cur.fetchone()[0]
            _log_life_start(conn, movement_id, to_pond_id, quantity, 'purchase')
        
        # ========== 其他类型：转池 / 孵化 / 死亡 ==========
        else:
            if movement_type == 'transfer':
                if from_pond_id is None or to_pond_id is None:
                    raise ValueError("转池必须指定源池和目标池")
                cur.execute("UPDATE pond_shiwa SET current_count = current_count - %s WHERE id = %s;", (quantity, from_pond_id))
                cur.execute("UPDATE pond_shiwa SET current_count = current_count + %s WHERE id = %s;", (quantity, to_pond_id))
            elif movement_type == 'hatch':
                if to_pond_id is None:
                    raise ValueError("孵化必须指定目标池")
                cur.execute("UPDATE pond_shiwa SET current_count = current_count + %s WHERE id = %s;", (quantity, to_pond_id))
            elif movement_type == 'death':
                if from_pond_id is None:
                    raise ValueError("死亡必须指定源池")
                cur.execute("UPDATE pond_shiwa SET current_count = current_count - %s WHERE id = %s;", (quantity, from_pond_id))
            else:
                raise ValueError(f"不支持的 movement_type: {movement_type}")
            
            cur.execute("""
                INSERT INTO stock_movement_shiwa
                (movement_type, from_pond_id, to_pond_id, quantity, description, unit_price, created_by, moved_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (movement_type, from_pond_id, to_pond_id, quantity, description, unit_price, created_by, actual_moved_at))
            movement_id = cur.fetchone()[0]
            if movement_type in ('transfer', 'hatch'):
                _log_life_start(conn, movement_id, to_pond_id, quantity, movement_type)

        conn.commit()
        return True, None
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()

def add_death_record(from_pond_id: int, quantity: int, note: str = "", image_files=None, created_by: str = None, moved_at=None):
    """
    记录死亡事件：
    1. 扣减源池 current_count
    2. 插入 stock_movement_shiwa（movement_type='death'）
    3. 保存死亡照片（如有）
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        actual_moved_at = moved_at or datetime.utcnow()
        
        # ✅ 关键修复：先扣减源池数量
        cur.execute("UPDATE pond_shiwa SET current_count = current_count - %s WHERE id = %s;", (quantity, from_pond_id))
        
        # 插入 movement 记录
        cur.execute("""
            INSERT INTO stock_movement_shiwa
            (movement_type, from_pond_id, to_pond_id, quantity, description, created_by, moved_at)
            VALUES ('death', %s, NULL, %s, %s, %s, %s)
            RETURNING id;
        """, (from_pond_id, quantity, note or f"死亡 {quantity} 只", created_by, actual_moved_at))
        movement_id = cur.fetchone()[0]

        # ========== 保存死亡照片（如有）==========
        if image_files:
            for uploaded_file in image_files:
                if uploaded_file is not None:
                    # 生成唯一文件名
                    ext = os.path.splitext(uploaded_file.name)[1].lower()
                    if ext not in ['.png', '.jpg', '.jpeg']:
                        continue  # 跳过非图片
                    unique_filename = f"{uuid.uuid4().hex}{ext}"
                    save_path = os.path.join(DEATH_IMAGE_DIR, unique_filename)
                    # 保存文件
                    with open(save_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    # 记录到数据库
                    cur.execute("""
                        INSERT INTO death_image_shiwa (death_movement_id, image_path)
                        VALUES (%s, %s);
                    """, (movement_id, save_path))

        conn.commit()
        return True, None
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()
def get_recent_death_records(limit=20, offset=0):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1️⃣ 先查死亡记录（不 JOIN 图片）
        cur.execute("""
            SELECT 
                sm.id,
                p.name AS pond_name,
                sm.quantity,
                sm.description,
                sm.moved_at,
                sm.created_by AS 操作人
            FROM stock_movement_shiwa sm
            JOIN pond_shiwa p ON sm.from_pond_id = p.id
            WHERE sm.movement_type = 'death'
            ORDER BY sm.moved_at DESC
            LIMIT %s OFFSET %s;
        """, (limit, offset))
        death_rows = cur.fetchall()  # [(id, pond, qty, desc, time, user), ...]
        death_ids = [row[0] for row in death_rows]

        # 2️⃣ 再查这些死亡记录对应的图片（批量查询）
        image_dict = {}
        if death_ids:
            cur.execute("""
                SELECT death_movement_id, image_path
                FROM death_image_shiwa
                WHERE death_movement_id = ANY(%s);
            """, (death_ids,))
            for mid, path in cur.fetchall():
                if mid not in image_dict:
                    image_dict[mid] = []
                image_dict[mid].append(path)

        # 3️⃣ 合并：每条死亡记录 + 其图片列表
        result = []
        for row in death_rows:
            mid = row[0]
            images = image_dict.get(mid, [])
            result.append((mid, row[1], row[2], row[3], row[4], row[5], images))
        return result
    finally:
        cur.close()
        conn.close()
def get_pond_type_id_by_name(name):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id FROM pond_type_shiwa WHERE name = %s;", (name,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None
# 在 initialize_database() 之后、run() 之前定义（或在 run() 开头缓存到 session_state）
def get_pond_type_map():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM pond_type_shiwa;")
    mapping = {row[1]: row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return mapping
# 新增
def get_frog_purchase_types():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, unit_price FROM frog_purchase_type_shiwa ORDER BY id;")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def add_frog_purchase_type(name, price):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO frog_purchase_type_shiwa (name, unit_price)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE SET unit_price = EXCLUDED.unit_price;
    """, (name, price))
    conn.commit(); cur.close(); conn.close()
# ---------- 客户 ----------
def get_customers():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, phone, type FROM customer_shiwa ORDER BY id;")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def add_customer(name, phone, ctype):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO customer_shiwa (name, phone, type) VALUES (%s,%s,%s) RETURNING id;",
        (name, phone, ctype)
    )
    cid = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return cid

# ---------- 销售 ----------
def do_sale(pond_id, customer_id, sale_type, qty_zhi, unit_price_per_zhi, 
            weight_jin=None, note="", sold_by=None):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # ❌ 不要计算 total_amount，也不要插入它！
        cur.execute("""
            INSERT INTO sale_record_shiwa 
            (pond_id, customer_id, sale_type, quantity, unit_price, 
             weight_jin, note, sold_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            pond_id, customer_id, sale_type, qty_zhi, unit_price_per_zhi,
            weight_jin, note, sold_by
        ))
        # 扣库存
        cur.execute("UPDATE pond_shiwa SET current_count = current_count - %s WHERE id = %s;",
                    (qty_zhi, pond_id))
        # 记录 movement
        cur.execute("""
            INSERT INTO stock_movement_shiwa (movement_type, from_pond_id, to_pond_id, quantity, description)
            VALUES ('sale', %s, NULL, %s, %s);
        """, (pond_id, qty_zhi, f"销售：{sale_type} {weight_jin} 斤"))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# ---------- 最近销售 ----------
def get_recent_sales(limit=20):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT sr.id, p.name pond, c.name customer, sr.sale_type, sr.quantity,
               sr.unit_price, sr.total_amount, sr.sold_at, sr.note
        FROM sale_record_shiwa sr
        JOIN pond_shiwa p ON p.id = sr.pond_id
        JOIN customer_shiwa c ON c.id = sr.customer_id
        ORDER BY sr.sold_at DESC
        LIMIT %s;
    """, (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows
# -----------------------------
# ROI 分析专用函数
# -----------------------------
def get_roi_data():
    conn = get_db_connection()
    cur = conn.cursor()

    # 获取所有蛙种（确保细皮蛙、粗皮蛙都在）
    cur.execute("SELECT name FROM frog_type_shiwa ORDER BY name;")
    all_frog_types = [row[0] for row in cur.fetchall()]
    if not all_frog_types:
        all_frog_types = ["细皮蛙", "粗皮蛙"]  # 安全兜底

    # 1. 喂养成本
    cur.execute("""
        SELECT ft.name, COALESCE(SUM(fr.total_cost), 0)
        FROM frog_type_shiwa ft
        LEFT JOIN pond_shiwa p ON ft.id = p.frog_type_id
        LEFT JOIN feeding_record_shiwa fr ON p.id = fr.pond_id
        GROUP BY ft.name;
    """)
    feed_dict = {row[0]: float(row[1]) for row in cur.fetchall()}

    # 2. 外购成本（使用 unit_price，若为 NULL 则按 20.0 估算）
    cur.execute("""
        SELECT ft.name, 
               COALESCE(SUM(sm.quantity * COALESCE(sm.unit_price, 20.0)), 0) AS total_cost
        FROM frog_type_shiwa ft
        LEFT JOIN pond_shiwa p ON ft.id = p.frog_type_id
        LEFT JOIN stock_movement_shiwa sm 
            ON p.id = sm.to_pond_id AND sm.movement_type = 'purchase'
        GROUP BY ft.name;
    """)
    purchase_dict = {row[0]: float(row[1]) for row in cur.fetchall()}

    # 3. 销售收入
    cur.execute("""
        SELECT ft.name, COALESCE(SUM(sr.total_amount), 0)
        FROM frog_type_shiwa ft
        LEFT JOIN pond_shiwa p ON ft.id = p.frog_type_id
        LEFT JOIN sale_record_shiwa sr ON p.id = sr.pond_id
        GROUP BY ft.name;
    """)
    sales_dict = {row[0]: float(row[1]) for row in cur.fetchall()}

    cur.close()
    conn.close()

    # 构建结果（确保所有蛙种都有行）
    result = []
    for frog_type in all_frog_types:
        feed = feed_dict.get(frog_type, 0.0)
        purchase = purchase_dict.get(frog_type, 0.0)
        total_cost = feed + purchase
        income = sales_dict.get(frog_type, 0.0)
        profit = income - total_cost
        roi = (profit / total_cost * 100) if total_cost > 0 else 0.0

        result.append({
            "蛙种": frog_type,
            "喂养成本 (¥)": round(feed, 2),
            "外购成本 (¥)": round(purchase, 2),
            "总成本 (¥)": round(total_cost, 2),
            "销售收入 (¥)": round(income, 2),
            "净利润 (¥)": round(profit, 2),
            "ROI (%)": round(roi, 2)
        })

    return result
def get_pond_roi_details():
    """获取每个池塘的喂养、外购、销售明细，用于 ROI 明细分析"""
    conn = get_db_connection()
    cur = conn.cursor()

    # 1. 喂养明细
    cur.execute("""
        SELECT 
            p.name AS pond_name,
            ft.name AS frog_type,
            fr.feed_weight_kg,
            ftype.name AS feed_type,
            fr.unit_price_at_time,
            fr.total_cost,
            fr.fed_at
        FROM feeding_record_shiwa fr
        JOIN pond_shiwa p ON fr.pond_id = p.id
        JOIN frog_type_shiwa ft ON p.frog_type_id = ft.id
        JOIN feed_type_shiwa ftype ON fr.feed_type_id = ftype.id
        ORDER BY fr.fed_at DESC;
    """)
    feedings = cur.fetchall()

    # 2. 外购明细（movement_type = 'purchase'）
    cur.execute("""
        SELECT 
            p.name AS pond_name,
            ft.name AS frog_type,
            sm.quantity,
            sm.unit_price,
            (sm.quantity * COALESCE(sm.unit_price, 20.0)) AS total_cost,
            sm.moved_at
        FROM stock_movement_shiwa sm
        JOIN pond_shiwa p ON sm.to_pond_id = p.id
        JOIN frog_type_shiwa ft ON p.frog_type_id = ft.id
        WHERE sm.movement_type = 'purchase'
        ORDER BY sm.moved_at DESC;
    """)
    purchases = cur.fetchall()

    # 3. 销售明细
    cur.execute("""
        SELECT 
            p.name AS pond_name,
            ft.name AS frog_type,
            sr.quantity,
            sr.unit_price,
            sr.total_amount,
            sr.sold_at,
            c.name AS customer_name
        FROM sale_record_shiwa sr
        JOIN pond_shiwa p ON sr.pond_id = p.id
        JOIN frog_type_shiwa ft ON p.frog_type_id = ft.id
        JOIN customer_shiwa c ON sr.customer_id = c.id
        ORDER BY sr.sold_at DESC;
    """)
    sales = cur.fetchall()

    cur.close()
    conn.close()

    return feedings, purchases, sales
def add_daily_log(pond_id, log_date, water_temp, ph_value, weather,
                  observation, do_value=None, humidity=None,
                  water_source=None, recorded_by=None):
    """
    写入/覆盖每日记录，新增天气 & 水源 & 记录人
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO daily_log_shiwa
        (pond_id, log_date, water_temp, ph_value, weather, observation,
         do_value, humidity, water_source, recorded_by, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
        ON CONFLICT (pond_id, log_date)
        DO UPDATE SET
            water_temp   = EXCLUDED.water_temp,
            ph_value     = EXCLUDED.ph_value,
            weather      = EXCLUDED.weather,
            observation  = EXCLUDED.observation,
            do_value     = EXCLUDED.do_value,
            humidity     = EXCLUDED.humidity,
            water_source = EXCLUDED.water_source,
            recorded_by  = EXCLUDED.recorded_by,
            updated_at   = NOW();
    """, (pond_id, log_date, water_temp, ph_value, weather, observation,
          do_value, humidity, water_source, recorded_by))
    conn.commit()
    cur.close(); conn.close()

def get_daily_logs(limit=50):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT dl.log_date,
               p.name,
               dl.water_temp,
               dl.ph_value,
               dl.do_value,
               dl.humidity,
               dl.light_condition,
               dl.observation
        FROM daily_log_shiwa dl
        JOIN pond_shiwa p ON dl.pond_id = p.id
        ORDER BY dl.log_date DESC, dl.created_at DESC
        LIMIT %s;
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
# ================== ② AI 问答专用函数 ==================
def get_ai_client():
    """统一拿到 DashScope 兼容 OpenAI 客户端"""
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        raise RuntimeError("请在 .env 里配置 DASHSCOPE_API_KEY")
    return OpenAI(api_key=api_key,
                  base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")

@st.cache_data(show_spinner=False)
def get_db_schema_for_ai():
    """一次性把 schema 抓回来给 AI，只抓表名-列名-类型，不做数据"""
    engine = create_engine(DATABASE_URL)
    inspector = inspect(engine)
    schema = {}
    for t in inspector.get_table_names():
        schema[t] = [{"col": c["name"], "type": str(c["type"])}
                     for c in inspector.get_columns(t)]
    return schema



def ai_ask_database(question: str):
    """两阶段：生成 SQL -> 自然语言回答"""
    client = get_ai_client()
    schema = get_db_schema_for_ai()

    tools = [{
        "type": "function",
        "function": {
            "name": "execute_sql_query",
            "description": "生成安全的 SELECT 查询",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string"},
                    "explanation": {"type": "string"}
                },
                "required": ["sql", "explanation"]
            }
        }
    }]

    sys_prompt = f"""
你是石蛙养殖场数据分析师，数据库 schema 如下（仅使用存在的表和字段）：
{json.dumps(schema, ensure_ascii=False, indent=2)}

必须调用 execute_sql_query 函数，规则：
- 只生成 SELECT
- 表名/字段严格与上面一致
- 用中文写 explanation
"""

    response = client.chat.completions.create(
        model="qwen-plus",
        messages=[{"role": "system", "content": sys_prompt},
                  {"role": "user", "content": question}],
        tools=tools,
        tool_choice={"type": "function", "function": {"name": "execute_sql_query"}},
        temperature=0.1
    )

    args = json.loads(response.choices[0].message.tool_calls[0].function.arguments)
    sql = args["sql"]
    df = execute_safe_select(sql)

    # 第二阶段：用数据回答用户
    second = client.chat.completions.create(
        model="qwen-plus",
        messages=[
            {"role": "system", "content": "你是石蛙养殖场场长，用简洁中文直接回答用户问题，不要提 SQL 或技术词汇。"},
            {"role": "user", "content": f"用户问题：{question}\n查询结果：\n{df.head(15).to_string(index=False)}"}
        ],
        temperature=0.3
    )
    return second.choices[0].message.content.strip(), sql, df
# =======================================================
# ----------------------------- ① 池子分组 -----------------------------
def group_ponds_by_type(pond_dict):
        from collections import defaultdict
        grouped = defaultdict(list)
        for pid, info in pond_dict.items():
            grouped[info["pond_type"]].append(
                (pid, f"{info['name']}  （当前 {info['current_count']} / {info['max_capacity']}）")
            )
        return grouped


    # ----------------------------- ② 两级选择组件 -----------------------------
def pond_selector(label, candidate_dict, grouped, key):
        """两步选池：先类型 → 再具体池子"""
        col1, col2 = st.columns([1, 2])
        with col1:
            type_pick = st.selectbox(f"{label} · 类型", options=list(grouped.keys()), key=f"{key}_type")
        with col2:
            pid_pick = st.selectbox(f"{label} · 池子", options=[p[0] for p in grouped[type_pick]],
                                    format_func=lambda x: next(p[1] for p in grouped[type_pick] if p[0] == x),
                                    key=f"{key}_pond")
        return pid_pick
def show_login_page():
    st.title("🔐 用户登录 - 中益石蛙基地")
    with st.form("login_form"):
        username = st.text_input("用户名")
        password = st.text_input("密码", type="password")
        submitted = st.form_submit_button("登录")
        if submitted:
            user = get_user_by_username(username)
            if user and verify_password(password, user[2]):
                st.session_state.logged_in = True
                st.session_state.user = {
                    "id": user[0],
                    "username": user[1],
                    "department": user[3],
                    "role": user[4]
                }
                st.success("登录成功！")
                st.rerun()
            else:
                st.error("❌ 用户名或密码错误")
    
    # 首次使用：创建初始用户
    if st.checkbox("首次使用？点击创建初始用户"):
        st.subheader("创建初始用户")
        with st.form("init_user"):
            init_user = st.text_input("初始用户名", value="admin")
            init_pass = st.text_input("初始密码", value="123456", type="password")
            dept = st.selectbox("部门", ["管理部", "现场部"])
            if st.form_submit_button("创建初始用户"):
                try:
                    create_user(init_user, init_pass, dept)
                    st.success(f"✅ 用户 {init_user} 创建成功！请返回登录。")
                except Exception as e:
                    st.error(f"创建失败：{e}")
def get_frog_allocation_records(name):
    """
    获取指定蛙苗名称的所有外购分配（出库）记录
    通过：frog_type_name → frog_type_id → pond.frog_type_id → stock_movement.to_pond_id
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 先获取 frog_type_id
        cur.execute("SELECT id FROM frog_type_shiwa WHERE name = %s;", (name,))
        frog_type_row = cur.fetchone()
        if not frog_type_row:
            return []
        frog_type_id = frog_type_row[0]

        # 查询所有分配到 frog_type_id 池塘的 purchase movement
        cur.execute("""
            SELECT 
                sm.moved_at,
                p.name AS pond_name,
                sm.quantity,
                sm.unit_price,
                (sm.quantity * COALESCE(sm.unit_price, 20.0)) AS total_cost,
                sm.created_by,
                sm.description
            FROM stock_movement_shiwa sm
            JOIN pond_shiwa p ON sm.to_pond_id = p.id
            WHERE sm.movement_type = 'purchase'
              AND p.frog_type_id = %s
            ORDER BY sm.moved_at DESC;
        """, (frog_type_id,))
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()
        conn.close()
def get_frog_records_by_name(name):
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, purchased_at, quantity, unit_price, total_amount,
                    supplier, supplier_phone, purchased_by, notes
                FROM frog_purchase_record_shiwa
                WHERE frog_type_name = %s
                ORDER BY purchased_at DESC;
            """, (name,))
            rows = cur.fetchall()
            cur.close(); conn.close()
            return rows
def get_frog_records_by_name(name):
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, purchased_at, quantity, unit_price, total_amount,
                    supplier, supplier_phone, purchased_by, notes
                FROM frog_purchase_record_shiwa
                WHERE frog_type_name = %s
                ORDER BY purchased_at DESC;
            """, (name,))
            rows = cur.fetchall()
            cur.close(); conn.close()
            return rows
# -----------------------------
# 主应用入口
# -----------------------------
def run():
    st.set_page_config(page_title="中益石蛙基地养殖系统", layout="wide")

    # ========== 🔐 初始化用户表 ==========
    init_user_table()

    # ========== 🔐 登录状态检查 ==========
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.user = None

    if not st.session_state.logged_in:
        show_login_page()
        return

    # ========== ✅ 登录后主界面 ==========
    st.title("🐸 中益石蛙基地养殖系统")
    st.markdown(f"欢迎，{st.session_state.user['username']}（{st.session_state.user['department']}）")
    if st.button("🚪 退出登录"):
        st.session_state.logged_in = False
        st.session_state.user = None
        st.rerun()
    # >>>>>>>>>>>>>>>>>> 在这里插入新函数定义 <<<<<<<<<<<<<<<<<<
    def get_frog_purchase_types_with_qty():
        """获取蛙型 + 数量（含 quantity 字段）"""
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, name, unit_price, COALESCE(quantity, 0) FROM frog_purchase_type_shiwa ORDER BY id;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def allocate_frog_purchase(frog_type_id, to_pond_id, quantity, description, created_by, moved_at=None):
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT unit_price, name FROM frog_purchase_type_shiwa WHERE id = %s;", (frog_type_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise ValueError("采购蛙苗不存在")
        unit_price = row[0] or 20.0
        purchase_name = row[1]  # 👈 获取采购名称
        # 修改 description，嵌入采购名称
        full_description = f"[{purchase_name}] {description}".strip()
        return add_stock_movement(
            movement_type='purchase',
            from_pond_id=None,
            to_pond_id=to_pond_id,
            quantity=quantity,
            description=full_description,  # 👈 带上采购 SKU 名称
            unit_price=unit_price,
            created_by=created_by,
            moved_at=moved_at,
            frog_type_id=frog_type_id
        )
    # >>>>>>>>>>>>>>>>>> 函数定义结束 <<<<<<<<<<<<<<<<<<
    st.markdown("---")
    # 创建三个 Tab（你原有的 7 个 Tab）
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
        ["📊 池塘总览", "🍽️ 喂养日志", "➕ 池塘创建", "🔄 孵转池与外购", 
         "🪱 采购类型", "💰 销售记录", "📈 投资回报（ROI）"]
    )


        # Tab 1: 池塘总览（表格 + 图表）
    with tab1:
                                # ================== ③ 新增：AI 问答子模块 ==================
        st.markdown("---")
        st.subheader("🤖 AI 养殖场问答")
        st.caption("例：「现在全场共有多少只蛙？」、「哪类池塘占用率最高？」")
        if "ai_chat_history" not in st.session_state:
            st.session_state.ai_chat_history = []

        # 显示历史
        for q, a in st.session_state.ai_chat_history:
            with st.chat_message("user"):
                st.write(q)
            with st.chat_message("assistant"):
                st.write(a)

        # 用户输入
        if q := st.chat_input("输入你的问题，按回车"):
            with st.chat_message("user"):
                st.write(q)
            with st.chat_message("assistant"):
                with st.spinner("AI 正在查询数据库..."):
                    try:
                        answer, sql, df = ai_ask_database(q)
                        st.write(answer)
                        with st.expander("🔍 技术详情（点击展开）"):
                            st.code(sql, language="sql")
                            st.dataframe(df.head(20), width='stretch')
                        st.session_state.ai_chat_history.append((q, answer))
                    except Exception as e:
                        st.error(f"查询失败：{e}")

        if st.button("🗑️ 清空对话"):
            st.session_state.ai_chat_history.clear()
            st.rerun()
        # =======================================================
        st.subheader("📊 所有池塘状态")
        ponds = get_all_ponds()
        
        if not ponds:
            st.warning("暂无池塘。请在「池塘创建」Tab 中添加，或点击「一键初始化示例数据」。")
        else:
            # 转为 DataFrame 便于展示和绘图
            df = pd.DataFrame(
                ponds,
                columns=["ID", "名称", "池类型", "蛙种", "最大容量", "当前数量"]
            )
            df["占用率 (%)"] = (df["当前数量"] / df["最大容量"] * 100).round(1)
            df["占用率 (%)"] = df["占用率 (%)"].clip(upper=100)  # 防止超容显示 >100

            # 可选：筛选器
            col1, col2 = st.columns(2)
            with col1:
                frog_filter = st.multiselect(
                    "按蛙种筛选",
                    options=df["蛙种"].unique(),
                    default=df["蛙种"].unique()
                )
            with col2:
                type_filter = st.multiselect(
                    "按池类型筛选",
                    options=df["池类型"].unique(),
                    default=df["池类型"].unique()
                )

            # 应用筛选
            filtered_df = df[
                (df["蛙种"].isin(frog_filter)) &
                (df["池类型"].isin(type_filter))
            ].copy()

            if filtered_df.empty:
                st.info("没有匹配的池塘。")
            else:
                # ---- 池塘总览分页 ----
                page_size = 20
                if "pond_overview_page" not in st.session_state:
                    st.session_state.pond_overview_page = 0

                total_rows = len(filtered_df)
                total_pages = (total_rows + page_size - 1) // page_size
                current_page = st.session_state.pond_overview_page
                current_page = max(0, min(current_page, total_pages - 1))  # 防越界

                col_prev, col_next, col_info = st.columns([1, 1, 3])
                with col_prev:
                    if st.button("⬅️ 上一页", disabled=(current_page == 0), key="pond_overview_prev"):
                        st.session_state.pond_overview_page -= 1
                        st.rerun()
                with col_next:
                    if st.button("下一页 ➡️", disabled=(current_page >= total_pages - 1), key="pond_overview_next"):
                        st.session_state.pond_overview_page += 1
                        st.rerun()
                with col_info:
                    st.caption(f"第 {current_page + 1} 页 / 共 {total_pages} 页（每页 {page_size} 条）")

                start_idx = current_page * page_size
                end_idx = start_idx + page_size
                page_df = filtered_df.iloc[start_idx:end_idx]

                st.dataframe(
                    page_df[["名称", "池类型", "蛙种", "当前数量", "最大容量", "占用率 (%)"]],
                    width='stretch',
                    hide_index=True
                )

                # === 图表展示 ===
                st.markdown("### 📈 池塘容量占用率")
                chart_data = filtered_df.set_index("名称")["占用率 (%)"]
                st.bar_chart(chart_data, height=400)


    # ===================== ① 标准库导入（放在文件顶部即可） =====================
    from collections import defaultdict
    from datetime import datetime, time
    # ============================================================================

    # ===================== ② Tab2  喂养记录（录入 + 总览） =====================
    with tab2:
        # ---- 0. 基础数据（只拉一次） ----
        all_ponds   = get_all_ponds()
        pond_types  = get_pond_types()
        feed_types  = get_feed_types()
        type_2_ponds = defaultdict(list)
        for p in all_ponds:
            type_2_ponds[p[2]].append({"id": p[0], "name": p[1], "current": p[5]})

        # ===================== 批量投喂（同类型多池均摊）=====================
        with st.expander("🍽️ 批量投喂（同类型多池均摊）", expanded=False):
            # ① 池塘类型选择（放在表单外，避免重载）
            if "feed_pt_sel" not in st.session_state:
                st.session_state.feed_pt_sel = pond_types[0][1]
            pt_sel = st.selectbox("1. 选择池塘类型",
                                options=[pt[1] for pt in pond_types],
                                key="feed_pt_sel")
            ponds_of_type = type_2_ponds.get(pt_sel, [])
            if not ponds_of_type:
                st.warning(f"暂无【{pt_sel}】类型的池塘")
            else:
                # ② 池子多选
                pond_id_to_label = {p["id"]: f"{p['name']}  （当前 {p['current']} 只）"
                                    for p in ponds_of_type}
                sel_pond_ids = st.multiselect(
                    "2. 选择要投喂的池子（已默认全选）",
                    options=list(pond_id_to_label.keys()),
                    format_func=lambda x: pond_id_to_label.get(x, f"未知池({x})"),
                    default=list(pond_id_to_label.keys())
                )
                # ③ 饲料多选
                feed_id_to_info = {f[0]: {"name": f[1], "price": f[2]} for f in feed_types}
                if not feed_id_to_info:
                    st.info("暂无饲料数据，请在「Tab5 · 采购类型」先添加饲料")
                    selected_feed_ids = []
                else:
                    selected_feed_ids = st.multiselect(
                        "3. 饲料类型（可多选）",
                        options=list(feed_id_to_info.keys()),
                        format_func=lambda x: feed_id_to_info[x]["name"],
                        default=[]
                    )
                # ④ 重量输入
                st.markdown("4. 为每种饲料输入**总投喂量 (kg)**（将均摊到所选池塘）")
                feed_total_weights = {}
                for fid in selected_feed_ids:
                    feed_name = feed_id_to_info[fid]["name"]
                    feed_total_weights[fid] = st.number_input(
                        f"总重量 - {feed_name}",
                        min_value=0.1,
                        step=0.1,
                        key=f"fw_out_{fid}"
                    )
                # ⑤ 日期 & 整点
                col1, col2 = st.columns(2)
                with col1:
                    feed_date = st.date_input("5. 投喂日期", value=datetime.today())
                with col2:
                    hour = st.selectbox("6. 投喂整点（0-23）", list(range(24)), format_func=lambda x: f"{x:02d}:00")
                # ⑦ 备注
                quick_remark = st.selectbox("7. 快捷备注", COMMON_REMARKS["喂养备注"])
                notes = st.text_area("8. 备注（可选）", value=quick_remark)
                # ⑧ 提交
                if st.button("✅ 提交批量投喂记录", type="primary"):
                    if not sel_pond_ids:
                        st.error("请至少选择一个池子！")
                        st.stop()
                    if not selected_feed_ids:
                        st.error("请至少选择一种饲料！")
                        st.stop()
                    feed_dt = datetime.combine(feed_date, time(hour, 0))
                    current_user = st.session_state.user['username']
                    try:
                        for fid in selected_feed_ids:
                            total_kg = feed_total_weights[fid]
                            if total_kg <= 0:
                                st.error(f"饲料「{feed_id_to_info[fid]['name']}」总重量必须 > 0")
                                st.stop()
                            per_kg = total_kg / len(sel_pond_ids)
                            unit_price = feed_id_to_info[fid]['price']
                            for pid in sel_pond_ids:
                                add_feeding_record(pid, fid, per_kg, float(unit_price), notes, feed_dt, fed_by=current_user)
                        st.success(f"✅ 已成功为 {len(sel_pond_ids)} 个【{pt_sel}】池子投喂 {len(selected_feed_ids)} 种饲料！")
                        st.rerun()
                    except ValueError as ve:
                        st.error(f"❌ 投喂失败：{ve}")
                    except Exception as e:
                        st.error(f"❌ 发生未知错误：{e}")

            # ---- 历史投喂总览（带分页）----
            # ---- 历史投喂总览（带分页）----
            st.markdown("### 📊 喂食总览（原始记录）")
            page_size = 20

            # 获取总记录数
            conn_count = get_db_connection()
            cur_count = conn_count.cursor()
            cur_count.execute("SELECT COUNT(*) FROM feeding_record_shiwa;")
            total_feedings = cur_count.fetchone()[0]
            cur_count.close()
            conn_count.close()

            total_pages = (total_feedings + page_size - 1) // page_size if total_feedings > 0 else 1

            if "feeding_page" not in st.session_state:
                st.session_state.feeding_page = 0

            # 校验 current_page 在 [0, total_pages)
            current_page = st.session_state.feeding_page
            current_page = max(0, min(current_page, total_pages - 1))
            st.session_state.feeding_page = current_page  # 确保状态合法
            with col_prev:
                if st.button("⬅️ 上一页", disabled=(current_page == 0), key="feeding_prev"):
                    st.session_state.feeding_page -= 1
                    st.rerun()
            with col_next:
                if st.button("下一页 ➡️", key="feeding_next"):
                    st.session_state.feeding_page += 1
                    st.rerun()
            with col_info:
                st.caption(f"第 {current_page + 1} 页（每页 {page_size} 条）")
            offset = current_page * page_size
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    fr.fed_at AT TIME ZONE 'UTC' AT TIME ZONE '+08' AS 投喂时间,
                    p.name AS 池塘名称,
                    ft.name AS 蛙种,
                    ftype.name AS 饲料类型,
                    fr.feed_weight_kg AS 投喂量_kg,
                    fr.unit_price_at_time AS 单价_元_kg,
                    fr.total_cost AS 成本_元,
                    fr.notes AS 备注,
                    fr.fed_by AS 喂食人
                FROM feeding_record_shiwa fr
                JOIN pond_shiwa p ON fr.pond_id = p.id
                JOIN frog_type_shiwa ft ON p.frog_type_id = ft.id
                JOIN feed_type_shiwa ftype ON fr.feed_type_id = ftype.id
                ORDER BY fr.fed_at DESC
                LIMIT %s OFFSET %s;
            """, (page_size, offset))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            if rows:
                df = pd.DataFrame(rows, columns=["投喂时间", "池塘名称", "蛙种", "饲料类型", "投喂量_kg", "单价_元_kg", "成本_元", "备注", "喂食人"])
                st.dataframe(df, width='stretch', hide_index=True)
                if len(rows) == page_size:
                    st.info("✅ 还有更多记录，请点击「下一页」查看")
                else:
                    st.success("已到最后一页")
            else:
                if current_page == 0:
                    st.info("暂无喂养记录")
                else:
                    st.warning("没有更多数据了")
                    st.session_state.feeding_page -= 1

            # ================= 月度投喂成本 =================
            st.markdown("---")
            st.subheader("📊 月度投喂总成本")
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT DATE_TRUNC('month', fr.fed_at) AS 月份,
                    SUM(fr.total_cost)            AS 月总成本
                FROM feeding_record_shiwa fr
                GROUP BY 月份
                ORDER BY 月份 DESC;
            """)
            month_rows = cur.fetchall()
            cur.close()
            conn.close()
            if not month_rows:
                st.info("暂无投喂记录")
            else:
                df_month = pd.DataFrame(month_rows,
                                        columns=["月份", "月总成本（元）"])
                df_month["月份"] = df_month["月份"].dt.strftime("%Y-%m")
                st.dataframe(df_month.style.format({"月总成本（元）": "¥{:,.2f}"}),
                            width='stretch', hide_index=True)
                st.bar_chart(df_month.set_index("月份")["月总成本（元）"])
                csv_month = df_month.to_csv(index=False)
                st.download_button("📥 导出月度成本 CSV",
                                data=csv_month,
                                file_name=f"monthly_feed_cost_{pd.Timestamp.now():%Y%m%d}.csv",
                                mime="text/csv")

        # ================ 养殖日志（每日记录） ================
        with st.expander("📝 每日养殖日志（水温 / pH / 光照 / 溶氧 / 湿度等）", expanded=False):
            # 池子联动：类型选在外部，保证切换时页面不卡
            if "log_pt_sel" not in st.session_state:
                st.session_state.log_pt_sel = pond_types[0][1]
            log_pt_sel = st.selectbox("① 池塘类型",
                                    options=[pt[1] for pt in pond_types],
                                    key="log_pt_sel")
            log_ponds_of_type = type_2_ponds.get(log_pt_sel, [])
            with st.form("daily_log_form"):
                if not log_ponds_of_type:
                    st.warning(f"暂无【{log_pt_sel}】类型的池塘")
                    st.form_submit_button("✅ 保存每日日志", disabled=True)
                else:
                    # ② 单选池子（同类型内选择）
                    log_pond_dict = {p["id"]: f"{p['name']}  （当前 {p['current']} 只）" for p in log_ponds_of_type}
                    pond_id = st.selectbox("② 具体池子",
                                        options=list(log_pond_dict.keys()),
                                        format_func=lambda x: log_pond_dict.get(x, f"未知池({x})"))
                    # ③ 日期
                    log_date = st.date_input("③ 日期", value=datetime.today())
                    # ④ 环境四件套：水温、 pH 、溶氧、湿度
                    col1, col2 = st.columns(2)
                    with col1:
                        water_temp = st.number_input("水温 (℃)", min_value=0.0, max_value=50.0, step=0.1, value=22.0)
                        ph_value = st.number_input("pH 值", min_value=0.0, max_value=14.0, step=0.1, value=7.0)
                    with col2:
                        do_value = st.number_input("溶氧量 (mg/L)", min_value=0.0, step=0.1, value=5.0)
                        humidity = st.number_input("湿度 (%)", min_value=0.0, max_value=100.0, step=1.0, value=70.0)
                    # ---- 天气选择（原光照）----
                    weather_opts = ["高温天气", "晴天", "阴天", "小雨", "大雨", "暴雨", "小雪", "大雪", "冰雹"]
                    weather = st.selectbox("当日天气", weather_opts, index=1)
                    # ---- 水源选择----
                    water_source = st.selectbox("水来源", ["山泉水", "地下水"])
                    # ⑥ 观察记录
                    quick_observe = st.selectbox("快捷观察", COMMON_REMARKS["每日观察"])
                    observation = st.text_area("观察记录（可记录卵块、行为、异常等）",
                                            value=quick_observe, height=120)
                    # ⑦ 提交
                    submitted = st.form_submit_button("✅ 保存每日日志", type="primary")
                    if submitted:
                        current_user = st.session_state.user['username']
                        add_daily_log(
                            pond_id     = pond_id,
                            log_date    = log_date,
                            water_temp  = water_temp,
                            ph_value    = ph_value,
                            weather     = weather,
                            observation = observation.strip(),
                            do_value    = do_value,
                            humidity    = humidity,
                            water_source= water_source,
                            recorded_by = current_user
                        )
                        st.success("✅ 每日日志已保存！")
                        st.rerun()

            # ---- 历史日志列表（带分页）----
            # ---- 历史日志列表（带分页）----
            st.markdown("### 📖 历史每日日志")
            page_size = 20

            # 获取总记录数（用于计算总页数）
            conn_count = get_db_connection()
            cur_count = conn_count.cursor()
            cur_count.execute("SELECT COUNT(*) FROM daily_log_shiwa;")
            total_logs = cur_count.fetchone()[0]
            cur_count.close()
            conn_count.close()

            total_pages = (total_logs + page_size - 1) // page_size if total_logs > 0 else 1

            if "daily_log_page" not in st.session_state:
                st.session_state.daily_log_page = 0

            # 校验 current_page 在 [0, total_pages)
            current_page = st.session_state.daily_log_page
            current_page = max(0, min(current_page, total_pages - 1))
            st.session_state.daily_log_page = current_page  # 确保状态合法
            with col_prev:
                if st.button("⬅️ 上一页", disabled=(current_page == 0), key="daily_log_prev"):
                    st.session_state.daily_log_page -= 1
                    st.rerun()
            with col_next:
                if st.button("下一页 ➡️", key="daily_log_next"):
                    st.session_state.daily_log_page += 1
                    st.rerun()
            with col_info:
                st.caption(f"第 {current_page + 1} 页（每页 {page_size} 条）")
            offset = current_page * page_size
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT dl.log_date,
                    p.name,
                    dl.water_temp,
                    dl.ph_value,
                    dl.do_value,
                    dl.humidity,
                    dl.weather,
                    dl.water_source,
                    dl.observation,
                    dl.recorded_by
                FROM daily_log_shiwa dl
                JOIN pond_shiwa p ON dl.pond_id = p.id
                ORDER BY dl.log_date DESC, dl.created_at DESC
                LIMIT %s OFFSET %s;
            """, (page_size, offset))
            rows = cur.fetchall()
            if rows:
                df_log = pd.DataFrame(rows,
                                    columns=["日期", "池塘", "水温(℃)", "pH", "溶氧(mg/L)", "湿度(%)",
                                            "天气", "水来源", "观察记录", "记录人"])
                st.dataframe(df_log, width='stretch', hide_index=True)
                if len(rows) == page_size:
                    st.info("✅ 还有更多记录，请点击「下一页」查看")
                else:
                    st.success("已到最后一页")
            else:
                if current_page == 0:
                    st.info("暂无每日日志记录")
                else:
                    st.warning("没有更多数据了")
                    st.session_state.daily_log_page -= 1

        with tab3:
                    # ========== 创建新池塘（放入 expander）==========
            with st.expander("➕ 创建新池塘", expanded=False):  # 默认展开，方便操作
                pond_types = get_pond_types()
                frog_types = get_frog_types()
                with st.form("pond_create_form"):
                    # ① 让用户输入编号
                    pond_code = st.text_input(
                        "池塘编号",
                        placeholder="例如：001 或 A-101"
                    )
                    col1, col2 = st.columns(2)
                    with col1:
                        pond_type_id = st.selectbox(
                            "池塘类型",
                            options=[pt[0] for pt in pond_types],
                            format_func=lambda x: next(pt[1] for pt in pond_types if pt[0] == x)
                        )
                    with col2:
                        frog_type_id = st.selectbox(
                            "蛙种类型",
                            options=[ft[0] for ft in frog_types],
                            format_func=lambda x: next(ft[1] for ft in frog_types if ft[0] == x)
                        )
                    max_cap = st.number_input(
                        "最大容量（只）", min_value=1, value=5000, step=10
                    )
                    initial = st.number_input(
                        "初始数量（只）", min_value=0, value=0, step=1, max_value=max_cap
                    )
                    submitted = st.form_submit_button("✅ 创建池塘")
                    if submitted:
                        code = pond_code.strip()
                        if not code:
                            st.error("请输入池塘编号！")
                            st.stop()
                        # 拼接名称：池类型 + 编号 + 蛙种（按新规则）
                        frog_name = next(ft[1] for ft in frog_types if ft[0] == frog_type_id)
                        type_name = next(pt[1] for pt in pond_types if pt[0] == pond_type_id)
                        final_name = f"{type_name}{code}{frog_name}"  # ← 修改顺序
                        try:
                            create_pond(final_name, pond_type_id, frog_type_id,
                                    int(max_cap), int(initial))
                            st.success(f"✅ 池塘「{final_name}」创建成功！容量：{max_cap}，初始：{initial}")
                            st.rerun()
                        except Exception as e:
                            if "unique_pond_name" in str(e) or "已存在" in str(e):
                                st.error(f"❌ 创建失败：拼接后的池塘名称「{final_name}」已存在，请更换编号！")
                            else:
                                st.error(f"❌ 创建失败：{e}")

            with st.expander("🔍 查看已建池塘", expanded=False):
                # ========== 已创建的池塘 ==========
                st.markdown("### 📋 已创建的池塘")
                ponds_now = get_all_ponds()
                if not ponds_now:
                    st.info("暂无池塘，快去创建第一个吧！")
                else:
                    df = pd.DataFrame(
                        ponds_now,
                        columns=["ID", "名称", "池类型", "蛙种", "最大容量", "当前数量"]
                    )
                    df = df.iloc[::-1].reset_index(drop=True)
                    st.dataframe(df, width='stretch', hide_index=True)

                st.markdown("---")

                # ========== 变更池塘用途（仅当数量为 0）==========
                st.markdown("### 🔄 变更池塘用途（仅当数量为 0 时可用）")
                st.caption("适用于：已完成养殖周期的空池，重新赋予新用途")
                empty_ponds = [p for p in get_all_ponds() if p[5] == 0]
                if not empty_ponds:
                    st.info("暂无空池，无法变更用途")
                else:
                    pond_types = get_pond_types()
                    frog_types = get_frog_types()
                    pond_type_map = {pt[0]: pt[1] for pt in pond_types}
                    frog_type_map = {ft[0]: ft[1] for ft in frog_types}

                    with st.form(key="change_purpose_form_unique"):
                        ep_dict = {ep[0]: f"{ep[1]}  （{ep[2]}｜{ep[3]}）" for ep in empty_ponds}
                        pond_id = st.selectbox("选择空池", options=list(ep_dict.keys()),
                                            format_func=lambda x: ep_dict[x])
                        current_pond = next(p for p in empty_ponds if p[0] == pond_id)

                        col1, col2 = st.columns(2)
                        with col1:
                            new_pt_id = st.selectbox(
                                "新池塘类型",
                                options=list(pond_type_map.keys()),
                                format_func=lambda x: pond_type_map.get(x, f"未知类型({x})")
                            )
                        with col2:
                            new_ft_id = st.selectbox(
                                "新蛙种类型",
                                options=list(frog_type_map.keys()),
                                format_func=lambda x: frog_type_map.get(x, f"未知蛙种({x})")
                            )
                        new_code = st.text_input("新编号", placeholder="如 002 或 B-202")
                        submitted = st.form_submit_button("✅ 确认变更", type="secondary")
                        if submitted:
                            if not new_code.strip():
                                st.error("请输入新编号！")
                                st.stop()
                            new_name = f"{pond_type_map[new_pt_id]}{new_code.strip()}{frog_type_map[new_ft_id]}"
                            ok, msg = update_pond_identity(pond_id, new_name, new_pt_id, new_ft_id)
                            if ok:
                                # ===== 记录日志 =====
                                old_vals = {
                                    "name": current_pond[1],
                                    "pond_type_id": next(pt[0] for pt in pond_types if pt[1] == current_pond[2]),
                                    "frog_type_id": next(ft[0] for ft in frog_types if ft[1] == current_pond[3]),
                                    "max_capacity": current_pond[4],
                                    "current_count": current_pond[5]
                                }
                                new_vals = {
                                    "name": new_name,
                                    "pond_type_id": new_pt_id,
                                    "frog_type_id": new_ft_id,
                                    "max_capacity": current_pond[4],
                                    "current_count": current_pond[5]
                                }
                                current_user = st.session_state.user["username"]
                                log_pond_change(
                                    pond_id=pond_id,
                                    change_type="变更用途",
                                    old_values=old_vals,
                                    new_values=new_vals,
                                    change_date=datetime.today().date(),
                                    notes="",
                                    changed_by=current_user
                                )
                                st.success(f"✅ 池塘已变更为「{new_name}」！")
                                st.rerun()
                            else:
                                st.error(f"❌ 变更失败：{msg}")

                st.markdown("---")

                # ========== 修正创建错误（仅限从未使用过的池塘）==========
                st.markdown("### ✏️ 修正创建错误（仅限从未使用过的池塘）")
                st.caption("适用于：刚创建但未进行任何操作的池塘，可修改全部字段")
                all_ponds = get_all_ponds()
                unused_ponds = [p for p in all_ponds if is_pond_unused(p[0])]
                if not unused_ponds:
                    st.info("暂无符合条件的池塘（需从未参与任何操作）")
                else:
                    pond_types = get_pond_types()
                    frog_types = get_frog_types()
                    pond_type_map = {pt[0]: pt[1] for pt in pond_types}
                    frog_type_map = {ft[0]: ft[1] for ft in frog_types}

                    with st.form(key="correct_creation_form_unique"):
                        up_dict = {up[0]: f"{up[1]}  （{up[2]}｜{up[3]}｜当前{up[5]}只）" for up in unused_ponds}
                        pond_id = st.selectbox("选择池塘", options=list(up_dict.keys()),
                                            format_func=lambda x: up_dict[x])
                        current_pond = next(p for p in unused_ponds if p[0] == pond_id)

                        col1, col2 = st.columns(2)
                        with col1:
                            new_pt_id = st.selectbox(
                                "新池塘类型",
                                options=list(pond_type_map.keys()),
                                format_func=lambda x: pond_type_map.get(x, f"未知类型({x})")
                            )
                        with col2:
                            new_ft_id = st.selectbox(
                                "新蛙种类型",
                                options=list(frog_type_map.keys()),
                                format_func=lambda x: frog_type_map.get(x, f"未知蛙种({x})")
                            )
                        new_code = st.text_input("新编号", placeholder="如 002 或 B-202")
                        new_max_cap = st.number_input("最大容量（只）", min_value=1, value=current_pond[4], step=10)
                        # ✅ 关键：不限制 max_value，允许自由输入
                        new_current_count = st.number_input(
                            "当前数量（只）",
                            min_value=0,
                            value=current_pond[5],
                            step=1
                        )

                        submitted = st.form_submit_button("✅ 修正创建信息", type="secondary")
                        if submitted:
                            if not new_code.strip():
                                st.error("请输入新编号！")
                                st.stop()
                            if new_current_count > new_max_cap:
                                st.error(f"❌ 当前数量（{new_current_count}）不能超过最大容量（{new_max_cap}）！")
                                st.stop()
                            new_name = f"{pond_type_map[new_pt_id]}{new_code.strip()}{frog_type_map[new_ft_id]}"
                            ok, msg = update_pond_full(
                                pond_id=pond_id,
                                new_name=new_name,
                                new_pond_type_id=new_pt_id,
                                new_frog_type_id=new_ft_id,
                                new_max_capacity=new_max_cap,
                                new_current_count=new_current_count
                            )
                            if ok:
                                # ===== 记录日志 =====
                                old_vals = {
                                    "name": current_pond[1],
                                    "pond_type_id": next(pt[0] for pt in pond_types if pt[1] == current_pond[2]),
                                    "frog_type_id": next(ft[0] for ft in frog_types if ft[1] == current_pond[3]),
                                    "max_capacity": current_pond[4],
                                    "current_count": current_pond[5]
                                }
                                new_vals = {
                                    "name": new_name,
                                    "pond_type_id": new_pt_id,
                                    "frog_type_id": new_ft_id,
                                    "max_capacity": new_max_cap,
                                    "current_count": new_current_count
                                }
                                current_user = st.session_state.user["username"]
                                log_pond_change(
                                    pond_id=pond_id,
                                    change_type="修正创建",
                                    old_values=old_vals,
                                    new_values=new_vals,
                                    change_date=datetime.today().date(),
                                    notes="",
                                    changed_by=current_user
                                )
                                st.success(f"✅ 池塘已修正为「{new_name}」！容量：{new_max_cap}，数量：{new_current_count}")
                                st.rerun()
                            else:
                                st.error(f"❌ 修正失败：{msg}")

                st.markdown("---")

                # ========== 池塘变更历史（备查）==========
                with st.expander("📜 池塘变更历史（备查）", expanded=False):
                    try:
                        conn = get_db_connection()
                        df_log = pd.read_sql("""
                            SELECT 
                                p.name AS 池塘,
                                change_type AS 类型,
                                change_date AS 业务日期,
                                old_name AS 原名称,
                                new_name AS 新名称,
                                pt_old.name AS 原池型,
                                pt_new.name AS 新池型,
                                ft_old.name AS 原蛙种,
                                ft_new.name AS 新蛙种,
                                old_max_capacity AS 原最大容量,
                                new_max_capacity AS 新最大容量,
                                old_current_count AS 原数量,
                                new_current_count AS 新数量,
                                notes AS 备注,
                                changed_by AS 操作人,
                                changed_at AS 系统时间
                            FROM pond_change_log l
                            JOIN pond_shiwa p ON l.pond_id = p.id
                            LEFT JOIN pond_type_shiwa pt_old ON l.old_pond_type_id = pt_old.id
                            LEFT JOIN pond_type_shiwa pt_new ON l.new_pond_type_id = pt_new.id
                            LEFT JOIN frog_type_shiwa ft_old ON l.old_frog_type_id = ft_old.id
                            LEFT JOIN frog_type_shiwa ft_new ON l.new_frog_type_id = ft_new.id
                            ORDER BY l.changed_at DESC;
                        """, conn)
                        conn.close()

                        if not df_log.empty:
                            st.dataframe(df_log, width='stretch', hide_index=True)
                            csv_data = df_log.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="📥 导出变更日志 CSV",
                                data=csv_data,
                                file_name="pond_change_log.csv",
                                mime="text/csv"
                            )
                        else:
                            st.info("暂无变更记录")
                    except Exception as e:
                        st.error(f"⚠️ 加载变更日志失败：{e}")
    # ----------------------------- Tab 4: 转池 · 外购 · 孵化 -----------------------------
    with tab4:

        with st.expander("🔄 转池 / 外购 / 孵化 / 死亡操作", expanded=False):
            operation = st.radio("操作类型", ["转池", "外购", "孵化", "死亡"],
                                horizontal=True, key="tab4_op_radio")
            ponds = get_all_ponds()
            if not ponds:
                st.warning("请先创建至少一个池塘！")
                st.stop()
            pond_id_to_info = {p[0]: {"name": p[1], "pond_type": p[2].strip(),
                                    "frog_type": p[3], "max_capacity": p[4], "current_count": p[5]}
                            for p in ponds}
            grouped = group_ponds_by_type(pond_id_to_info)

            # ========== 死亡 ==========
            if operation == "死亡":
                src_grouped = grouped
                if not src_grouped:
                    st.error("❌ 无可用的转出池类型")
                else:
                    from_pond_id = pond_selector("源池塘（死亡出库）", pond_id_to_info, src_grouped, "death_src")
                    current = pond_id_to_info[from_pond_id]["current_count"]
                    if current == 0:
                        st.error("该池当前数量为 0，无法记录死亡！")
                    else:
                        with st.form("death_record_form", clear_on_submit=True):
                            # ===== 新增：操作时间 =====
                            moved_at_date = st.date_input("操作日期", value=datetime.today(), key="death_date")
                            moved_at_time = st.time_input("操作时间", value=datetime.now().time(), key="death_time")
                            moved_at = datetime.combine(moved_at_date, moved_at_time)
                            
                            quantity = st.number_input("死亡数量", min_value=1, max_value=current, step=1,
                                                    key="death_qty")
                            note = st.text_area("备注（选填）", placeholder="如：病害、天气、人为等",
                                            key="death_note")
                            uploaded_files = st.file_uploader(
                                "上传死亡现场照片（可一次选多张）",
                                type=["png", "jpg", "jpeg"],
                                accept_multiple_files=True,
                                key="death_images"
                            )
                            submitted = st.form_submit_button("✅ 记录死亡", type="primary")
                            if submitted:
                                current_user = st.session_state.user['username']
                                ok, msg = add_death_record(
                                    from_pond_id, 
                                    quantity, 
                                    note,
                                    uploaded_files,
                                    created_by=current_user,
                                    moved_at=moved_at  # 👈 传入时间
                                )
                                if ok:
                                    st.success(f"✅ 死亡记录成功：{quantity} 只")
                                    st.rerun()
                                else:
                                    st.error(f"❌ 记录失败：{msg}")

            # ========== 转池 / 外购 / 孵化 ==========
            else:
                from_pond_id = None
                to_pond_id   = None
                purchase_price = None
                default_qty = 1000
                if operation == "外购":
                    st.markdown("#### 从采购库存分配蛙苗到池塘")
                    frog_types_with_qty = get_frog_purchase_types_with_qty()
                    available_frogs = [f for f in frog_types_with_qty if f[3] > 0]
                    if not available_frogs:
                        st.info("暂无可分配的蛙苗库存。请先在「采购类型」Tab 中添加蛙型并设置数量。")
                    else:
                        frog_options = {f[0]: f"{f[1]}（单价 ¥{f[2]}/只，库存 {f[3]} 只）" for f in available_frogs}
                        frog_id = st.selectbox("选择蛙型", options=list(frog_options.keys()),
                                            format_func=lambda x: frog_options[x], key="allocate_frog_type")
                        selected_frog = next(f for f in available_frogs if f[0] == frog_id)
                        max_qty = selected_frog[3]
                        to_pond_id = pond_selector("目标池塘", pond_id_to_info, grouped, "allocate_pond")

                        # 👇 新增：获取目标池塘的容量和当前数量
                        target_pond_info = pond_id_to_info[to_pond_id]
                        max_cap = target_pond_info["max_capacity"]
                        current_count = target_pond_info["current_count"]
                        remaining_capacity = max_cap - current_count

                        if remaining_capacity <= 0:
                            st.error(f"❌ 目标池塘「{target_pond_info['name']}」已满（容量 {max_cap}，当前 {current_count}），无法分配！")
                            st.stop()

                        # 分配数量上限 = min(采购库存, 池塘剩余容量)
                        alloc_max = min(max_qty, remaining_capacity)

                        if alloc_max <= 0:
                            st.error("❌ 目标池塘无剩余容量，或采购库存不足，无法分配。")
                        else:
                            pick_qty = st.number_input(
                                "分配数量",
                                min_value=1,
                                max_value=alloc_max,
                                value=min(50, alloc_max),  # 默认值不超过上限
                                step=50,
                                key="allocate_qty"
                            )
                            pick_note = st.text_input("备注", value="外购入库分配", key="allocate_note")
                            # ===== 新增：操作时间 =====
                            moved_at_date = st.date_input("操作日期", value=datetime.today(), key="purchase_date_op")
                            moved_at_time = st.time_input("操作时间", value=datetime.now().time(), key="purchase_time_op")
                            moved_at = datetime.combine(moved_at_date, moved_at_time)

                            if st.button("✅ 执行分配", type="primary", key="allocate_submit_op"):
                                current_user = st.session_state.user['username']
                                success, msg = allocate_frog_purchase(
                                    frog_id, to_pond_id, pick_qty, pick_note, current_user, moved_at=moved_at
                                )
                                if success:
                                    st.success(f"✅ 分配成功：{pick_qty} 只 {selected_frog[1]} 已入池")
                                    st.rerun()
                                else:
                                    st.error(f"❌ 分配失败：{msg}")
                        
                # ---------------- 孵化 ----------------
                elif operation == "孵化":
                    hatch_grouped = {k: v for k, v in grouped.items() if k == "孵化池"}
                    if not hatch_grouped:
                        st.error("❌ 请先至少创建一个‘孵化池’")
                    else:
                        to_pond_id = pond_selector("孵化池", pond_id_to_info, hatch_grouped, "hatch")
                        target_frog_type_id = pond_id_to_info[to_pond_id]["frog_type"]
                        breeding_ponds = [
                            (pid, info["name"])
                            for pid, info in pond_id_to_info.items()
                            if info["pond_type"] == "种蛙池"
                            and info["frog_type"] == target_frog_type_id
                            and info["current_count"] > 0
                        ]
                        source_breeding_ids = []
                        if breeding_ponds:
                            st.markdown("#### 🐸 选择亲本来源（种蛙池，可多选）")
                            source_breeding_ids = st.multiselect(
                                "来源种蛙池",
                                options=[p[0] for p in breeding_ponds],
                                format_func=lambda x: next(p[1] for p in breeding_ponds if p[0] == x),
                                key="hatch_source_ponds"
                            )
                            if not source_breeding_ids:
                                st.info("未选择来源种蛙池（可选）")
                        else:
                            st.info(f"暂无可用的【{pond_id_to_info[to_pond_id]['frog_type']}】种蛙池...")

                        plate_input = st.text_input(
                            "🥚 按板输入（1板 = 500只，如：1、1/2、2/3）",
                            placeholder="留空则手动输入数量",
                            key="hatch_plate"
                        )
                        if plate_input.strip():
                            try:
                                if '/' in plate_input:
                                    num, den = plate_input.split('/')
                                    plate_val = float(num) / float(den)
                                else:
                                    plate_val = float(plate_input)
                                default_qty = max(1, int(round(plate_val * 500)))
                            except Exception:
                                st.warning(f"板数格式无效：{plate_input}，已改用默认值 1000")
                                default_qty = 1000
                        quantity = st.number_input("数量", min_value=1, value=default_qty, step=50,
                                                key="hatch_qty")
                        quick_desc = st.selectbox("快捷描述", COMMON_REMARKS["操作描述"],
                                                key="hatch_desc")
                        base_description = st.text_input("操作描述", value=quick_desc or "自孵蝌蚪",
                                                key="hatch_note")
                        full_description = base_description
                        if source_breeding_ids:
                            pond_names = [next(p[1] for p in breeding_ponds if p[0] == pid) for pid in source_breeding_ids]
                            full_description += f" | 来源种蛙池: {', '.join(pond_names)}"
                        
                        # ===== 新增：操作时间 =====
                        moved_at_date = st.date_input("操作日期", value=datetime.today(), key="hatch_date")
                        moved_at_time = st.time_input("操作时间", value=datetime.now().time(), key="hatch_time")
                        moved_at = datetime.combine(moved_at_date, moved_at_time)
                        
                        if st.button("✅ 执行孵化", type="primary", key="hatch_submit"):
                            current_user = st.session_state.user['username']
                            success, hint = add_stock_movement(
                                movement_type='hatch',
                                from_pond_id=None,
                                to_pond_id=to_pond_id,
                                quantity=quantity,
                                description=full_description,
                                unit_price=None,
                                created_by=current_user,
                                moved_at=moved_at  # 👈 传入时间
                            )
                            if success:
                                st.success(f"✅ 孵化成功：{quantity} 只")
                                st.rerun()
                            else:
                                st.error(hint)

                # ---------------- 转池 ----------------
                else:  # 转池
                    src_grouped = {k: v for k, v in grouped.items() if k in TRANSFER_PATH_RULES}
                    if not src_grouped:
                        st.error("❌ 无可用的转出池类型")
                    else:
                        from_pond_id = pond_selector("源池塘（转出）", pond_id_to_info, src_grouped, "transfer_src")
                        live_info = pond_id_to_info[from_pond_id]
                        allowed = TRANSFER_PATH_RULES.get(live_info["pond_type"], [])
                        tgt_grouped = {k: v for k, v in grouped.items() if k in allowed and v}
                        if not tgt_grouped:
                            st.error("❌ 无合法目标池")
                        else:
                            to_pond_id = pond_selector("目标池塘（转入）", pond_id_to_info, tgt_grouped, "transfer_tgt")
                            quantity = st.number_input("数量", min_value=1, value=500, step=50,
                                                    key="transfer_qty")
                            quick_desc = st.selectbox("快捷描述", COMMON_REMARKS["操作描述"],
                                                    key="transfer_desc")
                            description = st.text_input("操作描述", value=quick_desc or "日常转池",
                                                    key="transfer_note")
                            
                            # ===== 新增：操作时间 =====
                            moved_at_date = st.date_input("操作日期", value=datetime.today(), key="transfer_date")
                            moved_at_time = st.time_input("操作时间", value=datetime.now().time(), key="transfer_time")
                            moved_at = datetime.combine(moved_at_date, moved_at_time)
                            
                            if st.button("✅ 执行转池", type="primary", key="transfer_submit"):
                                current_user = st.session_state.user['username']
                                from_frog_type = pond_id_to_info[from_pond_id]["frog_type"]
                                to_frog_type = pond_id_to_info[to_pond_id]["frog_type"]
                                if from_frog_type != to_frog_type:
                                    st.error(f"❌ 转池失败：源池蛙种「{from_frog_type}」与目标池蛙种「{to_frog_type}」不一致，禁止混养！")
                                else:
                                    to_pond = get_pond_by_id(to_pond_id)
                                    if to_pond[4] + quantity > to_pond[3]:
                                        st.error("❌ 目标池容量不足！")
                                    else:
                                        from_pond = get_pond_by_id(from_pond_id)
                                        if from_pond[4] < quantity:
                                            st.error("❌ 源池数量不足！")
                                        else:
                                            success, hint = add_stock_movement(
                                                movement_type='transfer',
                                                from_pond_id=from_pond_id,
                                                to_pond_id=to_pond_id,
                                                quantity=quantity,
                                                description=description,
                                                unit_price=None,
                                                created_by=current_user,
                                                moved_at=moved_at  # 👈 传入时间
                                            )
                                            if success:
                                                st.success("✅ 转池成功")
                                                st.rerun()
                                            else:
                                                st.error(hint)

                # ========== 合并：查看详细记录 expander ==========
        with st.expander("🔍 查看详细操作记录", expanded=False):
            # ========== 最近库存变动记录（分页）==========
            st.markdown("#### 📋 最近库存变动记录（转池 / 外购 / 孵化 / 死亡 / 销售）")
            page_size = 20
            if "movement_page" not in st.session_state:
                st.session_state.movement_page = 0
            col_prev, col_next, col_info = st.columns([1, 1, 3])
            current_page = max(0, st.session_state.movement_page)
            with col_prev:
                if st.button("⬅️ 上一页", disabled=(current_page == 0), key="movement_prev"):
                    st.session_state.movement_page -= 1
                    st.rerun()
            with col_next:
                if st.button("下一页 ➡️", key="movement_next"):
                    st.session_state.movement_page += 1
                    st.rerun()
            with col_info:
                st.caption(f"第 {current_page + 1} 页（每页 {page_size} 条）")
            offset = current_page * page_size
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT sm.id,
                    CASE sm.movement_type
                        WHEN 'transfer' THEN '转池'
                        WHEN 'purchase' THEN '外购'
                        WHEN 'hatch'    THEN '孵化'
                        WHEN 'sale'     THEN '销售出库'
                        WHEN 'death'    THEN '死亡'
                    END AS movement_type,
                    fp.name   AS from_name,
                    tp.name   AS to_name,
                    sm.quantity,
                    sm.description,
                    sm.moved_at,
                    sm.created_by AS 操作人
                FROM stock_movement_shiwa sm
                LEFT JOIN pond_shiwa fp ON sm.from_pond_id = fp.id
                LEFT JOIN pond_shiwa tp ON sm.to_pond_id = tp.id
                ORDER BY sm.moved_at DESC
                LIMIT %s OFFSET %s;
            """, (page_size, offset))
            rows = cur.fetchall()
            cur.close(); conn.close()
            if rows:
                df_log = pd.DataFrame(rows, columns=["ID", "类型", "源池", "目标池", "数量", "描述", "时间", "操作人"])
                st.dataframe(df_log, width='stretch', hide_index=True)
                csv = df_log.to_csv(index=False)
                st.download_button(label="📥 导出当前页 CSV", data=csv,
                                file_name=f"movement_page_{current_page + 1}_{pd.Timestamp.now():%Y%m%d_%H%M%S}.csv",
                                mime="text/csv")
                if len(rows) == page_size:
                    st.info("✅ 还有更多记录，请点击「下一页」查看")
                else:
                    st.success("已到最后一页")
            else:
                if current_page == 0:
                    st.info("暂无操作记录")
                else:
                    st.warning("没有更多数据了")
                    st.session_state.movement_page -= 1

            st.markdown("---")

            # ========== 最近死亡记录（独立区块）==========
            st.markdown("#### 💀 最近死亡记录")
            page_size_death = 20
            if "death_page" not in st.session_state:
                st.session_state.death_page = 0
            col_prev_d, col_next_d, col_info_d = st.columns([1, 1, 3])
            current_page_d = max(0, st.session_state.death_page)
            with col_prev_d:
                if st.button("⬅️ 上一页", disabled=(current_page_d == 0), key="death_prev"):
                    st.session_state.death_page -= 1
                    st.rerun()
            with col_next_d:
                if st.button("下一页 ➡️", key="death_next"):
                    st.session_state.death_page += 1
                    st.rerun()
            with col_info_d:
                st.caption(f"第 {current_page_d + 1} 页（每页 {page_size_death} 条）")
            offset_d = current_page_d * page_size_death
            death_records = get_recent_death_records(limit=page_size_death, offset=offset_d)
            if death_records:
                for record in death_records:
                    mid, pond, qty, desc, moved_at, operator, img_paths = record
                    with st.expander(f"🪦 {pond} · {qty} 只 · {moved_at:%Y-%m-%d %H:%M} · 操作人：{operator}"):
                        st.write(f"**描述**：{desc}")
                        if img_paths:
                            st.markdown("**现场照片：**")
                            cols_per_row = 3
                            for i in range(0, len(img_paths), cols_per_row):
                                cols = st.columns(cols_per_row)
                                for j, img_path in enumerate(img_paths[i:i+cols_per_row]):
                                    if os.path.exists(img_path):
                                        with cols[j]:
                                            st.image(img_path, caption=f"照片 {i+j+1}", width='stretch')
                                    else:
                                        with cols[j]:
                                            st.caption(f"照片 {i+j+1} 不存在")
                        else:
                            st.caption("🖼️ 无照片")
                if len(death_records) == page_size_death:
                    st.info("✅ 还有更多死亡记录，请点击「下一页」查看")
                else:
                    st.success("已到最后一页")
            else:
                if current_page_d == 0:
                    st.info("暂无死亡记录")
                else:
                    st.warning("没有更多数据了")
                    st.session_state.death_page -= 1
                    
    with tab5:
        current_user = st.session_state.user["username"]

        # ==================== 辅助函数（更新版） ====================
        def get_feed_stock_summary():
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT name, COALESCE(stock_kg, 0) AS total_stock
                FROM feed_type_shiwa
                ORDER BY name;
            """)
            rows = cur.fetchall()
            cur.close(); conn.close()
            return rows

        def get_frog_stock_summary():
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT name, COALESCE(quantity, 0) AS total_stock
                FROM frog_purchase_type_shiwa
                ORDER BY name;
            """)
            rows = cur.fetchall()
            cur.close(); conn.close()
            return rows

        def get_feed_records_by_name(name):
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, purchased_at, quantity_kg, unit_price, total_amount,
                    supplier, supplier_phone, purchased_by, notes
                FROM feed_purchase_record_shiwa
                WHERE feed_type_name = %s
                ORDER BY purchased_at DESC;
            """, (name,))
            rows = cur.fetchall()
            cur.close(); conn.close()
            return rows

        def get_feed_consumption_records(name):
            """获取该饲料的所有投喂（消耗）记录"""
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                    fr.fed_at,
                    p.name AS pond_name,
                    fr.feed_weight_kg,
                    fr.unit_price_at_time,
                    fr.total_cost,
                    fr.fed_by
                FROM feeding_record_shiwa fr
                JOIN feed_type_shiwa ft ON fr.feed_type_id = ft.id
                JOIN pond_shiwa p ON fr.pond_id = p.id
                WHERE ft.name = %s
                ORDER BY fr.fed_at DESC;
            """, (name,))
            rows = cur.fetchall()
            cur.close(); conn.close()
            return rows

        def add_feed_purchase(name, price, qty, supplier, phone, by, purchased_at, notes=""):
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute("""
                    INSERT INTO feed_type_shiwa
                    (name, unit_price, stock_kg, supplier, supplier_phone, purchased_by)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE
                    SET 
                        unit_price = EXCLUDED.unit_price,
                        stock_kg = feed_type_shiwa.stock_kg + EXCLUDED.stock_kg,
                        supplier = EXCLUDED.supplier,
                        supplier_phone = EXCLUDED.supplier_phone,
                        purchased_by = EXCLUDED.purchased_by;
                """, (name, price, qty, supplier, phone, by))
                cur.execute("""
                    INSERT INTO feed_purchase_record_shiwa 
                    (feed_type_name, quantity_kg, unit_price, total_amount, supplier, supplier_phone, purchased_by, purchased_at, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (name, qty, price, qty * price, supplier, phone, by, purchased_at, notes or ""))
                conn.commit()
            finally:
                cur.close(); conn.close()

        def add_frog_purchase(name, price, qty, supplier, phone, by, purchased_at, notes=""):
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute("""
                    INSERT INTO frog_purchase_type_shiwa
                    (name, unit_price, quantity, supplier, supplier_phone, purchased_by)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE
                    SET 
                        unit_price = EXCLUDED.unit_price,
                        quantity = frog_purchase_type_shiwa.quantity + EXCLUDED.quantity,
                        supplier = EXCLUDED.supplier,
                        supplier_phone = EXCLUDED.supplier_phone,
                        purchased_by = EXCLUDED.purchased_by;
                """, (name, price, qty, supplier, phone, by))
                cur.execute("""
                    INSERT INTO frog_purchase_record_shiwa 
                    (frog_type_name, quantity, unit_price, total_amount, supplier, supplier_phone, purchased_by, purchased_at, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (name, qty, price, qty * price, supplier, phone, by, purchased_at, notes or ""))
                conn.commit()
            finally:
                cur.close(); conn.close()

        # ==================== 1. 查看库存变动（放入 expander） ====================
        with st.expander("📄 查看库存变动", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 🌾 饲料库存")
                feed_summary = get_feed_stock_summary()
                if feed_summary:
                    df = pd.DataFrame(feed_summary, columns=["名称", "库存(kg)"])
                    st.dataframe(df, width='stretch', hide_index=True)
                    for name, _ in feed_summary:
                        if st.button(f"🔍 查看「{name}」流水", key=f"feed_detail_{name}"):
                            st.session_state.viewing_feed = name
                else:
                    st.info("暂无饲料库存")
            with col2:
                st.markdown("#### 🐸 蛙苗库存")
                frog_summary = get_frog_stock_summary()
                if frog_summary:
                    df = pd.DataFrame(frog_summary, columns=["名称", "库存(只)"])
                    st.dataframe(df, width='stretch', hide_index=True)
                    for name, _ in frog_summary:
                        if st.button(f"🔍 查看「{name}」流水", key=f"frog_detail_{name}"):
                            st.session_state.viewing_frog = name
                else:
                    st.info("暂无蛙苗库存")

            # ==================== 饲料详情（仅查看，无编辑） ====================
            if "viewing_feed" in st.session_state:
                name = st.session_state.viewing_feed
                st.markdown(f"### 📄 饲料「{name}」完整流水（采购 + 投喂）")
                purchase_records = get_feed_records_by_name(name)
                consumption_records = get_feed_consumption_records(name)
                all_records = []
                for r in purchase_records:
                    all_records.append({
                        "type": "入库",
                        "time": r[1],
                        "pond": "—",
                        "quantity": r[2],
                        "unit_price": r[3],
                        "total": r[4],
                        "operator": r[7] or "系统",
                        "notes": r[8] or "采购入库"
                    })
                for r in consumption_records:
                    all_records.append({
                        "type": "出库",
                        "time": r[0],
                        "pond": r[1],
                        "quantity": r[2],
                        "unit_price": r[3],
                        "total": r[4],
                        "operator": r[5],
                        "notes": "投喂消耗"
                    })
                all_records.sort(key=lambda x: x["time"], reverse=True)
                if all_records:
                    total_in = sum(r["quantity"] for r in all_records if r["type"] == "入库")
                    total_out = sum(r["quantity"] for r in all_records if r["type"] == "出库")
                    current_stock = total_in - total_out
                    st.info(f"**当前总库存：{current_stock:.2f} kg**（采购 {total_in:.2f} kg，已投喂 {total_out:.2f} kg）")
                    df = pd.DataFrame(all_records)
                    df = df[["type", "time", "pond", "quantity", "unit_price", "total", "operator", "notes"]]
                    df.columns = ["类型", "时间", "池塘", "数量(kg)", "单价(¥/kg)", "金额(¥)", "操作人", "备注"]
                    st.dataframe(df, width='stretch', hide_index=True)
                else:
                    st.warning("无任何记录")
                if st.button("⬅️ 返回库存总览", key="close_feed_detail"):
                    del st.session_state.viewing_feed
                    st.rerun()

                        # ==================== 蛙苗详情（完整流水：入库 + 出库） ====================
            if "viewing_frog" in st.session_state:
                name = st.session_state.viewing_frog
                st.markdown(f"### 📄 蛙苗「{name}」完整库存流水（入库 + 出库）")

                # 1. 获取采购入库记录
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("""
                    SELECT id, purchased_at, quantity, unit_price, total_amount,
                           supplier, supplier_phone, purchased_by, notes
                    FROM frog_purchase_record_shiwa
                    WHERE frog_type_name = %s
                    ORDER BY purchased_at DESC;
                """, (name,))
                purchase_records = cur.fetchall()

                # 2. 获取外购分配出库记录（通过 frog_type_id 关联）
                # 直接通过 description 包含 "[{name}]" 来匹配
                search_pattern = f"[{name}]%"
                cur.execute("""
                    SELECT 
                        sm.moved_at,
                        p.name AS pond_name,
                        sm.quantity,
                        sm.unit_price,
                        (sm.quantity * COALESCE(sm.unit_price, 20.0)) AS total_cost,
                        sm.created_by,
                        sm.description
                    FROM stock_movement_shiwa sm
                    JOIN pond_shiwa p ON sm.to_pond_id = p.id
                    WHERE sm.movement_type = 'purchase'
                    AND sm.description LIKE %s
                    ORDER BY sm.moved_at DESC;
                """, (search_pattern,))
                allocation_records = cur.fetchall()
                cur.close()
                conn.close()

                # 3. 合并流水
                all_records = []
                for r in purchase_records:
                    all_records.append({
                        "type": "入库",
                        "time": r[1],
                        "pond": "—",
                        "quantity": r[2],
                        "unit_price": r[3],
                        "total": r[4],
                        "operator": r[7] or "系统",
                        "notes": f"采购入库 | 供应商：{r[5] or '—'} | {r[8] or ''}".strip(" | ")
                    })
                for r in allocation_records:
                    all_records.append({
                        "type": "出库",
                        "time": r[0],
                        "pond": r[1],
                        "quantity": r[2],
                        "unit_price": r[3] or 20.0,
                        "total": r[4],
                        "operator": r[5] or "系统",
                        "notes": r[6] or "外购分配入池"
                    })

                all_records.sort(key=lambda x: x["time"], reverse=True)

                if all_records:
                    total_in = sum(r["quantity"] for r in all_records if r["type"] == "入库")
                    total_out = sum(r["quantity"] for r in all_records if r["type"] == "出库")
                    current_stock = total_in - total_out
                    st.info(f"**当前总库存：{current_stock} 只**（采购入库 {total_in} 只，已分配出库 {total_out} 只）")

                    df = pd.DataFrame(all_records)
                    df = df[["type", "time", "pond", "quantity", "unit_price", "total", "operator", "notes"]]
                    df.columns = ["类型", "时间", "池塘", "数量(只)", "单价(¥/只)", "金额(¥)", "操作人", "备注"]
                    st.dataframe(df, width='stretch', hide_index=True)

                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "📥 导出完整流水 CSV",
                        csv,
                        file_name=f"frog_stock_flow_{name}_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("无任何入库或出库记录")

                if st.button("⬅️ 返回库存总览", key="close_frog_detail"):
                    del st.session_state.viewing_frog
                    st.rerun()

        # ==================== 2. 新增采购记录（放入 expander） ====================
        with st.expander("📥 新增采购记录", expanded=False):
            # ========== 饲料 ==========
            with st.form("feed_purchase_form"):
                st.markdown("##### 🌾 饲料")
                c1, c2, c3 = st.columns(3)
                with c1: fname = st.text_input("饲料名称")
                with c2: fprice = st.number_input("单价 (¥/kg)", min_value=0.0, step=1.0, value=20.0)
                with c3: fqty = st.number_input("采购数量 (kg)", min_value=0.0, step=1.0, value=0.0)
                c4, c5 = st.columns(2)
                with c4: fsupp = st.text_input("供应商", placeholder="如 XX 饲料厂")
                with c5: fphone = st.text_input("联系方式", placeholder="手机/固话")
                
                # ===== 新增：采购时间 + 备注 =====
                col_time, col_note = st.columns([2, 3])
                with col_time:
                    f_purch_date = st.date_input("采购日期", value=datetime.today())
                    f_purch_time = st.time_input("采购时间", value=datetime.now().time())
                    f_purchased_at = datetime.combine(f_purch_date, f_purch_time)
                with col_note:
                    f_notes = st.text_input("备注（可选）", placeholder="如：发票号、批次号等")

                st.text_input("采购人", value=current_user, disabled=True)
                submitted_feed = st.form_submit_button("✅ 添加饲料采购")
                if submitted_feed:
                    if not fname.strip():
                        st.error("请输入饲料名称！")
                    else:
                        add_feed_purchase(fname, fprice, fqty, fsupp, fphone, current_user, f_purchased_at, f_notes)
                        st.success(f"饲料「{fname}」已记录")
                        st.rerun()

            st.markdown("---")

            # ========== 蛙苗 ==========
            st.markdown("##### 🐸 蛙苗")
            input_mode = st.radio(
                "数量输入方式",
                ["按只", "按斤"],
                horizontal=True,
                key="frog_input_mode"
            )
            if input_mode == "按只":
                tqty = st.number_input("采购数量 (只)", min_value=0, step=50, value=0, key="frog_qty_zhi")
            else:
                col_jin, col_rate = st.columns(2)
                with col_jin:
                    weight_jin = st.number_input("采购重量 (斤)", min_value=0.1, step=1.0, value=10.0, key="frog_weight_jin")
                with col_rate:
                    rate = st.number_input("每斤约等于多少只", min_value=1, max_value=20, value=4, step=1, key="frog_rate")
                tqty = int(round(weight_jin * rate))
                st.info(f"→ 自动换算为 **{tqty} 只**（{weight_jin} 斤 × {rate} 只/斤）")

            with st.form("frog_purchase_form"):
                tname = st.text_input("蛙型名称", key="frog_name_input")
                tprice = st.number_input("单价 (¥/只)", min_value=0.1, step=1.0, value=20.0, key="frog_price_input")
                tsupp = st.text_input("供应商", placeholder="如 XX 养殖场", key="frog_supplier_input")
                tphone = st.text_input("联系方式", placeholder="手机/微信", key="frog_phone_input")
                
                # ===== 新增：采购时间 + 备注 =====
                col_time, col_note = st.columns([2, 3])
                with col_time:
                    t_purch_date = st.date_input("采购日期", value=datetime.today())
                    t_purch_time = st.time_input("采购时间", value=datetime.now().time())
                    t_purchased_at = datetime.combine(t_purch_date, t_purch_time)
                with col_note:
                    t_notes = st.text_input("备注（可选）", placeholder="如：苗场批次、健康状况等")

                st.text_input("采购人", value=current_user, disabled=True)
                submitted_frog = st.form_submit_button("✅ 添加蛙苗采购")
                if submitted_frog:
                    if not tname.strip():
                        st.error("请输入蛙型名称！")
                    elif tqty <= 0:
                        st.error("采购数量必须大于 0！")
                    else:
                        add_frog_purchase(tname, tprice, tqty, tsupp, tphone, current_user, t_purchased_at, t_notes)
                        if input_mode == "按斤":
                            st.success(f"蛙型「{tname}」已保存（{weight_jin} 斤 ≈ {tqty} 只），流水已记录")
                        else:
                            st.success(f"蛙型「{tname}」已保存（{tqty} 只），流水已记录")
                        st.rerun()

        # ==================== 3. 采购流水记录（分页） ====================
        PAGE_SIZE = 20

        def get_feed_purchase_records(limit=20, offset=0):
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT purchased_at, feed_type_name, quantity_kg, unit_price,
                    total_amount, supplier, supplier_phone, purchased_by, notes
                FROM feed_purchase_record_shiwa
                ORDER BY purchased_at DESC
                LIMIT %s OFFSET %s;
            """, (limit, offset))
            rows = cur.fetchall()
            cur.close(); conn.close()
            return rows

        def get_frog_purchase_records(limit=20, offset=0):
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT purchased_at, frog_type_name, quantity, unit_price,
                    total_amount, supplier, supplier_phone, purchased_by, notes
                FROM frog_purchase_record_shiwa
                ORDER BY purchased_at DESC
                LIMIT %s OFFSET %s;
            """, (limit, offset))
            rows = cur.fetchall()
            cur.close(); conn.close()
            return rows

        def count_feed_records():
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM feed_purchase_record_shiwa;")
            cnt = cur.fetchone()[0]
            cur.close(); conn.close()
            return cnt

        def count_frog_records():
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM frog_purchase_record_shiwa;")
            cnt = cur.fetchone()[0]
            cur.close(); conn.close()
            return cnt

        # ==================== 4. 报表查看 expander ====================
        with st.expander("📊 报表查看", expanded=False):
            # ========== 饲料采购流水 ==========
            st.markdown("##### 饲料采购流水")
            total_feed = count_feed_records()
            total_pages_feed = (total_feed + PAGE_SIZE - 1) // PAGE_SIZE if total_feed > 0 else 1
            if "feed_purchase_page_in_report" not in st.session_state:
                st.session_state.feed_purchase_page_in_report = 0
            current_page_f = st.session_state.feed_purchase_page_in_report
            current_page_f = max(0, min(current_page_f, total_pages_feed - 1))
            st.session_state.feed_purchase_page_in_report = current_page_f

            col_prev_f, col_next_f, col_info_f = st.columns([1, 1, 3])
            with col_prev_f:
                if st.button("⬅️ 上一页", key="feed_prev_report", disabled=(current_page_f == 0)):
                    st.session_state.feed_purchase_page_in_report -= 1
                    st.rerun()
            with col_next_f:
                if st.button("下一页 ➡️", key="feed_next_report", disabled=(current_page_f >= total_pages_feed - 1)):
                    st.session_state.feed_purchase_page_in_report += 1
                    st.rerun()
            with col_info_f:
                st.caption(f"第 {current_page_f + 1} 页 / 共 {total_pages_feed} 页（每页 {PAGE_SIZE} 条）")

            feed_records = get_feed_purchase_records(limit=PAGE_SIZE, offset=current_page_f * PAGE_SIZE)
            if feed_records:
                df_feed = pd.DataFrame(feed_records, columns=[
                    "采购时间", "饲料名称", "数量(kg)", "单价(¥/kg)", "金额(¥)", "供应商", "联系方式", "采购人", "备注"
                ])
                st.dataframe(df_feed, width='stretch', hide_index=True)
            else:
                st.info("暂无饲料采购流水记录")

            st.markdown("---")

            # ========== 蛙苗采购流水 ==========
            st.markdown("##### 蛙苗采购流水")
            total_frog = count_frog_records()
            total_pages_frog = (total_frog + PAGE_SIZE - 1) // PAGE_SIZE if total_frog > 0 else 1
            if "frog_purchase_page_in_report" not in st.session_state:
                st.session_state.frog_purchase_page_in_report = 0
            current_page_t = st.session_state.frog_purchase_page_in_report
            current_page_t = max(0, min(current_page_t, total_pages_frog - 1))
            st.session_state.frog_purchase_page_in_report = current_page_t

            col_prev_t, col_next_t, col_info_t = st.columns([1, 1, 3])
            with col_prev_t:
                if st.button("⬅️ 上一页", key="frog_prev_report", disabled=(current_page_t == 0)):
                    st.session_state.frog_purchase_page_in_report -= 1
                    st.rerun()
            with col_next_t:
                if st.button("下一页 ➡️", key="frog_next_report", disabled=(current_page_t >= total_pages_frog - 1)):
                    st.session_state.frog_purchase_page_in_report += 1
                    st.rerun()
            with col_info_t:
                st.caption(f"第 {current_page_t + 1} 页 / 共 {total_pages_frog} 页（每页 {PAGE_SIZE} 条）")

            frog_records = get_frog_purchase_records(limit=PAGE_SIZE, offset=current_page_t * PAGE_SIZE)
            if frog_records:
                df_frog = pd.DataFrame(frog_records, columns=[
                    "采购时间", "蛙型名称", "数量(只)", "单价(¥/只)", "金额(¥)", "供应商", "联系方式", "采购人", "备注"
                ])
                st.dataframe(df_frog, width='stretch', hide_index=True)
            else:
                st.info("暂无蛙苗采购流水记录")

            st.markdown("---")

            # ========== 月度采购汇总 ==========
            st.markdown("##### 月度采购汇总")
            conn = get_db_connection()
            feed_month = pd.read_sql("""
                SELECT date_trunc('month', purchased_at) AS 月份,
                    SUM(quantity_kg) AS 采购量_kg,
                    SUM(total_amount) AS 采购金额_元
                FROM feed_purchase_record_shiwa
                GROUP BY 月份
                ORDER BY 月份 DESC;
            """, conn)
            frog_month = pd.read_sql("""
                SELECT date_trunc('month', purchased_at) AS 月份,
                    SUM(quantity) AS 采购量_只,
                    SUM(total_amount) AS 采购金额_元
                FROM frog_purchase_record_shiwa
                GROUP BY 月份
                ORDER BY 月份 DESC;
            """, conn)
            conn.close()

            col1, col2 = st.columns(2)
            with col1:
                st.caption("饲料采购")
                if not feed_month.empty:
                    feed_month["月份"] = feed_month["月份"].dt.strftime("%Y-%m")
                    st.dataframe(feed_month.style.format({"采购量_kg": "{:.2f}", "采购金额_元": "¥{:,.2f}"}),
                                width='stretch', hide_index=True)
                else:
                    st.info("暂无饲料采购记录")
            with col2:
                st.caption("蛙型采购")
                if not frog_month.empty:
                    frog_month["月份"] = frog_month["月份"].dt.strftime("%Y-%m")
                    st.dataframe(frog_month.style.format({"采购量_只": "{:.0f}", "采购金额_元": "¥{:,.2f}"}),
                                width='stretch', hide_index=True)
                else:
                    st.info("暂无蛙型采购记录")
    # Tab 6: 销售记录（按斤销售，保留原始斤数）
    # -----------------------------
    with tab6:
        st.subheader("💰 销售记录（按斤计算，1只 ≈ 4斤）")
        ponds = get_all_ponds()
        if not ponds:
            st.warning("暂无可销售池塘")
            # 不 stop，继续渲染历史记录
        else:
            SALEABLE_POND_TYPES = ["商品蛙池", "三年蛙池", "四年蛙池", "五年蛙池", "六年蛙池", "种蛙池"]
            cand = [p for p in ponds if p[2] in SALEABLE_POND_TYPES and p[5] > 0]
            if not cand:
                st.info("没有可销售的蛙（仅显示：商品蛙池、三年~六年蛙池）")
                # 不 stop，继续渲染历史记录
            else:
                # ========== 池塘选择 ==========
                st.markdown("#### 📋 待销售池塘清单（点击选择）")
                pond_options = []
                pond_id_list = []
                for p in cand:
                    pid, name, pond_type, frog_type, max_cap, current = p
                    label = f"[{frog_type}] {name}（{pond_type}｜现存 {current} 只 ≈ {current * 4} 斤）"
                    pond_options.append(label)
                    pond_id_list.append(pid)

                if "selected_sale_pond_id" not in st.session_state:
                    st.session_state.selected_sale_pond_id = pond_id_list[0]

                selected_label = st.radio(
                    "选择要销售的池塘",
                    options=pond_options,
                    index=pond_id_list.index(st.session_state.selected_sale_pond_id),
                    key="sale_pond_radio"
                )
                selected_pond_id = pond_id_list[pond_options.index(selected_label)]
                st.session_state.selected_sale_pond_id = selected_pond_id

                info = next(p for p in cand if p[0] == selected_pond_id)
                st.info(f"✅ 已选：{info[1]}｜类型：{info[2]}｜蛙种：{info[3]}｜库存：{info[5]} 只（≈ {info[5] * 4} 斤）")
                st.markdown("---")

                # ========== 客户选择 ==========
                st.markdown("#### 1. 选择客户")
                customers = get_customers() or []
                c1, c2 = st.columns([3, 1])
                with c1:
                    cust_opt = ["新建客户"] + [f"{c[1]} ({c[3]})" for c in customers]
                    cust_sel = st.selectbox("客户", cust_opt, key="sale_customer")
                new_cust = cust_sel == "新建客户"
                with c2:
                    sale_type = st.radio("销售类型", ["零售", "批发"], horizontal=True, key="sale_type")

                customer_id = None
                if new_cust:
                    with st.form("new_customer"):
                        name = st.text_input("客户姓名（单位/个人）*")
                        contact = st.text_input("联系人", placeholder="如：张先生 / 李阿姨")
                        phone = st.text_input("电话", max_chars=20)
                        if st.form_submit_button("添加客户"):
                            if not name.strip():
                                st.error("请输入客户姓名！")
                            else:
                                # 把联系人拼到备注里，或单独加字段均可
                                # 方案1：拼成 “名称（联系人）” 存入 name
                                full_name = f"{name.strip()}（{contact.strip()}）" if contact.strip() else name.strip()
                                customer_id = add_customer(full_name, phone, sale_type)
                                st.success(f"✅ 客户 {full_name} 已创建")
                                st.rerun()
                else:
                    if customers:
                        customer_id = customers[cust_opt.index(cust_sel) - 1][0]

                # ========== 仅当客户有效时，才显示销售表单 ==========
                if customer_id is None:
                    st.info("请选择或创建客户后再进行销售操作")
                else:
                    # 显示客户信息
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("SELECT name, phone, type FROM customer_shiwa WHERE id = %s;", (customer_id,))
                    cust_detail = cur.fetchone()
                    cur.close()
                    conn.close()
                    if cust_detail:
                        name, phone, ctype = cust_detail
                        phone_str = f"｜电话：{phone}" if phone else ""
                        st.info(f"已选客户：{name}（{ctype}）{phone_str}")

                                    # ========== 销售表单（按斤，动态换算）==========
                st.markdown("#### 2. 销售明细（按实际称重斤数，自动换算扣库存只数）")
                with st.form("sale_form"):
                    pond_id = st.session_state.selected_sale_pond_id
                    pond_info = next(c for c in cand if c[0] == pond_id)
                    max_zhi = pond_info[5]  # 当前库存只数

                    # --- 新增：每只多少斤（默认 0.25 斤/只）---
                    weight_per_frog = st.number_input(
                        "每只约多少斤（建议 0.2~0.3）",
                        min_value=0.01,
                        max_value=1.0,
                        value=0.25,
                        step=0.01,
                        format="%.2f"
                    )

                    # --- 销售重量（斤）---
                    weight_jin = st.number_input(
                        "实际称重销售重量 (斤)",
                        min_value=0.1,
                        step=0.1,
                        value=min(10.0, max_zhi * weight_per_frog)  # 默认最多卖 10 斤或全部
                    )

                    # --- 自动换算只数 ---
                    if weight_per_frog <= 0:
                        quantity_zhi = 0
                    else:
                        quantity_zhi = round(weight_jin / weight_per_frog)

                    # --- 校验 ---
                    if quantity_zhi <= 0:
                        st.error("换算后数量 ≤ 0，请检查输入！")
                        st.form_submit_button("✅ 确认销售", disabled=True)
                    elif quantity_zhi > max_zhi:
                        st.error(f"❌ 换算后需扣 {quantity_zhi} 只，但库存仅 {max_zhi} 只！")
                        st.form_submit_button("✅ 确认销售", disabled=True)
                    else:
                        st.info(f"→ **将扣减库存：{quantity_zhi} 只**（称重 {weight_jin} 斤 ÷ {weight_per_frog} 斤/只 ≈ {weight_jin / weight_per_frog:.2f} 只 → 四舍五入）")

                        # --- 单价（按斤）---
                        default_price_per_jin = 60.0 if sale_type == "零售" else 45.0  # 示例：零售 60元/斤
                        price_per_jin = st.number_input(
                            "单价 (元/斤)",
                            min_value=0.1,
                            value=default_price_per_jin,
                            step=0.5
                        )

                        note = st.text_area("备注")
                        submitted = st.form_submit_button("✅ 确认销售", type="primary")
                        if submitted:
                            current_user = st.session_state.user['username']
                            # 调用 do_sale：传入只数、单价（元/只 = 元/斤 × 斤/只）
                            unit_price_per_zhi = price_per_jin * weight_per_frog
                            do_sale(
                                pond_id=pond_id,
                                customer_id=customer_id,
                                sale_type=sale_type,
                                qty_zhi=quantity_zhi,
                                unit_price_per_zhi=unit_price_per_zhi,
                                weight_jin=weight_jin,  # 原始称重斤数，用于记录
                                note=note,
                                sold_by=current_user
                            )
                            total_yuan = weight_jin * price_per_jin
                            st.success(f"✅ 销售成功：{weight_jin} 斤 × {price_per_jin} 元/斤 = **{total_yuan:.2f} 元**")
                            st.rerun()
        # ========== 销售记录总览（始终显示）==========
        st.markdown("#### 3. 最近销售记录")
        page_size = 20
        if "sale_page" not in st.session_state:
            st.session_state.sale_page = 0
        col_prev, col_next, col_info = st.columns([1, 1, 3])
        current_page = st.session_state.sale_page
        with col_prev:
            if st.button("⬅️ 上一页", disabled=(current_page == 0), key="sale_prev"):
                st.session_state.sale_page -= 1
                st.rerun()
        with col_next:
            if st.button("下一页 ➡️", key="sale_next"):
                st.session_state.sale_page += 1
                st.rerun()
        with col_info:
            st.caption(f"第 {current_page + 1} 页（每页 {page_size} 条）")

        offset = current_page * page_size
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT sr.id, p.name pond, c.name customer, sr.sale_type, sr.quantity,
                sr.unit_price, sr.total_amount, sr.sold_at, sr.note, sr.weight_jin, sr.sold_by
            FROM sale_record_shiwa sr
            JOIN pond_shiwa p ON p.id = sr.pond_id
            JOIN customer_shiwa c ON c.id = sr.customer_id
            ORDER BY sr.sold_at DESC
            LIMIT %s OFFSET %s;
        """, (page_size, offset))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if rows:
            df = pd.DataFrame(
                rows,
                columns=["ID", "池塘", "客户", "类型", "数量_只", "单价_元每只", "总金额", "时间", "备注", "原始斤数", "销售人"]
            )
            # 兜底：如果 weight_jin 为 NULL（旧记录），用 quantity * 4
            df["重量_斤"] = df["原始斤数"].fillna(df["数量_只"] * 4)
            df["单价_元每斤"] = df["单价_元每只"] / 4
            df_display = df[["池塘", "客户", "类型", "重量_斤", "单价_元每斤", "总金额", "销售人", "时间", "备注"]]

            st.dataframe(
                df_display.style.format({
                    "重量_斤": "{:.2f} 斤",
                    "单价_元每斤": "¥{:.2f}/斤",
                    "总金额": "¥{:.2f}"
                }),
                width='stretch',
                hide_index=True
            )

            csv = df_display.to_csv(index=False)
            st.download_button(
                "📥 导出当前页 CSV",
                csv,
                file_name=f"sale_page_{current_page + 1}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            if len(rows) == page_size:
                st.info("✅ 还有更多记录，请点击「下一页」查看")
            else:
                st.success("已到最后一页")
        else:
            if current_page == 0:
                st.info("暂无销售记录")
            else:
                st.warning("没有更多数据了")
                st.session_state.sale_page -= 1
    # ----------------------------- Tab 7: 投资回报 ROI -----------------------------
    with tab7:
        st.subheader("📈 蛙种投资回报率（ROI）分析")
        st.caption("ROI = (销售收入 - 总成本) / 总成本 × 100% | 外购成本按 20 元/只估算（若未填单价）")

        # ========== 汇总视图 ==========
        roi_data = get_roi_data()
        if roi_data:
            df_roi = pd.DataFrame(roi_data)
            st.dataframe(
                df_roi.style.format({
                    "喂养成本 (¥)": "¥{:.2f}",
                    "外购成本 (¥)": "¥{:.2f}",
                    "总成本 (¥)": "¥{:.2f}",
                    "销售收入 (¥)": "¥{:.2f}",
                    "净利润 (¥)": "¥{:.2f}",
                    "ROI (%)": "{:.2f}%"
                }),
                width='stretch',
                hide_index=True
            )

            # ROI 柱状图
            st.markdown("### 📊 ROI 对比")
            chart_df = df_roi.set_index("蛙种")["ROI (%)"]
            st.bar_chart(chart_df, height=300)

            # 导出按钮
            csv = df_roi.to_csv(index=False)
            st.download_button(
                "📥 导出汇总报告 (CSV)",
                csv,
                file_name=f"shiwa_roi_summary_{pd.Timestamp.now().strftime('%Y%m%d')}.csv"
            )
        else:
            st.info("暂无 ROI 数据")

        st.markdown("---")
        st.subheader("🔍 ROI 明细：按池塘查看成本与收入")

        # ========== 明细视图 ==========
        feedings, purchases, sales = get_pond_roi_details()
        
        if not (feedings or purchases or sales):
            st.info("暂无喂养、外购或销售明细记录")
        else:
            # 按池塘分组
            from collections import defaultdict
            pond_details = defaultdict(lambda: {"feedings": [], "purchases": [], "sales": []})

            # 喂养
            for row in feedings:
                pond_name = row[0]
                pond_details[pond_name]["feedings"].append({
                    "feed_type": row[3],
                    "weight_kg": row[2],
                    "unit_price": row[4],
                    "total_cost": row[5],
                    "time": row[6]
                })

            # 外购
            for row in purchases:
                pond_name = row[0]
                pond_details[pond_name]["purchases"].append({
                    "quantity": row[2],
                    "unit_price": row[3] or 20.0,
                    "total_cost": row[4],
                    "time": row[5]
                })

            # 销售
            for row in sales:
                pond_name = row[0]
                pond_details[pond_name]["sales"].append({
                    "quantity": row[2],
                    "unit_price": row[3],
                    "total_amount": row[4],
                    "customer": row[6],
                    "time": row[5]
                })

            # 显示每个池塘
            for pond_name, details in pond_details.items():
                with st.expander(f"📍 {pond_name}", expanded=False):
                    frog_type = None
                    if details["feedings"]:
                        frog_type = next(iter(details["feedings"]))  # 无法直接取，改用其他方式
                    # 实际上我们可以在查询时带上 frog_type，但为简化，此处略过

                    # 喂养记录
                    if details["feedings"]:
                        st.markdown("**🍽️ 喂养记录**")
                        for f in details["feedings"]:
                            st.caption(f"- {f['feed_type']} {f['weight_kg']}kg × ¥{f['unit_price']}/kg = **¥{f['total_cost']:.2f}** ({f['time'].strftime('%Y-%m-%d')})")

                    # 外购记录
                    if details["purchases"]:
                        st.markdown("**📦 外购记录**")
                        for p in details["purchases"]:
                            st.caption(f"- 外购 {p['quantity']} 只 × ¥{p['unit_price']}/只 = **¥{p['total_cost']:.2f}** ({p['time'].strftime('%Y-%m-%d')})")

                    # 销售记录
                    if details["sales"]:
                        st.markdown("**💰 销售记录**")
                        for s in details["sales"]:
                            st.caption(f"- 销售 {s['quantity']} 只 × ¥{s['unit_price']}/只 = **¥{s['total_amount']:.2f}** （客户：{s['customer']}，{s['time'].strftime('%Y-%m-%d')})")

                    # 小计（可选）
                    total_feed = sum(f["total_cost"] for f in details["feedings"])
                    total_purchase = sum(p["total_cost"] for p in details["purchases"])
                    total_sales_amt = sum(s["total_amount"] for s in details["sales"])
                    net = total_sales_amt - total_feed - total_purchase

if __name__ == "__main__":
    run()