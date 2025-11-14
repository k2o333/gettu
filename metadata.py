import sqlite3
import logging
from datetime import datetime
from config import METADATA_DB_PATH

def init_metadata_db():
    """初始化元数据数据库"""
    try:
        conn = sqlite3.connect(METADATA_DB_PATH)
        cursor = conn.cursor()
        
        # 创建元数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_type TEXT UNIQUE NOT NULL,
                last_update_date TEXT,
                record_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 创建触发器来自动更新updated_at字段
        cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS update_metadata_timestamp
            AFTER UPDATE ON metadata
            FOR EACH ROW
            BEGIN
                UPDATE metadata SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
            END;
        ''')
        
        conn.commit()
        conn.close()
        
        logging.info("元数据数据库初始化完成")
    except Exception as e:
        logging.error(f"初始化元数据数据库失败: {str(e)}")
        raise


def get_last_update_date(data_type: str) -> str:
    """获取指定数据类型的最后更新日期"""
    try:
        conn = sqlite3.connect(METADATA_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT last_update_date FROM metadata WHERE data_type = ?
        ''', (data_type,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result and result[0] else None
    except Exception as e:
        logging.error(f"获取{data_type}最后更新日期失败: {str(e)}")
        return None


def update_last_update_date(data_type: str, update_date: str):
    """更新指定数据类型的最后更新日期"""
    try:
        conn = sqlite3.connect(METADATA_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO metadata (data_type, last_update_date, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (data_type, update_date))
        
        conn.commit()
        conn.close()
        
        logging.info(f"更新{data_type}最后更新日期为: {update_date}")
    except Exception as e:
        logging.error(f"更新{data_type}最后更新日期失败: {str(e)}")
        raise


def get_record_count(data_type: str) -> int:
    """获取指定数据类型的记录数量"""
    try:
        conn = sqlite3.connect(METADATA_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT record_count FROM metadata WHERE data_type = ?
        ''', (data_type,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else 0
    except Exception as e:
        logging.error(f"获取{data_type}记录数量失败: {str(e)}")
        return 0


def update_record_count(data_type: str, count: int):
    """更新指定数据类型的记录数量"""
    try:
        conn = sqlite3.connect(METADATA_DB_PATH)
        cursor = conn.cursor()
        
        # 如果记录不存在，先插入一条记录
        cursor.execute('''
            INSERT OR IGNORE INTO metadata (data_type, record_count)
            VALUES (?, 0)
        ''', (data_type,))
        
        cursor.execute('''
            UPDATE metadata SET record_count = ? WHERE data_type = ?
        ''', (count, data_type))
        
        conn.commit()
        conn.close()
        
        logging.info(f"更新{data_type}记录数量为: {count}")
    except Exception as e:
        logging.error(f"更新{data_type}记录数量失败: {str(e)}")
        raise


def get_all_data_types() -> list:
    """获取所有数据类型"""
    try:
        conn = sqlite3.connect(METADATA_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT data_type FROM metadata')
        results = cursor.fetchall()
        conn.close()
        
        return [row[0] for row in results]
    except Exception as e:
        logging.error(f"获取所有数据类型失败: {str(e)}")
        return []


def get_metadata_summary() -> dict:
    """获取元数据摘要"""
    try:
        conn = sqlite3.connect(METADATA_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT data_type, last_update_date, record_count, updated_at
            FROM metadata
            ORDER BY updated_at DESC
        ''')
        
        results = cursor.fetchall()
        conn.close()
        
        summary = {}
        for row in results:
            summary[row[0]] = {
                'last_update_date': row[1],
                'record_count': row[2],
                'updated_at': row[3]
            }
        
        return summary
    except Exception as e:
        logging.error(f"获取元数据摘要失败: {str(e)}")
        return {}


def delete_metadata(data_type: str):
    """删除指定数据类型的元数据"""
    try:
        conn = sqlite3.connect(METADATA_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM metadata WHERE data_type = ?', (data_type,))
        
        conn.commit()
        conn.close()
        
        logging.info(f"删除{data_type}元数据")
    except Exception as e:
        logging.error(f"删除{data_type}元数据失败: {str(e)}")
        raise


def update_metadata(data_type: str, update_date: str = None, record_count: int = None):
    """更新指定数据类型的元数据"""
    try:
        conn = sqlite3.connect(METADATA_DB_PATH)
        cursor = conn.cursor()
        
        # 检查记录是否存在
        cursor.execute('SELECT 1 FROM metadata WHERE data_type = ?', (data_type,))
        exists = cursor.fetchone()
        
        if exists:
            # 记录存在，进行更新
            update_fields = []
            params = []
            
            if update_date is not None:
                update_fields.append('last_update_date = ?')
                params.append(update_date)
            
            if record_count is not None:
                update_fields.append('record_count = ?')
                params.append(record_count)
            
            if update_fields:
                update_fields.append('updated_at = CURRENT_TIMESTAMP')
                params.append(data_type)
                
                query = f"UPDATE metadata SET {', '.join(update_fields)} WHERE data_type = ?"
                cursor.execute(query, params)
        else:
            # 记录不存在，插入新记录
            fields = ['data_type']
            placeholders = ['?']
            params = [data_type]
            
            if update_date is not None:
                fields.append('last_update_date')
                placeholders.append('?')
                params.append(update_date)
            
            if record_count is not None:
                fields.append('record_count')
                placeholders.append('?')
                params.append(record_count)
            
            query = f"INSERT INTO metadata ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
            cursor.execute(query, params)
        
        conn.commit()
        conn.close()
        
        logging.info(f"更新{data_type}元数据: date={update_date}, count={record_count}")
    except Exception as e:
        logging.error(f"更新{data_type}元数据失败: {str(e)}")
        raise