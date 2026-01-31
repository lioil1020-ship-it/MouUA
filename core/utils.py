import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def safe_getattr(obj: Any, attr: str, default: Any = None) -> Any:
    """安全属性获取"""
    try:
        return getattr(obj, attr, default)
    except (AttributeError, TypeError):
        return default


def safe_call(func: Any, *args, default: Any = None, **kwargs) -> Any:
    """安全函数调用"""
    if func is None:
        return default
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.warning(f"Function call failed: {e}")
        return default


def safe_item_data(
    item: Any, column: int, role: Optional[Any] = None, default: Any = None
) -> Any:
    """统一的树项数据访问"""
    if item is None:
        return default
    try:
        if role is None:
            return item.data(column, default)
        return item.data(column, role)
    except (AttributeError, TypeError, IndexError):
        return default


def safe_item_text(item: Any, column: int, default: str = "") -> str:
    """安全文本获取"""
    if item is None:
        return default
    try:
        text = item.text(column)
        return text if text is not None else default
    except (AttributeError, TypeError, IndexError):
        return default

def validate_and_get_float(value: Any, default: float) -> float:
    """浮点数验证"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_dict_get(d: dict, *keys, default: Any = None) -> Any:
    """嵌套字典安全访问"""
    if not isinstance(d, dict):
        return default
    result = d
    for key in keys:
        if isinstance(result, dict):
            result = result.get(key, default)
        else:
            return default
    return result


def update_tree_item_data(item: Any, column: int, role: Any, value: Any) -> bool:
    """安全数据更新"""
    if item is None:
        return False
    try:
        item.setData(column, role, value)
        return True
    except (AttributeError, TypeError):
        logger.warning(f"Failed to update item data at column {column}")
        return False


def update_tree_item_text(item: Any, column: int, text: str) -> bool:
    """安全文本更新"""
    if item is None:
        return False
    try:
        item.setText(column, str(text))
        return True
    except (AttributeError, TypeError):
        logger.warning(f"Failed to update item text at column {column}")
        return False
