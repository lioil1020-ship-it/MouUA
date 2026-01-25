"""Simple Modbus scheduler example that groups Tag mappings into batched read requests.

This module does not require `pymodbus` to run the grouping logic; it exposes
`group_reads(mapping_list, max_regs=120)` which returns read batches suitable
for passing to a pymodbus client (address_type, unit_id, start, count, tags).

It also contains a small demo when executed as __main__.
"""
from typing import List, Dict, Any, Tuple


def group_reads(tags: List[Dict[str, Any]], max_regs: int = 120) -> List[Dict[str, Any]]:
    """Group tags (canonical mapping dicts) into read batches.

    Returns list of batches with keys: address_type, unit_id, start, count, tags
    Each batch's address/count do not exceed `max_regs` (approximate limit).
    """
    # bucket by unit_id + address_type
    buckets = {}
    for t in tags:
        key = (t.get('unit_id'), t.get('address_type'))
        buckets.setdefault(key, []).append(t)

    batches = []
    for (unit, atype), items in buckets.items():
        # sort by address
        items_sorted = sorted(items, key=lambda x: int(x.get('address') or 0))
        i = 0
        while i < len(items_sorted):
            start_addr = int(items_sorted[i].get('address') or 0)
            end_addr = start_addr + int(items_sorted[i].get('count') or 1) - 1
            batch_tags = [items_sorted[i]]
            j = i + 1
            while j < len(items_sorted):
                t = items_sorted[j]
                t_start = int(t.get('address') or 0)
                t_end = t_start + int(t.get('count') or 1) - 1
                # if contiguous and within max_regs, merge
                if t_start <= end_addr + 1 and (t_end - start_addr + 1) <= max_regs:
                    end_addr = max(end_addr, t_end)
                    batch_tags.append(t)
                    j += 1
                    continue
                # if gap but still can include within max_regs, include
                if (t_end - start_addr + 1) <= max_regs and t_start <= end_addr + max_regs:
                    end_addr = max(end_addr, t_end)
                    batch_tags.append(t)
                    j += 1
                    continue
                break
            batch = {
                'address_type': atype,
                'unit_id': unit,
                'start': start_addr,
                'count': end_addr - start_addr + 1,
                'tags': batch_tags,
                # include function code for convenience
                'function_code': (1 if atype == 'coil' else 2 if atype == 'discrete_input' else 3 if atype == 'holding_register' else 4)
            }
            batches.append(batch)
            i = j
    return batches


def demo():
    sample = [
        {'name': 'T1', 'unit_id': 1, 'address_type': 'holding_register', 'address': 0, 'count': 2},
        {'name': 'T2', 'unit_id': 1, 'address_type': 'holding_register', 'address': 2, 'count': 2},
        {'name': 'T3', 'unit_id': 1, 'address_type': 'holding_register', 'address': 10, 'count': 1},
        {'name': 'C1', 'unit_id': 2, 'address_type': 'coil', 'address': 0, 'count': 1},
    ]
    batches = group_reads(sample, max_regs=10)
    for b in batches:
        print(f"Batch: unit={b['unit_id']} type={b['address_type']} start={b['start']} count={b['count']} tags={[t['name'] for t in b['tags']]}")


if __name__ == '__main__':
    demo()
