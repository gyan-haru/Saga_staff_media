from __future__ import annotations

from database import refresh_employee_slots


if __name__ == "__main__":
    counts = refresh_employee_slots()
    print(
        "employee slot rebuild complete:",
        f"created={counts.get('slots_created', 0)}",
        f"closed={counts.get('slots_closed', 0)}",
        f"slot_candidates={counts.get('slot_candidates', 0)}",
    )
