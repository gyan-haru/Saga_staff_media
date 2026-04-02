from __future__ import annotations

from database import rebuild_policy_topics


if __name__ == "__main__":
    counts = rebuild_policy_topics()
    print(
        "policy topic rebuild complete:",
        f"project_topic_links={counts.get('project_topic_links', 0)}",
        f"person_topic_rollups={counts.get('person_topic_rollups', 0)}",
        f"topics_removed={counts.get('topics_removed', 0)}",
    )
