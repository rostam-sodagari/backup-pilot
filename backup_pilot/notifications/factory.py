from __future__ import annotations

from typing import Any, Dict, List, Optional

from backup_pilot.logging.logger import get_logger
from backup_pilot.notifications.email_notifier import EmailNotifier
from backup_pilot.notifications.slack_notifier import SlackNotifier


def create_notifiers(config: Optional[Dict[str, Any]]) -> List[Any]:
    if not config:
        return []

    logger = get_logger()
    notifiers: List[Any] = []

    if slack_cfg := config.get("slack"):
        if isinstance(slack_cfg, dict) and slack_cfg.get("webhook_url"):
            notifiers.append(SlackNotifier(webhook_url=slack_cfg["webhook_url"]))
        else:
            logger.warning("Slack notification skipped: webhook_url is required")

    if email_cfg := config.get("email"):
        if not isinstance(email_cfg, dict):
            logger.warning("Email notification skipped: invalid email config")
        elif not all(email_cfg.get(k) for k in ("from", "to", "smtp_host")):
            logger.warning(
                "Email notification skipped: from, to, and smtp_host are required"
            )
        else:
            notifiers.append(
                EmailNotifier(
                    smtp_host=email_cfg["smtp_host"],
                    smtp_port=int(email_cfg.get("smtp_port", 587)),
                    username=email_cfg.get("username"),
                    password=email_cfg.get("password"),
                    from_addr=email_cfg["from"],
                    to_addr=email_cfg["to"],
                )
            )
    return notifiers
