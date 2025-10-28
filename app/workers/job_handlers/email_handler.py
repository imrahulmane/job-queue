"""
Email sending job handler
"""
import asyncio
from typing import Dict, Any

from app.constants.job_types import JobTypes
from app.workers.job_handlers.base_handler import BaseJobHandler
from app.core.logger import info, debug


class EmailHandler(BaseJobHandler):
    """Handler for email sending jobs"""

    @property
    def job_type(self) -> str:
        return JobTypes.emails.value


    async def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process email sending job

        Expected payload:
        {
            "to": "recipient@example.com",
            "subject": "Email subject",
            "body": "Email body content"
        }
        """
        debug(self.logger, "Email handler started", context=payload)

        # Extract email details
        to = payload.get("to", "unknown@example.com")
        subject = payload.get("subject", "No subject")
        body = payload.get("body", "")

        # Simulate email sending work
        # TODO: Replace with actual email sending logic (SMTP, SendGrid, etc.)
        await asyncio.sleep(3)

        result = {
            "status": "sent",
            "to": to,
            "subject": subject,
            "message": "Email sent successfully (mock)"
        }

        info(self.logger, "Email sent successfully", context=result)
        return result