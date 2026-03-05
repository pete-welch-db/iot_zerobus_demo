from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Dict, Optional

import requests

from config import AppConfig


@dataclass
class GenieClient:
    config: AppConfig

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.token}",
            "Content-Type": "application/json",
        }

    def available(self) -> bool:
        return bool(self.config.workspace_host and self.config.token and self.config.genie_space_id)

    def _extract_answer(self, message: Dict) -> str:
        attachments = message.get("attachments") or []
        rendered = []
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            text = (
                attachment.get("text")
                or attachment.get("content")
                or ((attachment.get("query") or {}).get("description"))
            )
            if text:
                rendered.append(str(text))
        if rendered:
            return "\n\n".join(rendered)
        return str(message.get("content") or "")

    def _poll_message(self, space_id: str, conversation_id: str, message_id: str) -> Dict:
        base = self.config.workspace_host
        url = (
            f"{base}/api/2.0/genie/spaces/{space_id}/conversations/"
            f"{conversation_id}/messages/{message_id}"
        )
        last_body: Dict = {}
        for _ in range(30):
            resp = requests.get(url, headers=self._headers(), timeout=45)
            if not resp.ok:
                return {"ok": False, "error": f"{url}: {resp.status_code} {resp.text[:400]}"}
            body = resp.json()
            last_body = body
            status = (body.get("status") or "").upper()
            if status in {"COMPLETED", "FAILED", "CANCELED"}:
                if status != "COMPLETED":
                    return {"ok": False, "error": f"Genie message status={status}", "raw": body}
                return {
                    "ok": True,
                    "answer": self._extract_answer(body),
                    "raw": body,
                }
            time.sleep(1.0)
        return {
            "ok": False,
            "error": "Timed out waiting for Genie response completion.",
            "raw": last_body,
        }

    def ask(self, prompt: str, conversation_id: Optional[str] = None) -> Dict:
        if not self.available() or self.config.genie_space_id in {"", "__AUTO__"}:
            return {
                "ok": False,
                "error": "Genie is not configured. Set APP_GENIE_SPACE_ID for this app environment.",
            }

        base = self.config.workspace_host
        space_id = self.config.genie_space_id
        try:
            if conversation_id:
                start_url = (
                    f"{base}/api/2.0/genie/spaces/{space_id}/conversations/"
                    f"{conversation_id}/messages"
                )
                start_resp = requests.post(
                    start_url,
                    headers=self._headers(),
                    json={"content": prompt},
                    timeout=45,
                )
                if not start_resp.ok:
                    return {"ok": False, "error": f"{start_url}: {start_resp.status_code} {start_resp.text[:400]}"}
                start_body = start_resp.json()
                message = start_body.get("message") or start_body
                message_id = message.get("id")
                if not message_id:
                    return {"ok": False, "error": f"Missing message id in Genie response: {start_body}"}
                polled = self._poll_message(space_id, conversation_id, message_id)
                polled["conversation_id"] = conversation_id
                return polled

            start_url = f"{base}/api/2.0/genie/spaces/{space_id}/start-conversation"
            start_resp = requests.post(
                start_url,
                headers=self._headers(),
                json={"content": prompt},
                timeout=45,
            )
            if not start_resp.ok:
                return {"ok": False, "error": f"{start_url}: {start_resp.status_code} {start_resp.text[:400]}"}
            start_body = start_resp.json()
            conversation = start_body.get("conversation") or {}
            message = start_body.get("message") or {}
            new_conversation_id = conversation.get("id")
            message_id = message.get("id")
            if not new_conversation_id or not message_id:
                return {"ok": False, "error": f"Missing Genie conversation/message id: {start_body}"}
            polled = self._poll_message(space_id, new_conversation_id, message_id)
            polled["conversation_id"] = new_conversation_id
            return polled
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
