from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Generator, Iterable, List, Optional

import requests


logger = logging.getLogger(__name__)


class SnowflakeCortexAgentClient:
    def __init__(
        self,
        account_url: str,
        auth_token: str,
        timeout_seconds: int = 300,
        read_timeout_seconds: int = 120,
    ) -> None:
        normalized = account_url.strip()
        if normalized and not normalized.startswith(("http://", "https://")):
            normalized = "https://" + normalized
        self.base_url = normalized.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update(
            {
                'Authorization': f'Bearer {auth_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            }
        )
        # Demo behavior: disable TLS verification
        self.session.verify = False
        try:
            import urllib3  # type: ignore

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
        self.timeout_seconds = timeout_seconds
        self.read_timeout_seconds = read_timeout_seconds
        # Track the last HTTP error encountered so the UI can surface it
        self.last_error: Optional[str] = None

    # Threads API
    def create_thread(self, application_name: str = "hcls_agent_st") -> Optional[str]:
        url = f"{self.base_url}/api/v2/cortex/threads"
        payload = {"origin_application": application_name}
        try:
            resp = self.session.post(url, json=payload, timeout=self.timeout_seconds)
            resp.raise_for_status()
            data = resp.json()
            return data.get('thread_id')
        except requests.RequestException as exc:
            self.last_error = self._format_http_error(exc)
            logger.error("create_thread failed: %s", self.last_error)
            return None

    def list_threads(self, limit: int = 20, origin_application: Optional[str] = None) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v2/cortex/threads"
        try:
            params: Dict[str, Any] = {"limit": limit}
            if origin_application:
                params["origin_application"] = origin_application
            resp = self.session.get(url, params=params, timeout=(10, self.read_timeout_seconds))
            resp.raise_for_status()
            data = resp.json()
            # Expected to be { threads: [...] } or a list; handle both
            if isinstance(data, dict) and 'threads' in data and isinstance(data['threads'], list):
                return data['threads']
            if isinstance(data, list):
                return data
            return []
        except requests.RequestException as exc:
            self.last_error = self._format_http_error(exc)
            logger.error("list_threads failed: %s", self.last_error)
            return []

    def get_thread(self, thread_id: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/api/v2/cortex/threads/{thread_id}"
        try:
            resp = self.session.get(url, timeout=(10, self.read_timeout_seconds))
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            self.last_error = self._format_http_error(exc)
            logger.error("get_thread failed: %s", self.last_error)
            return None

    def describe_thread(
        self, thread_id: str, page_size: int = 50, last_message_id: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Describe thread and return metadata plus a page of messages.

        Docs: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-threads-rest-api#describe-thread
        """
        url = f"{self.base_url}/api/v2/cortex/threads/{thread_id}"
        params: Dict[str, Any] = {"page_size": page_size}
        if last_message_id is not None:
            params["last_message_id"] = last_message_id
        try:
            resp = self.session.get(url, params=params, timeout=(10, self.read_timeout_seconds))
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            self.last_error = self._format_http_error(exc)
            logger.error("describe_thread failed: %s", self.last_error)
            return None

    def delete_thread(self, thread_id: str) -> bool:
        url = f"{self.base_url}/api/v2/cortex/threads/{thread_id}"
        try:
            resp = self.session.delete(url, timeout=(10, self.read_timeout_seconds))
            resp.raise_for_status()
            return True
        except requests.RequestException as exc:
            self.last_error = self._format_http_error(exc)
            logger.error("delete_thread failed: %s", self.last_error)
            return False

    # Agent run APIs
    def run_agent(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        messages: List[Dict[str, Any]],
        thread_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
        tool_choice: Optional[Dict[str, Any]] = None,
        stream: bool = True,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}:run"
        payload: Dict[str, Any] = {"messages": messages}
        if tool_choice is not None:
            payload["tool_choice"] = tool_choice
        if thread_id is not None and parent_message_id is not None:
            payload["thread_id"] = thread_id
            payload["parent_message_id"] = parent_message_id
        if stream:
            return self._post_sse(url, payload)
        return self._post_json(url, payload)

    def run_agent_stream(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        messages: List[Dict[str, Any]],
        thread_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
        tool_choice: Optional[Dict[str, Any]] = None,
        yield_events: bool = False,
    ) -> Generator[Dict[str, Any], None, None]:
        url = f"{self.base_url}/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}:run"
        payload: Dict[str, Any] = {"messages": messages}
        if tool_choice is not None:
            payload["tool_choice"] = tool_choice
        if thread_id is not None and parent_message_id is not None:
            payload["thread_id"] = thread_id
            payload["parent_message_id"] = parent_message_id
        try:
            with self.session.post(
                url,
                json=payload,
                timeout=(10, self.read_timeout_seconds),
                stream=True,
                headers={'Accept': 'text/event-stream'},
            ) as resp:
                if resp.status_code != 200:
                    yield {"type": "error", "error": f"HTTP {resp.status_code}: {resp.text[:2000]}"}
                    return
                for event in self._iter_sse(resp):
                    if not event:
                        continue
                    if yield_events:
                        yield {"type": "event", "event": event}
                    event_name = event.get('event') if isinstance(event, dict) else None
                    content = self._extract_content(event)
                    if event_name == 'response':
                        if content:
                            yield {"type": "final", "content": content}
                    else:
                        if content:
                            yield {"type": "content", "content": content}
        except requests.ReadTimeout as exc:
            self.last_error = f"Stream read timeout: {exc}"
            yield {"type": "error", "error": self.last_error}
        except requests.RequestException as exc:
            self.last_error = self._format_http_error(exc)
            yield {"type": "error", "error": self.last_error}

    # Internal helpers
    def _post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            resp = self.session.post(url, json=payload, timeout=self.timeout_seconds)
            resp.raise_for_status()
            return {"status": "success", "json": resp.json()}
        except requests.RequestException as exc:
            self.last_error = self._format_http_error(exc)
            return {"status": "error", "error": self.last_error}

    def _post_sse(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            with self.session.post(
                url, json=payload, timeout=self.timeout_seconds, stream=True, headers={'Accept': 'text/event-stream'}
            ) as resp:
                if resp.status_code != 200:
                    return {"status": "error", "error": f"HTTP {resp.status_code}: {resp.text[:2000]}"}
                full_text: List[str] = []
                events: List[Dict[str, Any]] = []
                for event in self._iter_sse(resp):
                    if not event:
                        continue
                    events.append(event)
                    event_name = event.get('event') if isinstance(event, dict) else None
                    content = self._extract_content(event)
                    if event_name == 'response':
                        if content:
                            full_text.append(content)
                    else:
                        if content:
                            full_text.append(content)
                return {"status": "success", "response": "".join(full_text), "events": events}
        except requests.RequestException as exc:
            self.last_error = self._format_http_error(exc)
            return {"status": "error", "error": self.last_error}

    def _iter_sse(self, resp: requests.Response) -> Generator[Optional[Dict[str, Any]], None, None]:
        buffer_lines: List[str] = []
        for raw_line in resp.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue
            line = raw_line.strip('\n')
            if not line:
                event = self._parse_sse_block(buffer_lines)
                buffer_lines = []
                yield event
            else:
                buffer_lines.append(line)
        if buffer_lines:
            yield self._parse_sse_block(buffer_lines)

    def _parse_sse_block(self, lines: Iterable[str]) -> Optional[Dict[str, Any]]:
        event_type: Optional[str] = None
        data_lines: List[str] = []
        for line in lines:
            if line.startswith('event:'):
                event_type = line[len('event:') :].strip()
            elif line.startswith('data:'):
                data_lines.append(line[len('data:') :].lstrip())
        data_str = "\n".join(data_lines).strip()
        if not data_str:
            return None
        try:
            parsed = json.loads(data_str)
        except json.JSONDecodeError:
            logger.debug("Non-JSON SSE data: %s", data_str[:200])
            result: Dict[str, Any] = {"data": data_str}
            if event_type:
                result["event"] = event_type
            return result
        if isinstance(parsed, dict):
            if event_type and "event" not in parsed:
                parsed["event"] = event_type
            return parsed
        else:
            result = {"data": parsed}
            if event_type:
                result["event"] = event_type
            return result

    def _extract_content(self, event: Dict[str, Any]) -> str:
        if not isinstance(event, dict):
            return ""
        event_name = event.get('event')
        if event_name in ('response.text.delta', 'response.text'):
            text = event.get('text')
            if isinstance(text, str):
                return text
        if event_name == 'response':
            content_list = event.get('content')
            if isinstance(content_list, list):
                parts: List[str] = []
                for block in content_list:
                    if isinstance(block, dict):
                        if block.get('type') == 'text' and isinstance(block.get('text'), str):
                            parts.append(block['text'])
                        elif 'text' in block and isinstance(block['text'], str):
                            parts.append(block['text'])
                if parts:
                    return "".join(parts)
        if 'content' in event:
            if isinstance(event['content'], str):
                return event['content']
            if isinstance(event['content'], list):
                parts2: List[str] = []
                for b in event['content']:
                    if isinstance(b, dict) and b.get('type') == 'text':
                        t = b.get('text')
                        if isinstance(t, str):
                            parts2.append(t)
                if parts2:
                    return "".join(parts2)
        try:
            messages = event.get('output', {}).get('messages')
            if isinstance(messages, list):
                parts3: List[str] = []
                for m in messages:
                    if not isinstance(m, dict):
                        continue
                    cl = m.get('content')
                    if isinstance(cl, list):
                        for c in cl:
                            if isinstance(c, dict) and c.get('type') == 'text':
                                t2 = c.get('text')
                                if isinstance(t2, str):
                                    parts3.append(t2)
                if parts3:
                    return "".join(parts3)
        except Exception:
            return ""
        return ""

    def _format_http_error(self, exc: requests.RequestException) -> str:
        base = f"{exc}"
        if hasattr(exc, 'response') and getattr(exc, 'response') is not None:
            resp = exc.response  # type: ignore[attr-defined]
            try:
                text = resp.text
            except Exception:
                text = "<no body>"
            return f"{base} | HTTP {resp.status_code}: {text[:2000]}"
        return base


def build_client_from_env() -> SnowflakeCortexAgentClient:
    url = os.environ.get("SNOWFLAKE_ACCOUNT_URL", "").strip()
    token = os.environ.get("SNOWFLAKE_AUTH_TOKEN", "").strip()
    if not url or not token:
        raise RuntimeError("SNOWFLAKE_ACCOUNT_URL and SNOWFLAKE_AUTH_TOKEN must be set")
    return SnowflakeCortexAgentClient(url, token)
