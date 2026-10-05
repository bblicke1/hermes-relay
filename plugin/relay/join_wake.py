"""LOCAL ONLY: HA wake adapter; filename retained for the local overlay."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)
QUEUE_WAIT_SECONDS = 5 * 60
WAKE_THROTTLE_SECONDS = 15 * 60


async def wake_queued_phone(payload: dict[str, Any], count: int) -> bool:
    """Send through the shared guarded sender without blocking the relay loop."""
    home = Path(os.getenv('HERMES_HOME') or Path.home() / '.hermes')
    script = home / 'scripts' / 'phone_alert.py'
    text = str(payload.get('text') or payload.get('title') or 'Open Relay')
    preview = (text.splitlines() or ['Open Relay'])[0]
    url = os.getenv('HERMES_RELAY_OPEN_URL', 'app://com.axiomlabs.hermesrelay.sideload')
    actions = [{'action': 'URI', 'title': 'Open Relay', 'uri': url}]
    process: asyncio.subprocess.Process | None = None
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable, str(script), f'Hermes: {count} waiting', preview,
            '--tier', 'urgent', '--tag', 'relay-queue', '--url', url,
            '--actions', json.dumps(actions),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(process.communicate(), timeout=45)
        if process.returncode != 0:
            logger.warning('local HA queue wake sender failed (code=%s)', process.returncode)
            return False
        return True
    except asyncio.CancelledError:
        if process and process.returncode is None:
            process.kill()
            await process.wait()
        raise
    except Exception:
        if process and process.returncode is None:
            process.kill()
            await process.wait()
        logger.exception('local HA queue wake failed')
        return False
