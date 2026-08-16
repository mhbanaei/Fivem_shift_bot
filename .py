"""Token checker — double-click or run ``python check_token.py``.

Reads the token from EMBEDDED_TOKEN in bot.py and checks it directly
against the Discord API, without starting the bot. Uses only the
standard library, so it works with any Python.

Output:
  [OK]  token is valid  ->  bot name (id)
  [401] token is invalid -> reset it in the Developer Portal
  [NET] cannot reach discord.com -> internet/firewall problem
"""

import json
import re
import sys
import urllib.request
from pathlib import Path

BASE = Path(__file__).resolve().parent
BOT_PY = BASE / "bot.py"


def read_token() -> str:
    src = BOT_PY.read_text(encoding="utf-8")
    match = re.search(r'EMBEDDED_TOKEN\s*=\s*"([^"]+)"', src)
    if not match:
        print("[X] Could not find EMBEDDED_TOKEN in bot.py")
        sys.exit(1)
    return match.group(1).strip()


def main():
    token = read_token()
    print(f"[*] Token found in bot.py (last 6 chars: ...{token[-6:]})")
    print("[*] Checking with Discord API...")

    req = urllib.request.Request(
        "https://discord.com/api/v10/users/@me",
        headers={"Authorization": f"Bot {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(f"[OK]  Token is VALID. Bot: {data.get('username')} (id={data.get('id')})")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        if exc.code == 401:
            print("[401] Token is INVALID or has been reset.")
            print("      -> Go to https://discord.com/developers/applications")
            print("      -> Your app -> Bot -> Reset Token -> copy the new one")
            print("      -> Paste it into EMBEDDED_TOKEN in bot.py")
        elif exc.code == 403:
            print(f"[403] Discord rejected the request. Body: {body}")
            print("      -> Common causes:")
            print("         - Bot application disabled/flagged by Discord (check Developer Portal)")
            print("         - Wrong token type pasted (must be the BOT token, not client secret)")
            print("         - User-Agent blocked (rare)")
        elif exc.code == 429:
            print(f"[429] Rate limited. Body: {body}")
        else:
            print(f"[HTTP {exc.code}] Unexpected response. Body: {body}")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"[NET] Cannot reach discord.com: {exc}")
        print("      -> Check internet connection / firewall. This is NOT a token problem.")
    finally:
        print()
        try:
            input("Press Enter to close...")
        except EOFError:
            pass


if __name__ == "__main__":
    main()
