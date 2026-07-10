"""inject.py <ID> [<ID> ...] | --all

Splices module fragments (content/modules/<ID>.js) into the MODS array of
player/index.html, between /* MODS:BEGIN */ and /* MODS:END */ markers.
Each entry is wrapped in /* MOD:<ID>:BEGIN */ ... /* MOD:<ID>:END */ so
re-injection replaces in place. Canonical track order is always preserved.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PLAYER = ROOT / "player" / "index.html"
MODULES = ROOT / "content" / "modules"

CANONICAL = (
    [f"A{i}" for i in range(1, 6)] + [f"B{i}" for i in range(1, 5)]
    + [f"C{i}" for i in range(1, 5)] + [f"D{i}" for i in range(1, 6)]
    + [f"E{i}" for i in range(1, 7)] + [f"F{i}" for i in range(1, 5)]
    + [f"G{i}" for i in range(1, 5)] + [f"H{i}" for i in range(1, 5)]
)


def fragment_literal(mod_id: str) -> str:
    """Return the object-literal text of a fragment (export default stripped)."""
    src = (MODULES / f"{mod_id}.js").read_text(encoding="utf-8")
    m = re.search(r"export\s+default\s*", src)
    if not m:
        raise SystemExit(f"{mod_id}.js: no `export default` found")
    body = src[m.end():].strip()
    if body.endswith(";"):
        body = body[:-1].rstrip()
    if not (body.startswith("{") and body.endswith("}")):
        raise SystemExit(f"{mod_id}.js: export default must be a single object literal")
    return body


def current_entries(player_src: str) -> dict:
    """Map of mod_id -> literal currently in the player."""
    entries = {}
    for m in re.finditer(
        r"/\* MOD:([A-H]\d):BEGIN \*/\n(.*?)\n/\* MOD:\1:END \*/,", player_src, re.S
    ):
        entries[m.group(1)] = m.group(2)
    return entries


def splice(player_src: str, entries: dict) -> str:
    begin = player_src.index("/* MODS:BEGIN */")
    end = player_src.index("/* MODS:END */")
    blocks = []
    for mid in CANONICAL:
        if mid in entries:
            blocks.append(f"/* MOD:{mid}:BEGIN */\n{entries[mid]}\n/* MOD:{mid}:END */,")
    inner = "/* MODS:BEGIN */\n" + "\n".join(blocks) + ("\n" if blocks else "")
    return player_src[:begin] + inner + player_src[end:]


def inject(mod_ids):
    player_src = PLAYER.read_text(encoding="utf-8")
    before = len(player_src)
    entries = current_entries(player_src)
    for mid in mod_ids:
        entries[mid] = fragment_literal(mid)
    out = splice(player_src, entries)
    PLAYER.write_text(out, encoding="utf-8")
    print(f"injected {', '.join(mod_ids)} | entries: {len(entries)}/{len(CANONICAL)} "
          f"| player: {before:,} -> {len(out):,} bytes")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        raise SystemExit("usage: inject.py <ID> [<ID>...] | --all")
    if args == ["--all"]:
        ids = [p.stem for p in sorted(MODULES.glob("*.js")) if p.stem in CANONICAL]
    else:
        ids = [a.upper() for a in args]
        for i in ids:
            if i not in CANONICAL:
                raise SystemExit(f"unknown module id: {i}")
    inject(ids)
