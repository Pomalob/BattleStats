"""WN8 from replay data.

WN8 compares what a player did with what an average player does on the same tank. The
per-tank expected values are XVM's table for Lesta's servers, kept as a copy next to this
module (source: https://static.modxvm.com/wn8-data-exp/json/lesta/wn8exp.json) so that a
request never depends on someone else's server. Refresh the file by downloading it again.

The table is calibrated on random battles: in clan 7x7 and Onslaught the number is good for
comparing players with each other, not with the WN8 they know from their profile.
"""
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

_TABLE = Path(__file__).with_name("wn8exp_lesta.json")


@lru_cache(maxsize=1)
def expected() -> dict[int, dict]:
    """Tank compact descriptor (typeCompDescr in the replay) -> expected per-battle values."""
    with _TABLE.open(encoding="utf-8") as f:
        return {int(row["IDNum"]): row for row in json.load(f)["data"]}


@dataclass
class Tally:
    """Actual and expected totals over any set of battles, possibly on different tanks."""
    battles: int = 0
    damage: float = 0
    frags: float = 0
    spotted: float = 0
    defence: float = 0
    wins: float = 0
    exp_damage: float = 0
    exp_frags: float = 0
    exp_spotted: float = 0
    exp_defence: float = 0
    exp_wins: float = 0

    def add(self, tank_id, battles, damage, frags, spotted, defence, wins) -> None:
        """Battles on one tank. A tank missing from the table is left out entirely."""
        exp = expected().get(int(tank_id)) if tank_id is not None else None
        if not exp or not battles:
            return
        # The database hands sums over as Decimal; keep everything in floats.
        battles = int(battles)
        self.battles += battles
        self.damage += float(damage or 0)
        self.frags += float(frags or 0)
        self.spotted += float(spotted or 0)
        self.defence += float(defence or 0)
        self.wins += float(wins or 0)
        self.exp_damage += exp["expDamage"] * battles
        self.exp_frags += exp["expFrag"] * battles
        self.exp_spotted += exp["expSpot"] * battles
        self.exp_defence += exp["expDef"] * battles
        self.exp_wins += exp["expWinRate"] / 100 * battles

    def score(self) -> int | None:
        """WN8, or None when no battle was on a tank the table knows."""
        if not self.battles or not self.exp_damage:
            return None
        r_damage = self.damage / self.exp_damage
        r_frag = self.frags / self.exp_frags if self.exp_frags else 0
        r_spot = self.spotted / self.exp_spotted if self.exp_spotted else 0
        r_def = self.defence / self.exp_defence if self.exp_defence else 0
        r_win = self.wins / self.exp_wins if self.exp_wins else 0

        win = max(0.0, (r_win - 0.71) / (1 - 0.71))
        damage = max(0.0, (r_damage - 0.22) / (1 - 0.22))
        frag = max(0.0, min(damage + 0.2, (r_frag - 0.12) / (1 - 0.12)))
        spot = max(0.0, min(damage + 0.1, (r_spot - 0.38) / (1 - 0.38)))
        defence = max(0.0, min(damage + 0.1, (r_def - 0.10) / (1 - 0.10)))
        return round(980 * damage + 210 * damage * frag + 155 * frag * spot
                     + 75 * defence * frag + 145 * min(1.8, win))
