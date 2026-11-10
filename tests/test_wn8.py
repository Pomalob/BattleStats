"""WN8: the formula against hand-checked points, and the expected-values table itself."""
import wn8


def _tally(factor: float, wins: float = 1.0, battles: int = 10) -> wn8.Tally:
    """`battles` on one tank, every counter at `factor` times the expected value."""
    tank_id, exp = next(iter(wn8.expected().items()))
    t = wn8.Tally()
    t.add(tank_id, battles, exp["expDamage"] * battles * factor, exp["expFrag"] * battles * factor,
          exp["expSpot"] * battles * factor, exp["expDef"] * battles * factor,
          exp["expWinRate"] / 100 * battles * wins)
    return t


def test_the_table_is_loaded_and_keyed_by_tank_id():
    table = wn8.expected()
    assert len(table) > 500
    assert all(isinstance(k, int) and row["expDamage"] > 0 for k, row in table.items())


def test_an_exactly_average_player_scores_1565():
    # Every ratio is 1, so the formula collapses to the sum of its weights.
    assert _tally(1.0).score() == 980 + 210 + 155 + 75 + 145


def test_doing_nothing_scores_zero_and_more_is_better():
    assert _tally(0.0, wins=0.0).score() == 0
    assert _tally(0.5).score() < _tally(1.0).score() < _tally(2.0).score()


def test_tanks_missing_from_the_table_are_left_out():
    t = wn8.Tally()
    t.add(999999999, 5, 10_000, 10, 10, 10, 5)
    t.add(None, 5, 10_000, 10, 10, 10, 5)
    assert t.battles == 0 and t.score() is None
