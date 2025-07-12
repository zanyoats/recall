import subprocess
import pytest

@pytest.mark.parametrize("args, expected", [
    ############################################################################
    # graph.pl
    ############################################################################
    pytest.param(
        ["-f", "graph.pl", "-s", "reachable(X,Y)?"],
        {
            "reachable(x, y)",
            "reachable(y, z)",
            "reachable(x, z)",
        },
        id="graph-reacheable"
    ),
    pytest.param(
        ["-f", "graph.pl", "-s", "indirect(X,Y)?"],
        {"indirect(x, z)"},
        id="graph-indirect",
    ),
    pytest.param(
        ["-f", "graph.pl", "-s", "unreachable(X,Y)?"],
        {
            "unreachable(x, x)",
            "unreachable(y, y)", "unreachable(y, x)",
            "unreachable(z, z)", "unreachable(z, y)", "unreachable(z, x)",
        },
        id="graph-unreachable",
    ),
    ############################################################################
    # bare_rule.pl
    ############################################################################
    pytest.param(
        ["-f", "bare_rule.pl", "-s", "all_fruits(X)?"],
        {"all_fruits(pear)"},
        id="bare_rule-all_fruits",
    ),
    pytest.param(
        ["-f", "bare_rule.pl", "-s", "all_fruits2(X)?"],
        {"all_fruits2(pear)"},
        id="bare_rule-all_fruits2",
    ),
    pytest.param(
        ["-f", "bare_rule.pl", "-s", "all_fruits3(X)?"],
        set(),
        id="bare_rule-all_fruits3",
    ),
])
def test_recall_cli(args, expected):
    """Run `recall` with various queries and compare output as a set."""
    # Ensure the binary exists
    bin_path = "../target/debug/recall"
    result = subprocess.run(
        [bin_path, *args],
        capture_output=True, text=True, check=True
    )
    # Split, trim, filter out blanks, and collect into a set
    got = { line.strip() for line in result.stdout.splitlines() if line.strip() }
    assert got == expected
