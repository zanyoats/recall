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
    pytest.param(
        ["-f", "graph.pl", "-s", "summary(X,Count)?"],
        {
            "summary(x, 2)",
            "summary(y, 1)",
        },
        id="graph-summary",
    ),
    pytest.param(
        ["-f", "graph.pl", "-s", "num_nodes(Count)?"],
        {"num_nodes(3)"},
        id="graph-num_nodes",
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
    ############################################################################
    # products.pl
    ############################################################################
    pytest.param(
        ["-f", "products.pl", "-s", "product_info_all_cities(Product,Sales,MinCost,MaxCost)?"],
        {
          'product_info_all_cities(groundhog_day, 232, 8, 11)',
          'product_info_all_cities(space_odyssey, 979, 12, 18)',
          'product_info_all_cities(total_recall, 404, 7, 7)',
        },
        id="products-product_info_all_cities",
    ),
    pytest.param(
        ["-f", "products.pl", "-s", "product_info_by_city(Product,City,Sales,MinCost,MaxCost)?"],
        {
            'product_info_by_city(groundhog_day, la, 44, 11, 11)',
            'product_info_by_city(groundhog_day, nyc, 61, 9, 9)',
            'product_info_by_city(groundhog_day, philly, 127, 8, 8)',
            'product_info_by_city(total_recall, la, 404, 7, 7)',
            'product_info_by_city(space_odyssey, philly, 103, 12, 18)',
            'product_info_by_city(space_odyssey, nyc, 77, 14, 14)',
            'product_info_by_city(space_odyssey, la, 799, 13, 13)',
        },
        id="products-product_info_by_city",
    ),
    pytest.param(
        ["-f", "products.pl", "-s", "num_products(Count)?"],
        {'num_products(3)'},
        id="products-num_products",
    ),
    pytest.param(
        ["-f", "products.pl", "-s", "total_sales(Count)?"],
        {'total_sales(1615)'},
        id="products-total_sales",
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
