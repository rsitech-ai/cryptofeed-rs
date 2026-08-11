import importlib.util
import pathlib
import unittest


MODULE_PATH = pathlib.Path(__file__).parents[1] / "release_canary.py"
SPEC = importlib.util.spec_from_file_location("release_canary", MODULE_PATH)
release_canary = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(release_canary)


def venue(
    venue_id,
    *,
    live=True,
    reconnects=0,
    invalidations=0,
    valid_books=0,
    dropped=0,
    queue=1,
    capacity=256,
):
    return {
        "id": venue_id,
        "live": live,
        "reconnects": reconnects,
        "book_invalidations": invalidations,
        "valid_books": valid_books,
        "events_dropped": dropped,
        "queue_occupancy": queue,
        "queue_capacity": capacity,
    }


def sample(elapsed, *, venues=None, live=200, ready=200, ui=200, rss=200_000):
    return {
        "elapsed_seconds": elapsed,
        "live_http": live,
        "ready_http": ready,
        "ui_http": ui,
        "status_http": 200,
        "metrics_http": 200,
        "api_latency_ms": 4.0,
        "rss_kib": rss,
        "cpu_percent": 20.0,
        "venues": venues
        or [
            venue("binance-usdm", valid_books=5),
            venue("okx-swap", valid_books=5),
        ],
    }


class AnalyzeSamplesTests(unittest.TestCase):
    def setUp(self):
        self.expectations = {
            "configured_venues": ["binance-usdm", "okx-swap"],
            "l2_books": {"binance-usdm": 5, "okx-swap": 5},
        }

    def test_stable_release_canary_is_go(self):
        samples = [sample(0), sample(600, rss=205_000), sample(1_200, rss=207_000)]

        result = release_canary.analyze_samples(samples, self.expectations)

        self.assertEqual("GO", result["verdict"])
        self.assertEqual([], result["holds"])
        self.assertEqual(2, result["venues_live_min"])

    def test_counter_increase_is_hold_even_when_final_value_is_small(self):
        start = sample(0)
        end = sample(
            600,
            venues=[
                venue("binance-usdm", valid_books=5, invalidations=1),
                venue("okx-swap", valid_books=5),
            ],
        )

        result = release_canary.analyze_samples([start, end], self.expectations)

        self.assertEqual("HOLD", result["verdict"])
        self.assertTrue(any("book invalidations" in item for item in result["holds"]))

    def test_health_or_ui_loss_is_hold(self):
        result = release_canary.analyze_samples(
            [sample(0), sample(30, ready=503), sample(60, ui=500)], self.expectations
        )

        self.assertEqual("HOLD", result["verdict"])
        self.assertTrue(any("health" in item for item in result["holds"]))
        self.assertTrue(any("UI" in item for item in result["holds"]))

    def test_status_or_metrics_loss_is_hold(self):
        status_failed = sample(30)
        status_failed["status_http"] = 500
        metrics_failed = sample(60)
        metrics_failed["metrics_http"] = 0

        result = release_canary.analyze_samples(
            [sample(0), status_failed, metrics_failed], self.expectations
        )

        self.assertEqual("HOLD", result["verdict"])
        self.assertTrue(any("observability" in item for item in result["holds"]))

    def test_missing_l2_books_is_hold(self):
        degraded = [
            venue("binance-usdm", valid_books=4),
            venue("okx-swap", valid_books=5),
        ]

        result = release_canary.analyze_samples(
            [sample(0), sample(30, venues=degraded)], self.expectations
        )

        self.assertEqual("HOLD", result["verdict"])
        self.assertTrue(any("valid books" in item for item in result["holds"]))

    def test_queue_saturation_is_hold(self):
        saturated = [
            venue("binance-usdm", valid_books=5, queue=90, capacity=100),
            venue("okx-swap", valid_books=5),
        ]

        result = release_canary.analyze_samples(
            [sample(0), sample(30, venues=saturated)], self.expectations
        )

        self.assertEqual("HOLD", result["verdict"])
        self.assertTrue(any("queue" in item for item in result["holds"]))

    def test_recovered_single_reconnect_is_warning_not_hold(self):
        end = sample(
            600,
            venues=[
                venue("binance-usdm", valid_books=5, reconnects=1),
                venue("okx-swap", valid_books=5),
            ],
        )

        result = release_canary.analyze_samples([sample(0), end], self.expectations)

        self.assertEqual("GO", result["verdict"])
        self.assertTrue(any("reconnect" in item for item in result["warnings"]))

    def test_excessive_memory_growth_is_hold(self):
        samples = [
            sample(0, rss=200_000),
            sample(600, rss=220_000),
            sample(1_200, rss=260_000),
        ]

        result = release_canary.analyze_samples(
            samples,
            self.expectations,
            {"max_rss_growth_mib_per_hour": 64.0},
        )

        self.assertEqual("HOLD", result["verdict"])
        self.assertTrue(any("RSS growth" in item for item in result["holds"]))

    def test_short_probe_does_not_call_cold_start_growth_a_leak(self):
        samples = [sample(0, rss=80_000), sample(60, rss=140_000)]

        result = release_canary.analyze_samples(samples, self.expectations)

        self.assertEqual("GO", result["verdict"])
        self.assertTrue(any("RSS trend" in item for item in result["warnings"]))


class PrometheusParserTests(unittest.TestCase):
    def test_parses_labels_and_ignores_comments(self):
        text = """# HELP ignored docs
marketfeed_up 1
marketfeed_venue_valid_books{id=\"okx-swap\"} 5
"""

        parsed = release_canary.parse_prometheus(text)

        self.assertEqual(1.0, parsed[("marketfeed_up", ())])
        self.assertEqual(
            5.0,
            parsed[("marketfeed_venue_valid_books", (("id", "okx-swap"),))],
        )


class SmokeRetryTests(unittest.TestCase):
    def test_retries_only_transient_tape_warmup_failure(self):
        self.assertTrue(
            release_canary.should_retry_smoke(
                1, "PASS status\nFAIL  critical tape empty bybit-spot\nRESULT: FAIL"
            )
        )
        self.assertFalse(
            release_canary.should_retry_smoke(
                1, "FAIL  book BBO invalid binance-usdm\nRESULT: FAIL"
            )
        )
        self.assertFalse(release_canary.should_retry_smoke(0, "RESULT: PASS"))


class DaemonLogTests(unittest.TestCase):
    def test_structured_error_is_hold_and_warning_is_reported(self):
        lines = [
            '{"level":"INFO","fields":{"message":"started"}}',
            '{"level":"WARN","fields":{"message":"slow source"}}',
            '{"level":"ERROR","fields":{"message":"worker failed"}}',
        ]

        result = release_canary.analyze_daemon_log("\n".join(lines))

        self.assertEqual(1, result["error_count"])
        self.assertEqual(1, result["warning_count"])
        self.assertIn("worker failed", result["error_messages"])


if __name__ == "__main__":
    unittest.main()
