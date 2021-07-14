"""
Tests for generic logging functions.
"""
from datetime import datetime

from bodywork_pipeline_utils.logging import configure_logger

from _pytest.capture import CaptureFixture


def test_configure_logger_log_format(capsys: CaptureFixture):
    log = configure_logger()
    log.info("foo")
    stdout = capsys.readouterr().out
    log_record_parts = stdout.split(" - ")
    assert len(log_record_parts) == 4
    assert datetime.fromisoformat(log_record_parts[0].replace(",", ".")) is not None
    assert log_record_parts[1] == "INFO"
    assert "test_logging.test_configure_logger_log_format" in log_record_parts[2]
    assert "foo" in log_record_parts[3]
