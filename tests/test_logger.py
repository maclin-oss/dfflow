import json
import os
import pytest
import pandas as pd
from dfflow.logging.logger import DFLogger


@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


def test_logger_creates_file(tmp_path, sample_df):
    log_file = tmp_path / "test.log"
    logger = DFLogger(log_file=str(log_file))
    logger.info("Test message", sample_df)
    assert os.path.exists(log_file)


def test_logger_text_mode_content(tmp_path, sample_df):
    log_file = tmp_path / "test.log"
    logger = DFLogger(log_file=str(log_file), mode="text")
    logger.info("text test", sample_df)
    content = log_file.read_text()
    assert "INFO" in content
    assert "text test" in content
    assert "Shape" in content


def test_logger_json_mode_content(tmp_path, sample_df):
    log_file = tmp_path / "test.json.log"
    logger = DFLogger(log_file=str(log_file), mode="json")
    logger.info("json test", sample_df)
    entry = json.loads(log_file.read_text().strip())
    assert entry["level"] == "INFO"
    assert entry["message"] == "json test"
    assert entry["shape"] == [3, 2]
    assert "columns" in entry
    assert "preview" in entry


def test_logger_without_df(tmp_path):
    log_file = tmp_path / "test.log"
    logger = DFLogger(log_file=str(log_file))
    logger.info("no dataframe")
    content = log_file.read_text()
    assert "no dataframe" in content


def test_logger_min_level_filters(tmp_path):
    log_file = tmp_path / "test.log"
    logger = DFLogger(log_file=str(log_file), min_level="WARNING")
    logger.debug("debug msg")
    logger.info("info msg")
    logger.warning("warning msg")
    content = log_file.read_text()
    assert "debug msg" not in content
    assert "info msg" not in content
    assert "warning msg" in content


def test_logger_all_levels(tmp_path):
    log_file = tmp_path / "test.log"
    logger = DFLogger(log_file=str(log_file), min_level="DEBUG")
    logger.debug("debug")
    logger.info("info")
    logger.warning("warning")
    logger.error("error")
    content = log_file.read_text()
    assert "debug" in content
    assert "info" in content
    assert "warning" in content
    assert "error" in content


def test_logger_invalid_df():
    logger = DFLogger()
    with pytest.raises(TypeError):
        logger.info("test", df="not_df")


def test_logger_invalid_mode():
    with pytest.raises(ValueError):
        DFLogger(mode="xml")


def test_logger_invalid_min_level():
    with pytest.raises(ValueError):
        DFLogger(min_level="VERBOSE")


def test_logger_file_mode_append(tmp_path):
    log_file = tmp_path / "test.log"
    logger = DFLogger(log_file=str(log_file), file_mode="a")
    logger.info("first")
    logger.info("second")
    content = log_file.read_text()
    assert "first" in content
    assert "second" in content


def test_logger_invalid_file_mode():
    with pytest.raises(ValueError):
        DFLogger(file_mode="x")