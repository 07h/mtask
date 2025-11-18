"""Tests for helper functions."""
import pytest
from mtask import calculate_backoff


def test_calculate_backoff():
    """Test exponential backoff calculation."""
    # Test basic exponential growth
    backoff_0 = calculate_backoff(0, base_delay=1)
    backoff_1 = calculate_backoff(1, base_delay=1)
    backoff_2 = calculate_backoff(2, base_delay=1)
    
    # Should grow exponentially: 1, 2, 4, ...
    assert 1.0 <= backoff_0 <= 1.2  # 1 + 10% jitter
    assert 2.0 <= backoff_1 <= 2.3  # 2 + 10% jitter
    assert 4.0 <= backoff_2 <= 4.5  # 4 + 10% jitter


def test_calculate_backoff_max_delay():
    """Test that backoff respects max delay."""
    # Even with high retry count, should not exceed max_delay
    backoff = calculate_backoff(100, base_delay=1, max_delay=30)
    
    assert backoff <= 33  # max_delay + 10% jitter


def test_calculate_backoff_custom_base():
    """Test backoff with custom base delay."""
    backoff = calculate_backoff(0, base_delay=5, max_delay=60)
    
    assert 5.0 <= backoff <= 5.6  # 5 + 10% jitter

