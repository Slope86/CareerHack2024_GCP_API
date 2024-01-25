from util.cloud_run_upscale import cloud_run_upscale, get_resources_limits


def test_get_resources_limits():
    result = get_resources_limits()
    assert isinstance(result, dict)
    assert "memory_limit" in result
    assert "cpu_limit" in result
    assert isinstance(result["memory_limit"], str)
    assert isinstance(result["cpu_limit"], str)
    assert result["memory_limit"].endswith("Mi")
    assert result["cpu_limit"].endswith("m")


def test_cloud_run_upscale():
    current_limits = get_resources_limits()
    current_memory_limit = current_limits["memory_limit"]
    current_cpu_limit = current_limits["cpu_limit"]
    result = cloud_run_upscale(memory_limit=current_memory_limit, cpu_limit=current_cpu_limit)
    assert result == 200
