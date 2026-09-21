from app.config.settings import Settings, load_settings


def test_default_settings_construct_without_a_config_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no config.yaml / config.example.yaml here
    settings = load_settings()
    assert isinstance(settings, Settings)
    assert settings.buffer.capacity == 10_000
    assert settings.strategy.friction_coverage_ratio == 2.5
    assert settings.strategy.breakout_atr_multiplier == settings.strategy.breakout_atr_multiplier  # locked pairing lives in Feature Engine (Phase 3), config just stores the single source value


def test_env_override_applies(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CMH_BUFFER__CAPACITY", "12345")
    settings = load_settings()
    assert settings.buffer.capacity == 12345
