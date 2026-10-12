from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from tests.conftest import make_settings


@pytest.mark.parametrize(
    "flag", ["AUTH_DISABLED", "FABRIC_ALLOW_AZ_CLI", "ALLOW_ENV_CREDENTIALS", "ALLOW_PRIVATE_HOSTS"]
)
def test_dev_flags_refused_in_production(tmp_path: Path, flag: str) -> None:
    with pytest.raises(ValidationError, match=flag):
        make_settings(
            tmp_path,
            **{
                "APP_ENV": "production",
                "AUTH_DISABLED": False,
                "API_KEY_HASHES": "$argon2id$x",
                flag: True,
            },
        )


def test_production_needs_api_keys(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="API_KEY_HASHES"):
        make_settings(tmp_path, APP_ENV="production", AUTH_DISABLED=False)
    s = make_settings(
        tmp_path,
        APP_ENV="production",
        AUTH_DISABLED=False,
        API_KEY_HASHES="'$argon2id$a' \n $argon2id$b",
    )
    assert len(s.api_key_hashes) == 2


def test_ai_configured(tmp_path: Path) -> None:
    assert not make_settings(tmp_path).ai_configured
    assert make_settings(tmp_path, ANTHROPIC_API_KEY="sk-ant-x").ai_configured


def test_validation_errors_do_not_echo_secrets(tmp_path: Path) -> None:
    with pytest.raises(ValidationError) as e:
        make_settings(
            tmp_path,
            APP_ENV="production",
            AUTH_DISABLED=True,
            ANTHROPIC_API_KEY="sk-ant-very-secret",
        )
    assert "sk-ant-very-secret" not in str(e.value)
