from __future__ import annotations

import pytest

from app.core.exceptions import ValidationFailed
from app.migration.mapping import DEFAULT_MAPPING, merge_mapping, validate_overrides


def test_unknown_key_rejected() -> None:
    with pytest.raises(ValidationFailed) as e:
        validate_overrides({"lakehose_default": "x"})
    assert e.value.field == "mapping.lakehose_default"


def test_wrong_type_rejected() -> None:
    with pytest.raises(ValidationFailed):
        validate_overrides({"warehouse_schemas": "gold"})
    with pytest.raises(ValidationFailed):
        validate_overrides({"allow_direct_managed_copy": "yes"})


def test_dicts_merge_and_scalars_replace() -> None:
    base = dict(DEFAULT_MAPPING, catalog_to_lakehouse={"a": "lh_a", "b": "lh_b"})
    m = merge_mapping(base, {"catalog_to_lakehouse": {"b": "lh_bb"}, "warehouse_name": "wh2"})
    assert m["catalog_to_lakehouse"] == {"a": "lh_a", "b": "lh_bb"} and m["warehouse_name"] == "wh2"
    assert base["catalog_to_lakehouse"]["b"] == "lh_b"
