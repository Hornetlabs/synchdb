"""Checks for shared test helpers that do not need source database containers."""

import pytest

from synchdbtests.common import verify_default_type_mappings


@pytest.mark.parametrize("vendor", ["oracle", "oracle23ai", "olr"])
@pytest.mark.parametrize("target_type", ["smallint", "integer", "bigint", "numeric"])
def test_oracle_number_base_type_accepts_precision_dependent_targets(vendor, target_type):
    # Attribute metadata records only NUMBER, including the reported failing
    # NUMBER(10, 0) -> bigint snapshot and NUMBER(19, 0) -> numeric columns.
    assert verify_default_type_mappings("number", target_type, vendor)


@pytest.mark.parametrize("vendor", ["oracle", "oracle23ai", "olr"])
@pytest.mark.parametrize("target_type", ["real", "double precision", "text"])
def test_oracle_number_rejects_lossy_or_unrelated_targets(vendor, target_type):
    assert not verify_default_type_mappings("number", target_type, vendor)


@pytest.mark.parametrize("vendor", ["mysql", "sqlserver", "postgres"])
@pytest.mark.parametrize("target_type", ["smallint", "integer", "bigint"])
def test_oracle_number_exception_is_scoped_to_oracle_connectors(vendor, target_type):
    assert not verify_default_type_mappings("number", target_type, vendor)


@pytest.mark.parametrize("vendor", ["mysql", "sqlserver", "postgres"])
def test_existing_numeric_mapping_remains_valid(vendor):
    assert verify_default_type_mappings("number", "numeric", vendor)


@pytest.mark.parametrize("vendor", ["oracle", "oracle23ai", "olr"])
def test_other_oracle_types_keep_their_mapping(vendor):
    assert verify_default_type_mappings("varchar2", "character varying", vendor)
    assert not verify_default_type_mappings("varchar2", "bigint", vendor)
