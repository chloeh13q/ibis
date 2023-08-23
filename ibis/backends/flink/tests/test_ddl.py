import pytest

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch


@pytest.fixture
def awards_players_schema():
    return sch.Schema(
        {
            "playerID": dt.string,
            "awardID": dt.string,
            "yearID": dt.int32,
            "lgID": dt.string,
            "tie": dt.string,
            "notes": dt.string,
        }
    )


@pytest.fixture
def awards_players_csv_connector_configs():
    return {
        "connector": "filesystem",
        "path": "ci/ibis-testing-data/csv/awards_players.csv",
        "format": "csv",
        "csv.ignore-parse-errors": "true",
    }


def test_create_table_from_filesystem_source(
    con, awards_players_schema, temp_table, awards_players_csv_connector_configs
):
    new_table = con.create_table(
        temp_table,
        schema=awards_players_schema,
        tbl_properties=awards_players_csv_connector_configs,
    )
    assert len(con.list_tables()) == 5
    assert temp_table in con.list_tables()
    assert new_table.schema() == awards_players_schema


def test_drop_table(con):
    con.drop_table("awards_players")
    assert len(con.list_tables()) == 3
    assert "awards_players" not in con.list_tables()
