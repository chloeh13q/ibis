import pytest
from pytest import param

import ibis
from ibis.backends.flink.compiler.core import translate


@pytest.fixture
def schema():
    return [
        ('a', 'int8'),
        ('b', 'int16'),
        ('c', 'int32'),
        ('d', 'int64'),
        ('e', 'float32'),
        ('f', 'float64'),
        ('g', 'string'),
        ('h', 'boolean'),
        ('i', 'timestamp'),
        ('j', 'date'),
        ('k', 'time'),
    ]


@pytest.fixture
def table(schema):
    return ibis.table(schema, name='table')


def test_translate_sum(snapshot, table):
    expr = table.a.sum()
    result = translate(expr.as_table().op())
    snapshot.assert_match(str(result), "out.sql")


def test_translate_count_star(snapshot, table):
    expr = table.group_by(table.i).size()
    result = translate(expr.as_table().op())
    snapshot.assert_match(str(result), "out.sql")


@pytest.mark.parametrize(
    "unit",
    [
        param("ms", id="timestamp_ms"),
        param("s", id="timestamp_s"),
    ],
)
def test_translate_timestamp_from_unix(snapshot, table, unit):
    expr = table.d.to_timestamp(unit=unit)
    result = translate(expr.as_table().op())
    snapshot.assert_match(result, "out.sql")


def test_translate_complex_projections(snapshot, table):
    expr = (
        table.group_by(['a', 'c'])
        .aggregate(the_sum=table.b.sum())
        .group_by('a')
        .aggregate(mad=lambda x: x.the_sum.abs().mean())
    )
    result = translate(expr.as_table().op())
    snapshot.assert_match(result, "out.sql")


def test_translate_filter(snapshot, table):
    expr = table[((table.c > 0) | (table.c < 0)) & table.g.isin(['A', 'B'])]
    result = translate(expr.as_table().op())
    snapshot.assert_match(result, "out.sql")


@pytest.mark.parametrize(
    "kind",
    [
        "year",
        "quarter",
        "month",
        "week_of_year",
        "day_of_year",
        "day",
        "hour",
        "minute",
        "second",
    ],
)
def test_translate_extract_fields(snapshot, table, kind):
    expr = getattr(table.i, kind)().name("tmp")
    result = translate(expr.as_table().op())
    snapshot.assert_match(result, "out.sql")


def test_translate_complex_groupby_aggregation(snapshot, table):
    keys = [table.i.year().name('year'), table.i.month().name('month')]
    b_unique = table.b.nunique()
    expr = table.group_by(keys).aggregate(total=table.count(), b_unique=b_unique)
    result = translate(expr.as_table().op())
    snapshot.assert_match(result, "out.sql")


def test_translate_simple_filtered_agg(snapshot, table):
    expr = table.b.nunique(where=table.g == 'A')
    result = translate(expr.as_table().op())
    snapshot.assert_match(result, "out.sql")


def test_translate_complex_filtered_agg(snapshot, table):
    expr = table.group_by('b').aggregate(
        total=table.count(),
        avg_a=table.a.mean(),
        avg_a_A=table.a.mean(where=table.g == 'A'),
        avg_a_B=table.a.mean(where=table.g == 'B'),
    )
    result = translate(expr.as_table().op())
    snapshot.assert_match(result, "out.sql")


def test_translate_value_counts(snapshot, table):
    expr = table.i.year().value_counts()
    result = translate(expr.as_table().op())
    snapshot.assert_match(result, "out.sql")


def test_translate_having(snapshot, table):
    expr = (
        table.group_by('g')
        .having(table.count() >= 1000)
        .aggregate(table.b.sum().name('b_sum'))
    )
    result = translate(expr.as_table().op())
    snapshot.assert_match(result, "out.sql")
