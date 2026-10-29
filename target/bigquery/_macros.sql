{# spark_round(x, n): Spark F.round on a DOUBLE, i.e. HALF_UP on the shortest decimal string of x.
    Compares |x| against the tie k + 0.5 (correctly-rounded IEEE division of exact integers) instead of rounding the binary
    value or a NUMERIC cast. Exact on BigQuery and on the emulator (where NUMERIC is a float and 16.955 rounds to 16.95). #}
{% macro spark_round(x, n) -%}
{%- set mag = "IF(ABS(" ~ x ~ ") >= (10 * FLOOR(ABS(" ~ x ~ ") * " ~ 10 ** n ~ ") + 5) / " ~ 10 ** (n + 1)
    ~ ", FLOOR(ABS(" ~ x ~ ") * " ~ 10 ** n ~ ") + 1, FLOOR(ABS(" ~ x ~ ") * " ~ 10 ** n ~ ")) / " ~ 10 ** n -%}
IF({{ x }} < 0, 0 - {{ mag }}, {{ mag }})  {#- emulator: no unary minus, and SIGN(x) * y truncates to integer #}
{%- endmacro %}
