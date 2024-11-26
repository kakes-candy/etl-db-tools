create table {{table.name}} (
    {%for c in table.columns -%}
    {{c.to_sql()}}{{ "," if not loop.last else "" }}
    {%endfor -%}
);
