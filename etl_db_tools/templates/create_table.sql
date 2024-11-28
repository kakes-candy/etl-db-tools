create table {{data.name}} (
    {%for c in data.columns -%}
    {{c.to_sql()}}{{ "," if not loop.last else "" }}
    {%endfor -%}
);
