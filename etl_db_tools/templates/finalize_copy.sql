drop table if exists {{data.target_name}};

exec sp_rename [{{data.temp_table_name}}],  [{{data.target_name}}] 