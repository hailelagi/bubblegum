package main

// maybe a sub set of sql with a recursive descent pratt parser
// or pull in: https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/parser/sql.y
/*
todo(investigate): could it be in scope to support:

db.Query("
create table users(
  id bigint primary key,
  name varchar(100),
  active" bool
);")

db.Query("insert into users values (1, "hello", true);");

db.Query("select * from users;")
*/
