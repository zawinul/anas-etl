CREATE TABLE tablename (
	jobid int(10) not NULL,
	priority int(10),
	nretry int(10),
	locktag varchar(6),
	queue varchar(20),
	operation varchar(50),
	key1 varchar(500),
	key2 varchar(500),
	key3 varchar(500),
	creation varchar(20),
	last_change varchar(20),

	parent_job int(10),
	duration   int(10),

	body varchar(2000),
	output varchar(500)
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
