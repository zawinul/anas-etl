CREATE TABLE tablename (
	jobid int(10) not NULL,
	priority int(10),
	nretry int(10),
	status varchar(20),
	queue varchar(50),
	operation varchar(50),
	key1 varchar(500),
	key2 varchar(500),
	key3 varchar(500),
	creation varchar(20),
	last_change varchar(20),

	parent_job int(10),
	duration   int(10),

	body varchar(500),
	output varchar(1000)
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
