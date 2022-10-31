CREATE TABLE tablename (
	`jobid` int(10) not NULL,
	`priority` int(10),
	`nretry` int(10),
	`status` varchar(20),
	`queue` varchar(50),
	`operation` varchar(50),
	`par1` varchar(500),
	`par2` varchar(500),
	`par3` varchar(500),
	`creation` varchar(20),
	`last_change` varchar(20),

	`parent_job` int(10),
	`duration`   int(10),

	`extra` longtext,
	`output` longtext
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
