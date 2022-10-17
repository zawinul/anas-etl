CREATE TABLE `job` (
	`jobid` int(10) not NULL,
	`priority` int(10) 
	`nretry` int(10)
	`status` varchar(20),
	`queue` varchar(50),
	`type` varchar(50),
	`par1` varchar(50),
	`par2` varchar(50),
	`par3` varchar(50),
	`creation` varchar(20),
	`last_change` varchar(20),
	`extra` longtext
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;





CREATE TABLE `job` (
 `jobid` int(11) NOT NULL,
 `objectid` varchar(30),
 `priority` int(11),
 `status` varchar(8)  NOT NULL,
 `queue` varchar(20)  NOT NULL,
 `creation` varchar(20) ,
 `last_change` varchar(20) ,
 `command` varchar(30),
 `body` longtext 
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

CREATE TABLE `job_done` (
 `jobid` int(11) NOT NULL,
 `objectid` varchar(30),
 `priority` int(11),
 `status` varchar(8)  NOT NULL,
 `queue` varchar(20)  NOT NULL,
 `creation` varchar(20) ,
 `last_change` varchar(20) ,
 `command` varchar(30),
 `body` longtext 
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

CREATE TABLE `jobid_sequence` (
 `id` int(11) NOT NULL
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

delete from jobid_sequence;
insert into jobid_sequence (id) values(1000);