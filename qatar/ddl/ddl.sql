create table people
(
	code integer,
	company varchar(40),
	name varchar(40),
	gender varchar(40),
	age integer
);

create table flight
(
	travel_code integer,
	people_code integer,
	source varchar(40),
	destination varchar(40),
	flight_type varchar(40),
	price float,
	time float,
	distance float,
	agency varchar(40),
	date date
);

create table hotel
(
	travel_code integer,
	people_code integer,
	name varchar(40),
	place varchar(40),
	days integer,
	price float,
	total float,
	date date
);