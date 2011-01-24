Introduction
------------

Hiredispp is a C++ wrapper around hiredis C library. Hiredis is powerful yet low level client interface for Redis server. Hiredispp aims at providing Redis client interface for standard C++ by leveraging hiredis implementation.

Connection
----------

Connection to Redis server is represented by hiredispp::Redis class with type safe implementations of Redis commands

	hiredispp::Redis r("localhost");
	r.set("foo", "bar");
	std::string s = r.get("foo");
	boost::int64_t i = r.incr("counter");
	
Reply
-----

Multi-bulk Redis reply is represented by hiredispp::Redis::Reply type

	hiredispp::Redis::Reply reply = r.keys("foo:*");
	std::vector<std::string> keys;
	reply.toVector(keys);	

Dynamic Commands
----------------

It is possible to issue dynamically created commands with hiredispp::Redis::Command class

	std::vector<std::string> keys;
	hiredispp::Redis::Command sunionstore("SUNIONSTORE");
	sunionstore << "result" << keys;
	r.execute(sunionstore);

Pipelining
----------

Redis pipelining is enabled by pair of begin/end calls in hiredispp::Redis

	for (...)
	{	
		r.beginSadd(key, member);
	}

	for (...)
	{
		boost::int64_t c = r.endCommand();ng
	}

Also it possible to pipeline dynamic commands by executing vector of hiredispp::Redis::Command objects

	std::vector<hiredispp::Redis::Command> commands;
	for (...)
	{	
		commands.push_back(hiredispp::Redis::Command("SADD") << key << member);
	}
	std::vector<hiredispp::Redis::Reply> replies;
	r.execute(commands, replies);

Exceptions
----------

All hiredis library error conditions are thrown as hiredispp::RedisException

	try
	{
		r.ping();
	}
	catch (const hiredispp::RedisException& e)
	{
		cerr << e.what();
	}

After disconnection it is possible to reuse existing hiredispp::Redis object, it will attempt to restore connection.

UNICODE support
---------------

hiredispp::Redis has corresponding hiredispp::wRedis template instance that supports std::wstring. UNICODE strings are UTF-8 encoded for Redis.
