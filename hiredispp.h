/* hiredispp.h -- a C++ wrapper for hiredis.
 */

#ifndef HIREDISPP_H
#define HIREDISPP_H

#include <hiredis/hiredis.h>
#include <string>
#include <stdexcept>

namespace hiredispp
{
	class RedisReply
	{
	private:
		redisReply* _reply;
		bool _owns;

	public:
		static const std::string Nil;

		RedisReply(redisReply* reply, bool owns)
		{
			_reply = reply;
			_owns = owns;
		}

		virtual ~RedisReply()
		{
			if (_owns)
			{
				::freeReplyObject(_reply);
			}

			_reply = 0;
			_owns = false;
		}

		std::string getStatus() const
		{
			if (_reply->type != REDIS_REPLY_STATUS)
			{
				throw std::runtime_error("Invalid reply type");
			}

			return std::string(_reply->str, _reply->len);
		}

		operator std::string() const
		{
			if (
				_reply->type != REDIS_REPLY_STRING &&
				_reply->type != REDIS_REPLY_NIL)
			{
				throw std::runtime_error("Invalid reply type");
			}

			if (_reply->type == REDIS_REPLY_NIL)
			{
				return Nil;
			}

			return std::string(_reply->str, _reply->len);
		}

		operator long long() const
		{
			if (_reply->type != REDIS_REPLY_INTEGER)
			{
				throw std::runtime_error("Invalid reply type");
			}

			return _reply->integer;
		}

		size_t size() const
		{
			if (_reply->type != REDIS_REPLY_ARRAY)
			{
				throw std::runtime_error("Invalid reply type");
			}

			return _reply->elements;
		}

		RedisReply operator[](size_t i) const
		{
			if (_reply->type != REDIS_REPLY_ARRAY)
			{
				throw std::runtime_error("Invalid reply type");
			}

			if (i >= _reply->elements)
			{
				throw std::runtime_error("Out of range");
			}

			return RedisReply(_reply->element[i], false);
		}
	};

	const std::string RedisReply::Nil = "**NIL**";

	class RedisException : public std::exception
	{
	private:
		std::string _what;

	public:
		RedisException(const std::string& what)
		{
			_what = what;
		}

		virtual ~RedisException() throw() { }

		virtual const char* what() const throw()
		{
			return _what.c_str();
		}
	};

	class Redis
	{
	private:
		mutable redisContext* _context;

		std::string _host;
		int _port;

		void connect() const
		{
			if (_context == 0)
			{
				_context = ::redisConnect(_host.c_str(), _port);

				if (_context->err)
				{
					RedisException e(_context->errstr);

					::redisFree(_context);
					_context = 0;

					throw e;
				}
			}
		}

		RedisReply makeReply(redisReply* reply) const
		{
			if (reply->type == REDIS_REPLY_ERROR)
			{
				RedisException e(std::string(reply->str, reply->len));
				::freeReplyObject(reply);

				throw e;
			}

			return RedisReply(reply, true);
		}

	public:
		virtual ~Redis()
		{
			if (_context != 0)
			{
				::redisFree(_context);

				_context = 0;
			}
		}

		Redis(const std::string& host, int port = 6379)
			: _context(0)
		{
			_host = host;
			_port = port;
		}

		RedisReply endCommand() const
		{
			redisReply* reply;

			if (::redisGetReply(_context, reinterpret_cast<void**>(&reply)) != REDIS_OK)
			{
				RedisException e(_context->errstr);

				::redisFree(_context);
				_context = 0;

				throw e;
			}

			return makeReply(reply);
		}

		void beginSelect(int database) const
		{
			connect();

			::redisAppendCommand(_context, "SELECT %d", database);
		}

		void select(int database) const
		{
			beginSelect(database);
			endCommand();
		}

		void beginGet(const std::string& key) const
		{
			connect();

			::redisAppendCommand(_context, "GET %b", key.c_str(), key.size());
		}

		std::string get(const std::string& key) const
		{
			beginGet(key);
			return endCommand();
		}

		void beginSet(const std::string& key, const std::string& value) const
		{
			connect();

			::redisAppendCommand(_context, "SET %b %b", key.c_str(), key.size(), value.c_str(), value.size());
		}

		void set(const std::string& key, const std::string& value) const
		{
			beginSet(key, value);
			endCommand();
		}

		void beginSetnx(const std::string& key, const std::string& value) const
		{
			connect();

			::redisAppendCommand(_context, "SETNX %b %b", key.c_str(), key.size(), value.c_str(), value.size());
		}

		long long setnx(const std::string& key, const std::string& value) const
		{
			beginSetnx(key, value);
			return endCommand();
		}

		void beginHget(const std::string& key, const std::string& field) const
		{
			connect();

			::redisAppendCommand(_context, "HGET %b %b", key.c_str(), key.size(), field.c_str(), field.size());
		}

		std::string hget(const std::string& key, const std::string& field) const
		{
			beginHget(key, field);
			return endCommand();
		}

		void beginHset(const std::string& key, const std::string& field, const std::string& value) const
		{
			connect();

			::redisAppendCommand(_context, "HSET %b %b %b",
					key.c_str(), key.size(), field.c_str(), field.size(), value.c_str(), value.size());
		}

		long long hset(const std::string& key, const std::string& field, const std::string& value) const
		{
			beginHset(key, field, value);
			return endCommand();
		}

		void beginHgetall(const std::string& key) const
		{
			connect();

			::redisAppendCommand(_context, "HGETALL %b", key.c_str(), key.size());
		}

		RedisReply hgetall(const std::string& key) const
		{
			beginHgetall(key);
			return endCommand();
		}

		void beginSadd(const std::string& key, const std::string& member) const
		{
			connect();

			::redisAppendCommand(_context, "SADD %b %b", key.c_str(), key.size(), member.c_str(), member.size());
		}

		long long sadd(const std::string& key, const std::string& member) const
		{
			beginSadd(key, member);
			return endCommand();

		}

		void beginSmembers(const std::string& key) const
		{
			connect();

			::redisAppendCommand(_context, "SMEMBERS %b", key.c_str(), key.size());
		}

		RedisReply smembers(const std::string& key) const
		{
			beginSmembers(key);
			return endCommand();
		}
	};
}

#endif // HIREDISPP_H
