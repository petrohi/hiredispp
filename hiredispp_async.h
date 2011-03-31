#ifndef _HiredisppAsync_H_
#define _HiredisppAsync_H_

#include <hiredis/adapters/libev.h>
#include <memory>
#include <boost/function.hpp>

#define HIREDISPP_DEBUG

namespace hiredispp
{
    class RedisConnectionAsync
    {
    public:
        RedisConnectionAsync(const std::string& host, int port)
            : _host(host), _port(port), _reconnect(false)
        {}

        template<typename HandlerC, typename HandlerD>
        void connect(HandlerC handlerC, HandlerD handlerD)
        {
            _onConnected = createOnHandler(handlerC);
            _onDisconnected = createOnHandler(handlerD);
            asyncConnect();
        }
        
        void disconnect()
        {
            if (_ac) 
                redisAsyncDisconnect(_ac);
        }

        template<typename CharT, typename ExecHandler>
        void execAsyncCommand(const RedisCommandBase<CharT> & cmd, ExecHandler handler)
        {
            if (_ac->c.flags & (REDIS_DISCONNECTING | REDIS_FREEING))
                throw RedisException("Can't execute a command, disconnecting or freeing");

            const int sz=cmd.size();
            const char* argv[sz];
            size_t argvlen[sz];
            
            for (size_t i = 0; i < sz; ++i) {
                argv[i] = cmd[i].data();
                argvlen[i] = cmd[i].size();
            }
            
            Handler<ExecHandler> *hand=new Handler<ExecHandler>(handler);
            int result = 
                ::redisAsyncCommandArgv(_ac, Handler<ExecHandler>::callback, hand,
                                        sz, argv, argvlen);

            if (result == REDIS_ERR) {
                delete hand;
                throw RedisException("Can't execute a command, REDIS ERROR");
            }
        }

    private:
        typedef RedisConnectionAsync ThisType;

        class BaseOnHandler
        {
        public:
            virtual void operator() (int)=0;
            virtual ~BaseOnHandler() {}
        };

        template<typename Handler>
        class OnHandler : public BaseOnHandler
        {
            Handler _handler;
        public:
            OnHandler(Handler handler) : _handler(handler) {}
            virtual void operator() (int arg) {
                std::cout<<"virtual operator("<<arg<<")"<<std::endl;
                _handler(arg);
            }
        };
        
        template<typename HandlerT>
        typename std::auto_ptr< BaseOnHandler > createOnHandler(HandlerT handler)
        {
            return std::auto_ptr< BaseOnHandler >(new OnHandler<HandlerT>(handler));
        }

        template<typename Callback>
        class Handler // : public enable_shared_from_this<Handler <Callback> >
        {
        public:
            Handler(Callback c) : _c(c) {}

            static void callback(redisAsyncContext *c, void *reply, void *privdata)
            {
                (static_cast< Handler<Callback> * > (privdata)) -> operator() (c,reply);
            }

            void operator() (redisAsyncContext *c, void *reply)
            {
                if (reply) {
                    Redis::Element el(static_cast<redisReply*>(reply));
                    _c(*static_cast<ThisType*>(c->data),&el);
                }
                else {
                    _c(*static_cast<ThisType*>(c->data),static_cast<Redis::Element*>(NULL));
                }
                delete(this);
            }

        private:
            Callback _c;
        };
        
        void onConnected()
        {
            _onConnected->operator()(0);
        }
        
        void onDisconnected(int status)
        {
            if (status==REDIS_ERR) {
                if (_ac->err) {
                    // print an error 
                    printf("Error: %s\n", _ac->errstr);
                }
                asyncClose();
            }
            else if (status==REDIS_OK) {
                _ac=NULL;
            }
            _onDisconnected->operator()(status);

            if (status==REDIS_ERR && _ac && _reconnect) {
                // reconnect
                asyncConnect();
            }
        }
        
        static void connected(const redisAsyncContext *ac)
        {
            std::cout<<"static::connected"<<std::endl;
            if (ac && ac->data) {
                ((RedisConnectionAsync*)(ac->data))->onConnected();
            }
        }
        
        static void disconnected(const redisAsyncContext *ac, int status)
        {
            std::cout<<"static::disconnected"<<std::endl;
            if (ac && ac->data) {
                ((RedisConnectionAsync*)(ac->data))->onDisconnected(status);
            }
        }

        std::string        _host;
        uint16_t           _port;
        bool               _reconnect;
        redisAsyncContext* _ac;
        std::auto_ptr<BaseOnHandler>   _onConnected;
        std::auto_ptr<BaseOnHandler>   _onDisconnected;

        int asyncConnect()
        {
#ifdef HIREDISPP_DEBUG
            std::cout<<"asyncConnect()"<<std::endl;
#endif
            _ac = redisAsyncConnect(_host.c_str(), _port);
            _ac->data = (void*)this;

            if (_ac->err) {
                std::cout << "Error: " << _ac->errstr << std::endl;
                // TODO: handle error
            }

            if (redisAsyncSetConnectCallback(_ac, connected)!=REDIS_OK ||
                redisAsyncSetDisconnectCallback(_ac, disconnected)!=REDIS_OK)
                std::cout << "Can't register callbacks" << std::endl;
            redisLibevAttach(EV_DEFAULT, _ac);
            // actually start io proccess
            ev_io_start(EV_DEFAULT, &((((redisLibevEvents*)(_ac->ev.data)))->rev));
            ev_io_start(EV_DEFAULT, &((((redisLibevEvents*)(_ac->ev.data)))->wev));
        }

        void asyncClose()
        {
            ev_io_stop(EV_DEFAULT, &((((redisLibevEvents*)(_ac->ev.data)))->rev));
            ev_io_stop(EV_DEFAULT, &((((redisLibevEvents*)(_ac->ev.data)))->wev));
            // redisLibevCleanup(_ac->_adapter_data);
            close(_ac->c.fd);
        }
    };

}
#endif
