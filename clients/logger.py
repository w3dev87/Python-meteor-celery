import logging
import logging.config
import socket


# wrapper for configuring Python's standard logging API with Loggly.com
class Logger:
    def __init__(self, loggly_token):
        # use this device's ip address as a tag for all logs issued from program, hacky because cross-platform is hard
        ip = [l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]

        # if ip returns a multi length array, return the public ip which should be at [0]
        if len(ip) == 2:
            ip = ip[0]

        configuration = {
            u'version': 1,
            u'handlers': {
                u'loggly': {
                    u'class': u'loggly.handlers.HTTPSHandler',
                    u'formatter': u'basic',
                    u'level': u'DEBUG',
                    u'url': u'https://logs-01.loggly.com/inputs/' + loggly_token + '/tag/' + ip + '/'
                },
                u'console': {
                    u'class': u'logging.StreamHandler',
                    u'level': u'DEBUG',
                    u'formatter': u'basic',
                    u'stream': u'ext://sys.stdout',
                }
            },
            u'formatters': {
                u'basic': {
                    u'format': u'%(asctime)s | %(name)15s:%(lineno)3s:%(funcName)15s | %(levelname)7s | %(message)s'
                }
            },
            u'loggers': {
                u'root': {
                    u'level': u'DEBUG',
                    u'handlers': [u'console', u'loggly']
                }
            }
        }

        logging.config.dictConfig(configuration)
        self.logger = logging.getLogger(u'root')
        self.logger.info(ip)

    def get_logger(self):
        return self.logger
