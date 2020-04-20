#!/bin/bash
/usr/local/bin/supervisord -c /usr/local/etc/supervisord.conf
exec "$@"
