FROM ubuntu:18.04

RUN apt-get update -y && \
    apt-get install -y nginx && \
    rm -rf /var/lib/apt/lists/*

RUN rm -f /etc/nginx/sites-enabled/default
ADD gateway.nginx.conf /etc/nginx/conf.d/gateway.conf
ADD gzip.conf /etc/nginx/conf.d/gzip.conf

RUN sed -e 's,/var/log/nginx/error.log,/var/log/nginx/error.log info,g' < /etc/nginx/nginx.conf > /tmp/baz && mv /tmp/baz /etc/nginx/nginx.conf

RUN ln -sf /dev/stdout /var/log/nginx/access.log
RUN ln -sf /dev/stderr /var/log/nginx/error.log

CMD ["nginx", "-g", "daemon off;"]
