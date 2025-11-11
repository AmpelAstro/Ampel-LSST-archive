FROM lipanski/docker-static-website:2.6.0

# serve index.html in place of missing files (single page app)
COPY <<EOF httpd.conf
E404:index.html
EOF

# override command line to use our custom config
# see: https://github.com/lipanski/docker-static-website/issues/39
CMD ["/busybox-httpd", "-f", "-v", "-p", "3000", "-c", "httpd.conf"]

COPY ./build /home/static
