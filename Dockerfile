FROM java
MAINTAINER Gal Dolber <gal@dolber.com>
ADD target/nube.jar /nube.jar
WORKDIR /
EXPOSE 80
ENV PORT 80
ENTRYPOINT java -jar nube.jar
