mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file \
                         -Dfile=./non-maven-libs/jace-5.2.jar \
						 -DgroupId=eng.anas.local \ 
                         -DartifactId=jace -Dversion=1.0.0 