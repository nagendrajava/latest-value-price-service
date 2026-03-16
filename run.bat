@echo off
echo Starting Kafka and Application...

REM Make sure Kafka is running
docker-compose up -d

REM Run the application
mvn compile exec:java -Dexec.mainClass="com.assignment.price.Application"

echo Done.
pause
