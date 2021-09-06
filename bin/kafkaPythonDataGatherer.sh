# Stop
docker stop kafkaPythonDataGatherer

# Remove previuos container 
docker container rm kafkaPythonDataGatherer

docker build ../python/ --tag tap:python
docker run --network tap -e PYTHON_APP=DataGatherer.py --name kafkaPythonDataGatherer -it tap:python
