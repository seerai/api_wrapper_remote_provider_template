# API Wrapper Remote Provider Template

This repo is meant to help jump start the creation of a remote provider that 
wraps some external API serving feature data. While this application of a remote provider is
fairly straightforward, there are many options to handle, and each API presents its own
unique challenges. The main challenges in writing the provider are 1) translating a Dataset search
request in Geodesic to an API request for the provider, and 2) receiving the response and parsing the
results into Geodesic Features. This template is set up so that the user only has to provide the
details of those operations, depending on the API in question.

---


This is a simple demo showing how a Boson Remote Provider can be used to serve
simulated data into a GIS application or model. The data generated is a simple
non-homogeneous Poisson process with a spatial density that is a gaussian 
for three different locations. Each time the Boson search endpoint is called,
a new set of lightning points will be generated and returned.

## Building the Image
Boson remote providers are built as docker images using the Boson Python SDK.
To build the image, change to the boson directory run the build script

```bash
sh build.sh 1
```
The number argument is the version number you would like to tag the image with.

## Running the Image Locally
If you want to test the image locally you can run it using the run script

```bash
sh run.sh 1
```
Here the number argument is the version number of the image you would like to run.
This will start the uvicorn server on port 8000.

There is also a test script that will call the search endpoint and print the results to make 
sure that you are revieving the expected data.

```bash
python test.py
```

## Setting up the Remote Provider

To set up the remote provider, you will need to push the image to a container registry. In this case
we will push it to GCP artifact registry.

```bash
docker push us-central1-docker.pkg.dev/double-catfish-291717/seerai-docker/images/lightning-simulator:v0.0.1
```

You then need to create a cloud run service using this image. On the GCP console go to cloud run and create a new service.
Select the image from the artifact registry and set the port to 8000. You can also set the memory and cpu limits here.
One problem I have found is that the cloud run service will overwrite some environment variables that 
may cause the container to fail. In the case of this particular image you will need to set the `HOME`
environment variable to `/root`. All of these settings are found on the creation page for the 
cloud run service.

Once the service is created you need to create the dataset in Entanglement that points to it. This 
is easy to do with the _from_ method:
    
```python
ds = geodesic.Dataset.from_remote_provider(
    name='lightning-simulation',
    url='https://lightning-simulator-azwzjbkrwq-uc.a.run.app',
    middleware=middleware.SearchFilter.spatial()
)
ds.save()
```

The cloud run URL can be found on the cloud run service page. Once the dataset is created you can
create a share link for it.

```python
share = ds.share_as_arcgis_service()
share.get_feature_layer_url()
```
With this URL you can add the layer into an Arcgis map.

