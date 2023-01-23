push: image
	docker push images.local:5000/object-ingest

image:
	docker build . --tag=images.local:5000/object-ingest
