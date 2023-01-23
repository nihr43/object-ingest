# object-ingest

phones these days seem to be generating HEIC images, which dont get along with my linux computers, browsers, etc.

this is a tool that weeds out these unwanted files after i've dropped them into object storage.

kubernetes elements are provided to run this as a kubernetes Job, so data/network traffic stays physically local to my webservers.

```
$ kubectl create -f job.yaml
$ kubectl logs $(kubectl get pods --selector=job-name=object-ingest -ojson | jq -r .items[0].metadata.name)
privileged_main(): 0 objects added to queue
```
