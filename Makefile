IMAGE_NAME := gcr.io/buffer-data/carden:0.2.1

all: run

build:
	docker build -t $(IMAGE_NAME) .

run: build
	docker run -it --rm -v ~/.config/gcloud/:/root/.config/gcloud --env-file .env $(IMAGE_NAME)

dev: build
	docker run -it --rm -v ~/.config/gcloud/:/root/.config/gcloud -v $(PWD):/app --env-file .env $(IMAGE_NAME) /bin/bash

push: build
	docker push $(IMAGE_NAME)
