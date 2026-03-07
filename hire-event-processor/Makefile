docker-image-build:
	docker build -t hire_insight_ai_assistant_hire_event_processor:latest .
	minikube image load hire_insight_ai_assistant_hire_event_processor:latest

docker-image-rm:
	docker image rm hire_insight_ai_assistant_hire_event_processor:latest

test:
	go test -parallel=1 -count=1 ./... -coverprofile cover.out
	go tool cover -func=cover.out | tail -n1

gomod:
	go mod tidy
	go mod vendor
